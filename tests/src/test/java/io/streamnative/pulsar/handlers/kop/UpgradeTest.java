/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

/**
 * Test upgrade from older version that doesn't support BrokerEntryMetadata.
 */
@Slf4j
public class UpgradeTest extends KopProtocolHandlerTestBase {

    private final List<TestTopic> testTopicList = Lists.newArrayList(
            new TestTopic(4, 1),
            new TestTopic(4, 2),
            new TestTopic(3, 3),
            new TestTopic(2, 4),
            new TestTopic(1, 4)
    );

    @BeforeClass(timeOut = 30000L)
    @Override
    protected void setup() throws Exception {
        conf.setBrokerEntryMetadataInterceptors(null);
        enableBrokerEntryMetadata = false;
        internalSetup();
        for (TestTopic testTopic : testTopicList) {
            testTopic.sendOldMessages();
        }

        stopBroker();
        enableBrokerEntryMetadata = true;
        startBroker();
        createAdmin();
        createClient();
        for (TestTopic testTopic : testTopicList) {
            testTopic.sendNewMessages();
        }
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void testOffsetsInSendCallback() {
        for (TestTopic testTopic : testTopicList) {
            testTopic.testOffsetsInSendCallback();
        }
    }

    @Test(timeOut = 20000L)
    public void testConsumeEarliest() throws Exception {
        for (TestTopic testTopic : testTopicList) {
            testTopic.testConsumeEarliest();
        }
    }

    @Test(timeOut = 20000L)
    public void testConsumeLatest() throws Exception {
        for (TestTopic testTopic : testTopicList) {
            testTopic.testConsumeLatest();
        }
    }

    private void sendMessages(final String topic,
                              final int start,
                              final int end,
                              final List<Long> offsets) throws Exception {
        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());
        for (int i = start; i < end; i++) {
            final String value = "msg-" + i;
            producer.send(new ProducerRecord<>(topic, value), (metadata, e) -> {
                if (e == null) {
                    offsets.add(metadata.offset());
                    log.info("Send {} to {}-{}@{}", value, metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Failed to send {} to {}: {}", value, topic, e.getMessage());
                }
            }).get();
        }
        producer.close();
    }

    private class TestTopic {
        private final int numOldMessages;
        private final int numNewMessages;
        private final int numMessages;
        private final String topicName;
        private final List<Long> oldOffsets = Lists.newArrayList();
        private final List<Long> expectedOldOffsets = Lists.newArrayList();
        private final List<Long> newOffsets = Lists.newArrayList();
        private final List<Long> expectedNewOffsets = Lists.newArrayList();

        public TestTopic(final int numOldMessages, final int numNewMessages) {
            this.numOldMessages = numOldMessages;
            this.numNewMessages = numNewMessages;
            this.numMessages = numOldMessages + numNewMessages;
            this.topicName = "test-skip-old-messages-" + numOldMessages + "-" + numNewMessages;
            for (int i = 0; i < numOldMessages; i++) {
                this.expectedOldOffsets.add(MessagePublishContext.DEFAULT_OFFSET);
            }
            for (int i = 0; i < numNewMessages; i++) {
                this.expectedNewOffsets.add((long) i);
            }
        }

        public void sendOldMessages() throws Exception {
            sendMessages(topicName, 0, numOldMessages, oldOffsets);
        }

        public void sendNewMessages() throws Exception {
            sendMessages(topicName, numOldMessages, numMessages, newOffsets);
        }

        public void testOffsetsInSendCallback() {
            log.info("[{}] old offsets: {} (expected: {}), new offsets: {} (expected: {})",
                    topicName, oldOffsets, expectedOldOffsets, newOffsets, expectedNewOffsets);
            Assert.assertEquals(oldOffsets.size(), numOldMessages);
            Assert.assertEquals(oldOffsets, expectedOldOffsets);
            Assert.assertEquals(newOffsets.size(), numNewMessages);
            Assert.assertEquals(newOffsets, expectedNewOffsets);
        }

        public void testConsumeEarliest() throws Exception {
            // Verify the restart of broker doesn't clear the previous messages
            final List<String> allValues = receiveValuesByPulsarConsumer(numMessages);
            Assert.assertEquals(allValues.size(), numMessages);

            final KafkaConsumer<String, String> consumer =
                    new KafkaConsumer<>(newKafkaConsumerProperties("test-consume-earliest"));
            consumer.subscribe(Collections.singleton(topicName));

            final Pair<List<String>, List<Long>> valuesAndOffsets = receiveValuesAndOffsets(consumer, numMessages);
            log.info("[{}] All values: {}, received: {}, offsets: {} (expect: {})",
                    topicName, allValues, valuesAndOffsets.getLeft(), valuesAndOffsets.getRight(), expectedNewOffsets);
            Assert.assertEquals(valuesAndOffsets.getLeft(), allValues.subList(numOldMessages, numMessages));
            Assert.assertEquals(valuesAndOffsets.getRight(), expectedNewOffsets);
            consumer.close();
        }

        public void testConsumeLatest() throws Exception {
            final Properties props = newKafkaConsumerProperties("test-consume-latest");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

            @Cleanup
            final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            final AtomicBoolean rebalanceDone = new AtomicBoolean(false);
            consumer.subscribe(Collections.singleton(topicName), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // No ops
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    rebalanceDone.set(true);
                }
            });
            for (int i = 0; i < 50 && !rebalanceDone.get(); i++) {
                Assert.assertEquals(consumer.poll(Duration.ofMillis(100)).count(), 0);
            }
            Assert.assertTrue(rebalanceDone.get());

            final List<Long> offsets = Lists.newArrayList();
            sendMessages(topicName, numMessages, numMessages + 1, offsets);
            Assert.assertEquals(offsets, Lists.newArrayList(Collections.singletonList((long) numNewMessages)));

            final Pair<List<String>, List<Long>> valuesAndOffsets = receiveValuesAndOffsets(consumer, 1);
            log.info("[{}] testConsumeLatest received values: {}, offsets: {}",
                    topicName, valuesAndOffsets.getLeft(), valuesAndOffsets.getRight());
            Assert.assertEquals(valuesAndOffsets.getLeft(), Lists.newArrayList("msg-" + numMessages));
            Assert.assertEquals(valuesAndOffsets.getRight(), offsets);
        }

        private List<String> receiveValuesByPulsarConsumer(int maxNumMessages) throws Exception {
            final List<String> values = Lists.newArrayList();
            final Consumer<byte[]> consumer = pulsarClient.newConsumer()
                    .topic(topicName)
                    .subscriptionName("my-sub")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
            final long startTimeMs = System.currentTimeMillis();
            while (values.size() < maxNumMessages && System.currentTimeMillis() - startTimeMs < 3000L) {
                final Message<byte[]> message = consumer.receive(100, TimeUnit.MILLISECONDS);
                if (message != null) {
                    values.add(Schema.STRING.decode(message.getData()));
                }
            }
            consumer.close();
            return values;
        }

        private Pair<List<String>, List<Long>> receiveValuesAndOffsets(final KafkaConsumer<String, String> consumer,
                                                                       int maxNumMessages) {
            final List<String> values = Lists.newArrayList();
            final List<Long> offsets = Lists.newArrayList();
            final long startTimeMs = System.currentTimeMillis();
            while (values.size() < maxNumMessages && System.currentTimeMillis() - startTimeMs < 3000L) {
                consumer.poll(Duration.ofMillis(100)).forEach(record -> {
                    values.add(record.value());
                    offsets.add(record.offset());
                });
            }
            return Pair.of(values, offsets);
        }
    }
}
