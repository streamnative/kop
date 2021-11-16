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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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

    @Test(timeOut = 20000L)
    public void testSkipOldMessages() throws Exception {
        for (TestTopic testTopic : testTopicList) {
            testTopic.verify();
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

        public void verify() throws Exception {
            log.info("[{}] old offsets: {} (expected: {}), new offsets: {} (expected: {})",
                    topicName, oldOffsets, expectedOldOffsets, newOffsets, expectedNewOffsets);
            Assert.assertEquals(oldOffsets.size(), numOldMessages);
            Assert.assertEquals(oldOffsets, expectedOldOffsets);
            Assert.assertEquals(newOffsets.size(), numNewMessages);
            Assert.assertEquals(newOffsets, expectedNewOffsets);

            // Verify the restart of broker doesn't clear the previous messages
            final List<String> allValues = receiveAllMessages();
            Assert.assertEquals(allValues.size(), numMessages);

            final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(newKafkaConsumerProperties());
            consumer.subscribe(Collections.singleton(topicName));

            final List<String> values = Lists.newArrayList();
            final List<Long> offsets = Lists.newArrayList();
            final long startTimeMs = System.currentTimeMillis();
            while (values.size() < numNewMessages && System.currentTimeMillis() - startTimeMs < 3000L) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                    values.add(record.value());
                    offsets.add(record.offset());
                }
            }

            log.info("[{}] All values: {}, received: {}, offsets: {} (expect: {})",
                    topicName, allValues, values, offsets, expectedNewOffsets);
            Assert.assertEquals(values, allValues.subList(numOldMessages, numMessages));
            Assert.assertEquals(offsets, expectedNewOffsets);
            consumer.close();
        }

        private List<String> receiveAllMessages() throws Exception {
            final List<String> values = Lists.newArrayList();
            final Consumer<byte[]> consumer = pulsarClient.newConsumer()
                    .topic(topicName)
                    .subscriptionName("my-sub")
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
            for (int i = 0; i < numMessages; i++) {
                final Message<byte[]> message = consumer.receive(3, TimeUnit.SECONDS);
                Assert.assertNotNull(message);
                values.add(Schema.STRING.decode(message.getData()));
            }
            consumer.close();
            return values;
        }
    }
}
