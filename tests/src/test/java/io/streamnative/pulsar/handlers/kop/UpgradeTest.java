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
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
            new TestTopic(9, 1),
            new TestTopic(7, 3),
            new TestTopic(5, 5),
            new TestTopic(3, 7),
            new TestTopic(1, 9)
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

        internalCleanup(false);
        enableBrokerEntryMetadata = true;
        internalSetup();
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
    public void testSkipOldMessages() {
        for (TestTopic testTopic : testTopicList) {
            testTopic.verify();
        }
    }

    private void sendMessages(final String topic,
                              final int start,
                              final int end,
                              final List<Long> offsets,
                              @Nullable final List<String> values) throws Exception {
        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());
        for (int i = start; i < end; i++) {
            final String value = "msg-" + i;
            if (values != null) {
                values.add(value);
            }
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
        private final List<String> expectedNewValues = Lists.newArrayList();

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
            sendMessages(topicName, 0, numOldMessages, oldOffsets, null);
        }

        public void sendNewMessages() throws Exception {
            sendMessages(topicName, numOldMessages, numMessages, newOffsets, expectedNewValues);
        }

        public void verify() {
            log.info("[{}] old offsets: {} (expected: {}), new offsets: {} (expected: {})",
                    topicName, oldOffsets, expectedOldOffsets, newOffsets, expectedNewOffsets);
            Assert.assertEquals(oldOffsets.size(), numOldMessages);
            Assert.assertEquals(oldOffsets, expectedOldOffsets);
            Assert.assertEquals(newOffsets.size(), numNewMessages);
            Assert.assertEquals(newOffsets, expectedNewOffsets);

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

            log.info("[{}] Received values: {} (expect: {}), offsets: {} (expect: {})",
                    topicName, values, expectedNewValues, offsets, expectedNewOffsets);
            Assert.assertEquals(values, expectedNewValues);
            Assert.assertEquals(offsets, expectedNewOffsets);
            consumer.close();
        }
    }
}
