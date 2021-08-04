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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Basic end-to-end test with `entryFormat=kafka`.
 */
@Slf4j
public class BasicEndToEndKafkaTest extends BasicEndToEndTestBase {

    public BasicEndToEndKafkaTest() {
        super("kafka");
    }

    @DataProvider(name = "enableBatching")
    public static Object[][] enableBatching() {
        return new Object[][]{ { true }, { false } };
    }

    @Test(timeOut = 20000)
    public void testNullValueMessages() throws Exception {
        final String topic = "test-produce-null-value";

        @Cleanup
        final KafkaProducer<String, String> kafkaProducer = newKafkaProducer();
        sendSingleMessages(kafkaProducer, topic, Arrays.asList(null, ""));
        sendBatchedMessages(kafkaProducer, topic, Arrays.asList("test", "", null));

        final List<String> expectedMessages = Arrays.asList(null, "", "test", "", null);

        @Cleanup
        final KafkaConsumer<String, String> kafkaConsumer = newKafkaConsumer(topic);
        List<String> kafkaReceives = receiveMessages(kafkaConsumer, expectedMessages.size());
        assertEquals(kafkaReceives, expectedMessages);
    }

    @Test(timeOut = 20000)
    public void testDeleteClosedTopics() throws Exception {
        final String topic = "test-delete-closed-topics";
        final List<String> expectedMessages = Collections.singletonList("msg");

        admin.topics().createPartitionedTopic(topic, 1);
        final KafkaProducer<String, String> kafkaProducer = newKafkaProducer();
        sendSingleMessages(kafkaProducer, topic, expectedMessages);

        try {
            // topics cannot be deleted because the broker Producer has been attached to the PersistentTopic
            admin.topics().deletePartitionedTopic(topic);
            fail();
        } catch (PulsarAdminException e) {
            log.info("Failed to delete partitioned topic \"{}\": {}", topic, e.getMessage());
            assertTrue(e.getMessage().contains("Topic has active producers/subscriptions")
                    || e.getMessage().contains("Partitioned topic does not exist"));
        }

        final KafkaConsumer<String, String> kafkaConsumer1 = newKafkaConsumer(topic, "sub-1");
        assertEquals(receiveMessages(kafkaConsumer1, expectedMessages.size()), expectedMessages);
        try {
            admin.topics().deletePartitionedTopic(topic);
            fail();
        } catch (PulsarAdminException e) {
            log.info("Failed to delete partitioned topic \"{}\": {}", topic, e.getMessage());
            assertTrue(e.getMessage().contains("Topic has active producers/subscriptions")
                    || e.getMessage().contains("Partitioned topic does not exist"));
        }

        final KafkaConsumer<String, String> kafkaConsumer2 = newKafkaConsumer(topic, "sub-2");
        assertEquals(receiveMessages(kafkaConsumer2, expectedMessages.size()), expectedMessages);

        kafkaProducer.close();
        Thread.sleep(1000); // wait for Producer was removed from PersistentTopic
        try {
            // topics can be deleted even if the Kafka consumers are active because there are no broker side Consumers
            // that are attached to the PersistentTopic.
            admin.topics().deletePartitionedTopic(topic);
        } catch (PulsarAdminException e) {
            log.error("Failed to delete topic even if the producer has been closed: {}", e.getMessage());
            fail("Failed to delete topic after the producer is closed", e);
        }
        kafkaConsumer1.close();
        kafkaConsumer2.close();
    }

    @Test(timeOut = 30000)
    public void testPollEmptyTopic() throws Exception {
        int partitionNumber = 50;
        String kafkaTopic = "kopPollEmptyTopic" + partitionNumber;
        String pulsarTopicName = "persistent://public/default/" + kafkaTopic;

        admin.topics().createPartitionedTopic(pulsarTopicName, partitionNumber);
        int totalMsg = 500;

        String msgStrPrefix = "Message_kop_KafkaProduceAndConsume_" + partitionNumber + "_";
        @Cleanup
        KProducer kProducer = new KProducer(kafkaTopic, false, getKafkaBrokerPort(), true);
        kafkaPublishMessage(kProducer, totalMsg, msgStrPrefix);

        @Cleanup
        KConsumer kConsumer1 = new KConsumer(kafkaTopic, getKafkaBrokerPort(), "consumer-group-1");
        @Cleanup
        KConsumer kConsumer2 = new KConsumer(kafkaTopic, getKafkaBrokerPort(), "consumer-group-2");
        @Cleanup
        KConsumer kConsumer3 = new KConsumer(kafkaTopic, getKafkaBrokerPort(), "consumer-group-3");
        @Cleanup
        KConsumer kConsumer4 = new KConsumer(kafkaTopic, getKafkaBrokerPort(), "consumer-group-4");
        @Cleanup
        KConsumer kConsumer5 = new KConsumer(kafkaTopic, getKafkaBrokerPort(), "consumer-group-5");

        List<TopicPartition> topicPartitions = IntStream.range(0, partitionNumber)
            .mapToObj(i -> new TopicPartition(kafkaTopic, i)).collect(Collectors.toList());

        kafkaConsumeCommitMessage(kConsumer1, totalMsg, msgStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer2, totalMsg, msgStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer3, totalMsg, msgStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer4, totalMsg, msgStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer5, totalMsg, msgStrPrefix, topicPartitions);

        ConsumerRecords<Integer, String> records = kConsumer1.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        records = kConsumer2.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        records = kConsumer3.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        records = kConsumer4.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        records = kConsumer5.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());

        kafkaPublishMessage(kProducer, totalMsg, msgStrPrefix);

        kafkaConsumeCommitMessage(kConsumer1, totalMsg, msgStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer2, totalMsg, msgStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer3, totalMsg, msgStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer4, totalMsg, msgStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer5, totalMsg, msgStrPrefix, topicPartitions);

    }

    @Test(timeOut = 20000, dataProvider = "enableBatching")
    public void testMixedProduceKafkaConsume(boolean enableBatching) throws Exception {
        final String topic = "test-mixed-produce-kafka-consume";
        final int numMessages = 30;
        final List<String> values =
                IntStream.range(0, numMessages).mapToObj(i -> "value-" + i).collect(Collectors.toList());
        // Some messages doesn't have keys
        final List<String> keys = IntStream.range(0, numMessages)
                .mapToObj(i -> (i % 3 == 0) ? ("key-" + i) : null)
                .collect(Collectors.toList());
        // Some messages doesn't have properties or headers
        final List<Pair<String, String>> properties = IntStream.range(0, numMessages)
                .mapToObj(i -> (i % 5 == 0) ? Pair.of("prop-key-" + i, "prop-value-" + i) : null)
                .collect(Collectors.toList());

        final KafkaProducer<String, String> kafkaProducer = newKafkaProducer();
        final Producer<byte[]> pulsarProducer = newPulsarProducer(topic, enableBatching);

        boolean useOrderingKey = false;
        for (int i = 0; i < numMessages; i++) {
            final String key = keys.get(i);
            final String value = values.get(i);
            final Pair<String, String> property = properties.get(i);

            if (i % 2 == 0) {
                // Kafka's produce context
                final Iterable<Header> headers = (property == null)
                        ? null
                        : Collections.singletonList(new RecordHeader(
                        property.getKey(), property.getValue().getBytes(StandardCharsets.UTF_8)));
                kafkaProducer.send(new ProducerRecord<>(topic, 0, key, value, headers), (metadata, e) -> {
                    if (log.isDebugEnabled()) {
                        log.debug("KafkaProducer send {} to {}-partition-{}@{}",
                                value, metadata.topic(), metadata.partition(), metadata.offset());
                    }
                }).get();
            } else {
                final TypedMessageBuilder<byte[]> messageBuilder = pulsarProducer.newMessage()
                        .value(value.getBytes(StandardCharsets.UTF_8));
                if (key != null) {
                    // verify both orderingKey and key
                    if (useOrderingKey) {
                        messageBuilder.orderingKey(key.getBytes());
                        messageBuilder.key("XXX"); // verify ordering key's priority is higher
                    } else {
                        messageBuilder.key(key);
                    }
                    useOrderingKey = !useOrderingKey;
                }
                if (property != null) {
                    messageBuilder.property(property.getKey(), property.getValue());
                }
                final MessageId id = messageBuilder.send();
                if (log.isDebugEnabled()) {
                    log.debug("PulsarProducer send {} to {}", value, id);
                }
            }
        }

        pulsarProducer.close();
        kafkaProducer.close();

        @Cleanup
        final KafkaConsumer<String, String> kafkaConsumer = newKafkaConsumer(topic);
        final List<String> receivedKeys = new ArrayList<>();
        final List<String> receivedValues = new ArrayList<>();
        final List<Pair<String, String>> receivedProperties = new ArrayList<>();

        for (int i = 0; i < numMessages; ) {
            for (ConsumerRecord<String, String> record : kafkaConsumer.poll(Duration.ofSeconds(1))) {
                receivedValues.add(record.value());
                receivedKeys.add(record.key());
                final Iterator<Header> headers = record.headers().iterator();
                if (headers.hasNext()) {
                    final Header header = headers.next();
                    receivedProperties.add(Pair.of(header.key(), new String(header.value(), StandardCharsets.UTF_8)));
                } else {
                    receivedProperties.add(null);
                }
                i++;
            }
        }
        assertEquals(receivedValues, values);
        assertEquals(receivedKeys, keys);
        assertEquals(receivedProperties, properties);
    }
}
