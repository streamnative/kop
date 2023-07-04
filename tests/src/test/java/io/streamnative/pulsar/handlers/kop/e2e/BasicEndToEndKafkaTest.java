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
package io.streamnative.pulsar.handlers.kop.e2e;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.streamnative.kafka.client.api.Header;
import io.streamnative.pulsar.handlers.kop.KafkaPayloadProcessor;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.awaitility.Awaitility;
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
        return new Object[][]{{true}, {false}};
    }

    @Test(timeOut = 20000)
    public void testNullValueMessages() throws Exception {
        final String topic = "test-produce-null-value";

        @Cleanup final KafkaProducer<String, String> kafkaProducer = newKafkaProducer();
        sendSingleMessages(kafkaProducer, topic, Arrays.asList(null, ""));
        sendBatchedMessages(kafkaProducer, topic, Arrays.asList("test", "", null));

        final List<String> expectedMessages = Arrays.asList(null, "", "test", "", null);

        @Cleanup final KafkaConsumer<String, String> kafkaConsumer = newKafkaConsumer(topic);
        List<String> kafkaReceives = receiveMessages(kafkaConsumer, expectedMessages.size());
        assertEquals(kafkaReceives, expectedMessages);

        @Cleanup final Consumer<byte[]> pulsarConsumer =
                newPulsarConsumer(topic, SUBSCRIPTION, new KafkaPayloadProcessor());
        List<String> pulsarReceives = receiveMessages(pulsarConsumer, expectedMessages.size());
        assertEquals(pulsarReceives, expectedMessages);
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
        KopProtocolHandlerTestBase.KProducer kProducer =
                new KopProtocolHandlerTestBase.KProducer(kafkaTopic, false, getKafkaBrokerPort(), true);
        kafkaPublishMessage(kProducer, totalMsg, msgStrPrefix);

        @Cleanup
        KopProtocolHandlerTestBase.KConsumer kConsumer1 =
                new KopProtocolHandlerTestBase.KConsumer(kafkaTopic, getKafkaBrokerPort(), "consumer-group-1");
        @Cleanup
        KopProtocolHandlerTestBase.KConsumer kConsumer2 =
                new KopProtocolHandlerTestBase.KConsumer(kafkaTopic, getKafkaBrokerPort(), "consumer-group-2");
        @Cleanup
        KopProtocolHandlerTestBase.KConsumer kConsumer3 =
                new KopProtocolHandlerTestBase.KConsumer(kafkaTopic, getKafkaBrokerPort(), "consumer-group-3");
        @Cleanup
        KopProtocolHandlerTestBase.KConsumer kConsumer4 =
                new KopProtocolHandlerTestBase.KConsumer(kafkaTopic, getKafkaBrokerPort(), "consumer-group-4");
        @Cleanup
        KopProtocolHandlerTestBase.KConsumer kConsumer5 =
                new KopProtocolHandlerTestBase.KConsumer(kafkaTopic, getKafkaBrokerPort(), "consumer-group-5");

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
    public void testPulsarProduceKafkaConsume(boolean enableBatching) throws Exception {
        final String topic = "test-pulsar-produce-kafka-consume";
        final int numMessages = 30;
        final List<String> values =
                IntStream.range(0, numMessages).mapToObj(i -> "value-" + i).collect(Collectors.toList());
        // Some messages doesn't have keys
        final List<String> keys = IntStream.range(0, numMessages)
                .mapToObj(i -> (i % 3 == 0) ? ("key-" + i) : null)
                .collect(Collectors.toList());
        // Some messages doesn't have properties or headers
        final List<Header> headers = IntStream.range(0, numMessages)
                .mapToObj(i -> (i % 6 == 0) ? new Header("prop-key-" + i, "prop-value-" + i) : null)
                .collect(Collectors.toList());

        final Producer<byte[]> pulsarProducer = newPulsarProducer(topic, enableBatching);

        boolean useOrderingKey = false;
        final CountDownLatch sendCompleteLatch = new CountDownLatch(numMessages);
        for (int i = 0; i < numMessages; i++) {
            final String key = keys.get(i);
            final String value = values.get(i);
            final Header header = headers.get(i);

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
            if (header != null) {
                messageBuilder.property(header.getKey(), header.getValue());
            }
            final int index = i;
            messageBuilder.sendAsync().whenComplete((id, e) -> {
                if (e != null) {
                    log.error("Failed to send {}: {}", index, e.getMessage());
                } else if (log.isDebugEnabled()) {
                    log.debug("PulsarProducer send {} to {}", value, id);
                }
                sendCompleteLatch.countDown();
            });
        }

        sendCompleteLatch.await();
        pulsarProducer.close();

        @Cleanup final KafkaConsumer<String, String> kafkaConsumer = newKafkaConsumer(topic);
        final List<ConsumerRecord<String, String>> receivedRecords = receiveRecords(kafkaConsumer, numMessages);

        assertEquals(getValuesFromRecords(receivedRecords), values);
        assertEquals(getKeysFromRecords(receivedRecords), keys);
        assertEquals(getFirstHeadersFromRecords(receivedRecords), headers);
    }

    @Test(timeOut = 20000)
    public void testMixedProduceKafkaConsume() throws Exception {
        final String topic = "test-mixed-produce-kafka-consume";
        final int numMessages = 10;
        final List<String> values =
                IntStream.range(0, numMessages).mapToObj(i -> "value-" + i).collect(Collectors.toList());
        final List<String> keys = IntStream.range(0, numMessages)
                .mapToObj(i -> "key-" + i)
                .collect(Collectors.toList());
        final List<Header> headers = IntStream.range(0, numMessages)
                .mapToObj(i -> new Header("prop-key-" + i, "prop-value-" + i))
                .collect(Collectors.toList());

        final Producer<byte[]> pulsarProducer = newPulsarProducer(topic);
        final KafkaProducer<String, String> kafkaProducer = newKafkaProducer();

        for (int i = 0; i < numMessages; i++) {
            final String key = keys.get(i);
            final String value = values.get(i);
            final Header header = headers.get(i);

            if (i % 2 == 0) {
                final MessageId id = pulsarProducer.newMessage()
                        .value(value.getBytes(StandardCharsets.UTF_8))
                        .key(key)
                        .property(header.getKey(), header.getValue())
                        .send();
                if (log.isDebugEnabled()) {
                    log.debug("PulsarProducer send {} to {}", i, id);
                }
            } else {
                final RecordMetadata metadata = kafkaProducer.send(new ProducerRecord<>(topic, 0, key, value,
                        Header.toHeaders(Collections.singletonList(header), RecordHeader::new))).get();
                if (log.isDebugEnabled()) {
                    log.debug("KafkaProducer send {} to {}", i, metadata);
                }
            }
        }

        kafkaProducer.close();
        pulsarProducer.close();

        @Cleanup final KafkaConsumer<String, String> kafkaConsumer = newKafkaConsumer(topic);
        final List<ConsumerRecord<String, String>> receivedRecords = receiveRecords(kafkaConsumer, numMessages);

        assertEquals(getValuesFromRecords(receivedRecords), values);
        assertEquals(getKeysFromRecords(receivedRecords), keys);
        assertEquals(getFirstHeadersFromRecords(receivedRecords), headers);
    }


    @Test(timeOut = 20000, dataProvider = "enableBatching")
    public void testKafkaProducePulsarConsume(boolean enableBatching) throws Exception {
        final String topic = "test-kafka-produce-pulsar-consume";
        final KafkaProducer<String, String> producer = newKafkaProducer();
        final int numMessages = 10;

        Future<RecordMetadata> sendFuture = null;
        for (int i = 0; i < numMessages; i++) {
            if (i % 2 == 0) {
                sendFuture = producer.send(new ProducerRecord<>(topic, "msg-" + i));
            } else {
                sendFuture = producer.send(new ProducerRecord<>(topic, "key-" + i, "msg-" + i));
            }
            if (!enableBatching) {
                sendFuture.get();
            }
        }
        sendFuture.get();
        producer.close();

        @Cleanup final Consumer<byte[]> consumer = newPulsarConsumer(
                topic, "my-sub-" + enableBatching, new KafkaPayloadProcessor());
        final List<Message<byte[]>> messages = receivePulsarMessages(consumer, numMessages);
        assertEquals(messages.size(), numMessages);

        for (int i = 0; i < numMessages; i++) {
            final Message<byte[]> message = messages.get(i);
            assertEquals(convertPulsarMessageToString(message), "msg-" + i);
            if (i % 2 == 0) {
                assertNull(message.getKey());
            } else {
                assertEquals(message.getKey(), "key-" + i);
                assertEquals(message.getOrderingKey(), ("key-" + i).getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    @Test(timeOut = 20000, dataProvider = "enableBatching")
    public void testKafkaProducePulsarConsumeWithHeaders(boolean enableBatching) throws Exception {
        final String topic = "test-kafka-produce-pulsar-consume-with-headers";
        final KafkaProducer<String, String> producer = newKafkaProducer();
        final int numMessages = 10;
        Future<RecordMetadata> sendFuture = null;
        for (int i = 0; i < numMessages; i++) {
            final List<Header> headers = Collections.singletonList(new Header("prop-key-" + i, "prop-value-" + i));
            sendFuture = producer.send(new ProducerRecord<>(
                    topic, 0, "", "msg", Header.toHeaders(headers, RecordHeader::new)));
        }
        sendFuture.get();
        producer.close();

        @Cleanup final Consumer<byte[]> consumer = newPulsarConsumer(
                topic, "my-sub-" + enableBatching, new KafkaPayloadProcessor());
        final List<Message<byte[]>> messages = receivePulsarMessages(consumer, numMessages);
        assertEquals(messages.size(), numMessages);

        for (int i = 0; i < numMessages; i++) {
            assertEquals(messages.get(i).getProperty("prop-key-" + i), "prop-value-" + i);
        }
    }

    @Test(timeOut = 20000)
    public void testMixedProducePulsarConsume() throws Exception {
        final String topic = "test-mixed-produce-pulsar-consume";
        final int numMessages = 10;
        final KafkaProducer<String, String> kafkaProducer = newKafkaProducer();
        final Producer<byte[]> pulsarProducer = newPulsarProducer(topic);

        for (int i = 0; i < numMessages; i++) {
            final String key = "key-" + i;
            final String value = "value-" + i;
            final Header header = new Header("prop-key-" + i, "prop-value-" + i);

            if (i % 2 == 0) {
                pulsarProducer.newMessage()
                        .orderingKey(key.getBytes(StandardCharsets.UTF_8))
                        .value(value.getBytes(StandardCharsets.UTF_8))
                        .property(header.getKey(), header.getValue())
                        .send();
            } else {
                kafkaProducer.send(new ProducerRecord<>(topic, 0, key, value,
                        Header.toHeaders(Collections.singletonList(header), RecordHeader::new))).get();
            }
        }
        kafkaProducer.close();
        pulsarProducer.close();

        @Cleanup final Consumer<byte[]> consumer = newPulsarConsumer(topic, SUBSCRIPTION, new KafkaPayloadProcessor());
        final List<Message<byte[]>> messages = receivePulsarMessages(consumer, numMessages);
        assertEquals(messages.size(), numMessages);

        for (int i = 0; i < numMessages; i++) {
            final Message<byte[]> message = messages.get(i);
            assertEquals(message.getOrderingKey(), ("key-" + i).getBytes(StandardCharsets.UTF_8));
            assertEquals(message.getValue(), ("value-" + i).getBytes(StandardCharsets.UTF_8));
            assertEquals(message.getProperty("prop-key-" + i), "prop-value-" + i);
        }
    }

    @Test(timeOut = 20000)
    public void testDeletePartition() throws Exception {
        final String topic = "test-delete-partition";

        pulsar.getAdminClient().topics().createPartitionedTopic(topic, 2);

        @Cleanup final KafkaProducer<String, String> kafkaProducer = newKafkaProducer();
        sendSingleMessages(kafkaProducer, topic, Arrays.asList("a", "b", "c"));

        List<String> expectValues = Arrays.asList("a", "b", "c");

        @Cleanup final KafkaConsumer<String, String> kafkaConsumer = newKafkaConsumer(topic, "the-group");
        List<String> kafkaReceives = receiveMessages(kafkaConsumer, expectValues.size());
        assertEquals(kafkaReceives.stream().sorted().collect(Collectors.toList()), expectValues);

        sendSingleMessages(kafkaProducer, topic, Arrays.asList("d", "e", "f"));
        expectValues = Arrays.asList("d", "e", "f");
        kafkaReceives = receiveMessages(kafkaConsumer, expectValues.size());
        assertEquals(kafkaReceives.stream().sorted().collect(Collectors.toList()), expectValues);

        kafkaProducer.send(new ProducerRecord<>(topic, 0, null, "g")).get();
        kafkaProducer.send(new ProducerRecord<>(topic, 1, null, "h")).get();
        kafkaProducer.send(new ProducerRecord<>(topic, 0, null, "i")).get();

        pulsar.getAdminClient().topics().delete(topic + "-partition-1", true);

        Awaitility.await().untilAsserted(() -> {
            try {
                pulsar.getAdminClient().topics().getInternalStats(topic + "-partition-1");
                fail("topic still exists....");
            } catch (PulsarAdminException.NotFoundException notFound) {
                log.info("Topic is confirmed to be deleted from Pulsar");
            }
        });

        // "h" is written to Partition 1, so deleting partition-1 means that we lose "h"
        expectValues = Arrays.asList("g", "i");

        // kafkaConsume could have pre-fetched some messages
        // we use a new consumer in order to see the message from the partition that has been deleted
        kafkaConsumer.close();

        @Cleanup final KafkaConsumer<String, String> kafkaConsumer2 = newKafkaConsumer(topic, "the-group");
        kafkaReceives = receiveMessages(kafkaConsumer2, expectValues.size());
        assertEquals(kafkaReceives.stream().sorted().collect(Collectors.toList()), expectValues,
                "Expected " + expectValues
                        + " but received " + kafkaReceives.stream().sorted().collect(Collectors.toList()));

        pulsar.getAdminClient().topics().deletePartitionedTopic(topic);
    }


    @Test(timeOut = 20000)
    public void testReadCommitted() throws Exception {
        final String topic = "test-read-committed";

        pulsar.getAdminClient().topics().createPartitionedTopic(topic, 2);

        @Cleanup final KafkaProducer<String, String> kafkaProducer = newKafkaProducer();
        sendSingleMessages(kafkaProducer, topic, Arrays.asList("a", "b", "c"));

        List<String> expectValues = Arrays.asList("a", "b", "c");

        @Cleanup final KafkaConsumer<String, String> kafkaConsumer = newKafkaConsumer(topic, "test-group", true);
        List<String> kafkaReceives = receiveMessages(kafkaConsumer, expectValues.size());
        assertEquals(kafkaReceives.stream().sorted().collect(Collectors.toList()), expectValues);
    }
}
