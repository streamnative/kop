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


import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for Different kafka request type.
 * Test:
 * KafkaProducePulsarConsume
 * KafkaProduceKafkaConsume
 * PulsarProduceKafkaConsume
 * with
 * different partitions
 * batch enabled/disabled.
 * This test will involved test for class:
 * KafkaRequestHandler
 * MessageRecordUtils
 * MessagePublishContext
 * MessageConsumeContext
 */
@Slf4j
public class KafkaRequestTypeTest extends KopProtocolHandlerTestBase {

    @DataProvider(name = "partitions")
    public static Object[][] partitions() {
        return new Object[][] { { 1 }, { 7 } };
    }

    @DataProvider(name = "partitionsAndBatch")
    public static Object[][] partitionsAndBatch() {
        return new Object[][] {
            { 1, true },
            { 1, false },
            { 7, true },
            { 7, false }
        };
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000, dataProvider = "partitionsAndBatch")
    public void testKafkaProducePulsarConsume(int partitionNumber, boolean isBatch) throws Exception {
        String kafkaTopicName = "kopKafkaProducePulsarConsume" + partitionNumber;
        String pulsarTopicName = "persistent://public/default/" + kafkaTopicName;
        String key1 = "header_key1_";
        String key2 = "header_key2_";
        String value1 = "header_value1_";
        String value2 = "header_value2_";

        // create partitioned topic.
        admin.topics().createPartitionedTopic(kafkaTopicName, partitionNumber);

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(pulsarTopicName)
            .subscriptionName("test_k_producer_k_pconsumer_sub")
            .subscribe();

        // 1. produce message with Kafka producer.
        @Cleanup
        KProducer kProducer = new KProducer(kafkaTopicName, false, getKafkaBrokerPort());

        int totalMsgs = 10;
        String messageStrPrefix = "Message_Kop_KafkaProducePulsarConsume_"  + partitionNumber + "_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            ProducerRecord record = new ProducerRecord<>(
                kafkaTopicName,
                i,
                messageStr);

            record.headers()
                .add(key1 + i, (value1 + i).getBytes(UTF_8))
                .add(key2 + i, (value2 + i).getBytes(UTF_8));

            if (isBatch) {
                kProducer.getProducer()
                    .send(record);
            } else {
                kProducer.getProducer()
                    .send(record)
                    .get();
            }
            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message with header: ({}, {})", i, messageStr);
            }
        }

        // 2. Consume messages use Pulsar client Consumer. verify content and key and headers
        Message<byte[]> msg = null;
        for (int i = 0; i < totalMsgs; i++) {
            msg = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            Integer key = kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey()));
            assertEquals(messageStrPrefix + key.toString(), new String(msg.getValue()));

            // verify added 2 key-value pair
            Map<String, String> properties = msg.getProperties();
            assertEquals(properties.size(), 2);
            for (Map.Entry<String, String> kv: properties.entrySet()) {
                String k = kv.getKey();
                String v = kv.getValue();

                if (log.isDebugEnabled()) {
                    log.debug("headers key: {}, value:{}", k, v);
                }

                assertTrue(k.contains(key1) || k.contains(key2));
                assertTrue(v.contains(value1) || v.contains(value2));
            }
            if (log.isDebugEnabled()) {
                log.debug("Pulsar consumer get message: {}, key: {}",
                    new String(msg.getData()),
                    kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey())).toString());
            }
            consumer.acknowledge(msg);
        }

        // verify have received all messages
        msg = consumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);
    }

    @Test(timeOut = 20000, dataProvider = "partitionsAndBatch")
    public void testKafkaProduceKafkaConsume(int partitionNumber, boolean isBatch) throws Exception {
        String kafkaTopicName = "kopKafkaProduceKafkaConsume" + partitionNumber;
        String key1 = "header_key1_";
        String key2 = "header_key2_";
        String value1 = "header_value1_";
        String value2 = "header_value2_";

        // create partitioned topic.
        admin.topics().createPartitionedTopic(kafkaTopicName, partitionNumber);

        // 1. produce message with Kafka producer.
        int totalMsgs = 10;
        String messageStrPrefix = "Message_Kop_KafkaProduceKafkaConsume_" + partitionNumber + "_";

        @Cleanup
        KProducer kProducer = new KProducer(kafkaTopicName, false, getKafkaBrokerPort());

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            ProducerRecord record = new ProducerRecord<>(
                kafkaTopicName,
                i,
                messageStr);
            record.headers()
                .add(key1 + i, (value1 + i).getBytes(UTF_8))
                .add(key2 + i, (value2 + i).getBytes(UTF_8));

            if (isBatch) {
                kProducer.getProducer()
                    .send(record);
            } else {
                kProducer.getProducer()
                    .send(record)
                    .get();
            }
            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message with header: ({}, {})", i, messageStr);
            }
        }

        // 2. use kafka consumer to consume.
        @Cleanup
        KConsumer kConsumer = new KConsumer(kafkaTopicName, getKafkaBrokerPort());
        List<TopicPartition> topicPartitions = IntStream.range(0, partitionNumber)
            .mapToObj(i -> new TopicPartition(kafkaTopicName, i)).collect(Collectors.toList());
        log.info("Partition size: {}", topicPartitions.size());
        kConsumer.getConsumer().assign(topicPartitions);

        int i = 0;
        while (i < totalMsgs) {
            if (log.isDebugEnabled()) {
                log.debug("start poll message: {}", i);
            }
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());

                Header[] headers = record.headers().toArray();
                for (int j = 1; j <= 2; j++) {
                    String k = headers[j - 1].key();
                    String v = new String(headers[j - 1].value(), UTF_8);

                    if (log.isDebugEnabled()) {
                        log.debug("headers key: {}, value:{}", k, v);
                    }
                    assertTrue(k.contains(key1) || k.contains(key2));
                    assertTrue(v.contains(value1) || v.contains(value2));
                }
                if (log.isDebugEnabled()) {
                    log.debug("Kafka consumer get message: {}, key: {} at offset {}",
                        record.value(), record.key(), record.offset());
                }
                i++;
            }
        }
        assertEquals(i, totalMsgs);

        // no more records
        ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
    }

    @Test(timeOut = 20000, dataProvider = "partitionsAndBatch")
    public void testPulsarProduceKafkaConsume(int partitionNumber, boolean isBatch) throws Exception {
        String kafkaTopicName = "kopPulsarProduceKafkaConsume";
        String pulsarTopicName = "persistent://public/default/" + kafkaTopicName;
        String key1 = "header_key1_";
        String key2 = "header_key2_";
        String value1 = "header_value1_";
        String value2 = "header_value2_";

        // create partitioned topic.
        admin.topics().createPartitionedTopic(kafkaTopicName, partitionNumber);

        // 1. use pulsar producer to produce.
        int totalMsgs = 10;
        String messageStrPrefix = "Message_Kop_PulsarProduceKafkaConsume_" + partitionNumber + "_";

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
            .topic(pulsarTopicName)
            .enableBatching(isBatch);

        for (CompressionType compressionType : CompressionType.values()) {
            ProducerBuilder<byte[]> builder = producerBuilder.clone();
            @Cleanup
            Producer<byte[]> producer = builder.create();
            List<CompletableFuture<MessageId>> sendResults = Lists.newArrayListWithExpectedSize(totalMsgs);
            CountDownLatch latch = new CountDownLatch(1);
            for (int i = 0; i < totalMsgs; i++) {
                String message = messageStrPrefix + i;
                CompletableFuture<MessageId> id = producer.newMessage()
                        .keyBytes(kafkaIntSerialize(Integer.valueOf(i)))
                        .value(message.getBytes())
                        .property(key1 + i, value1 + i)
                        .property(key2 + i, value2 + i)
                        .sendAsync();
                sendResults.add(id);
            }
            FutureUtil.waitForAll(sendResults).whenCompleteAsync((r, t) -> {
                latch.countDown();
            });
            latch.await();
        }
        totalMsgs *= CompressionType.values().length;

        // 2. use kafka consumer to consume.
        @Cleanup
        KConsumer kConsumer = new KConsumer(kafkaTopicName, getKafkaBrokerPort());
        List<TopicPartition> kafkaTopicPartitions = IntStream.range(0, partitionNumber)
            .mapToObj(i -> new TopicPartition(kafkaTopicName, i)).collect(Collectors.toList());
        log.info("Partition size: {}. partitions: {}:",
            kafkaTopicPartitions.size(), kafkaTopicPartitions);
        kafkaTopicPartitions.forEach(partition -> log.info("     partition: {}", partition));
        kConsumer.getConsumer().assign(kafkaTopicPartitions);

        int i = 0;
        while (i < totalMsgs) {
            if (log.isDebugEnabled()) {
                log.debug("start poll message: {}", i);
            }
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                Header[] headers = record.headers().toArray();
                for (int j = 1; j <= 2; j++) {
                    String k = headers[j - 1].key();
                    String v = new String(headers[j - 1].value(), UTF_8);

                    if (log.isDebugEnabled()) {
                        log.debug("headers key: {}, value:{}", k, v);
                    }

                    assertTrue(k.contains(key1) || k.contains(key2));
                    assertTrue(v.contains(value1) || v.contains(value2));
                }
                if (log.isDebugEnabled()) {
                    log.debug("Kafka consumer get message: {}, key: {} at offset {}",
                            record.value(), record.key(), record.offset());
                }
                i++;
            }
        }
        assertEquals(i, totalMsgs);

        // no more records
        ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
        assertTrue(records.isEmpty());
    }


    // Test kafka consumer to consume, use consumer group and offset auto-commit
    @Test(timeOut = 20000, dataProvider = "partitions")
    public void testPulsarProduceKafkaConsume2(int partitionNumber) throws Exception {
        String topicName = "kopPulsarProduceKafkaConsume2" + partitionNumber;
        String pulsarTopicName = "persistent://public/default/" + topicName;

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, partitionNumber);

        // 1. use pulsar producer to produce.
        int totalMsgs = 10;
        String messageStrPrefix = "Message_Kop_PulsarProduceKafkaConsume_" + partitionNumber + "_";

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
            .topic(pulsarTopicName)
            .enableBatching(false);

        @Cleanup
        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < totalMsgs; i++) {
            String message = messageStrPrefix + i;
            producer.newMessage()
                .keyBytes(kafkaIntSerialize(Integer.valueOf(i)))
                .value(message.getBytes())
                .send();
        }

        // 2. use kafka consumer to consume, use consumer group, offset auto-commit
        @Cleanup
        KConsumer kConsumer = new KConsumer(topicName, getKafkaBrokerPort(), true);
        kConsumer.getConsumer().subscribe(Collections.singletonList(topicName));

        int i = 0;
        while (i < totalMsgs / 2) {
            if (log.isDebugEnabled()) {
                log.debug("start poll message: {}", i);
            }
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                if (log.isDebugEnabled()) {
                    log.debug("Kafka Consumer Received message: {}, {} at offset {}",
                            record.key(), record.value(), record.offset());
                }
                i++;
            }
        }
        kConsumer.close();

        // wait for offset commit complete
        Thread.sleep(500);

        log.info("start another consumer, will consume from the left place of first consumer");
        KConsumer kConsumer2 = new KConsumer(topicName, getKafkaBrokerPort(), true);

        kConsumer2.getConsumer().subscribe(Collections.singletonList(topicName));
        while (i < totalMsgs) {
            if (log.isDebugEnabled()) {
                log.debug("start poll message 2: {}", i);
            }
            ConsumerRecords<Integer, String> records = kConsumer2.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                if (log.isDebugEnabled()) {
                    log.debug("Kafka Consumer Received message 2: {}, {} at offset {}",
                        record.key(), record.value(), record.offset());
                }
                i++;
            }
        }

        assertEquals(i, totalMsgs);

        kConsumer2.getConsumer().close();
        // wait for offset commit complete
        Thread.sleep(500);

        // resubscribe the topic with the same group, no more records will be received
        kConsumer2 = new KConsumer(topicName, getKafkaBrokerPort(), true);
        kConsumer2.getConsumer().subscribe(Collections.singletonList(topicName));
        ConsumerRecords<Integer, String> records = kConsumer2.getConsumer().poll(Duration.ofSeconds(1));
        assertTrue(records.isEmpty());
    }

    // use String instead of Int as key
    @Test(timeOut = 20000)
    public void testKafkaProduceKafkaConsumeString() throws Exception {
        boolean isBatch = false;
        int partitionNumber = 1;
        String kafkaTopicName = "kopKafkaProduceKafkaConsume" + partitionNumber;
        String key1 = "header_key1_";
        String key2 = "header_key2_";
        String value1 = "header_value1_";
        String value2 = "header_value2_";
        String keyPrefix = "tenant-";

        // create partitioned topic.
        admin.topics().createPartitionedTopic(kafkaTopicName, partitionNumber);

        // 1. produce message with Kafka producer.
        int totalMsgs = 10;
        String messageStrPrefix = "Message_Kop_KafkaProduceKafkaConsume_" + partitionNumber + "_";

        @Cleanup
        KProducer kProducer = new KProducer(kafkaTopicName, false, getKafkaBrokerPort(),
                StringSerializer.class.getName(), StringSerializer.class.getName());

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            ProducerRecord record = new ProducerRecord<>(
                    kafkaTopicName,
                    keyPrefix + i,
                    messageStr);
            record.headers()
                    .add(key1 + i, (value1 + i).getBytes(UTF_8))
                    .add(key2 + i, (value2 + i).getBytes(UTF_8));

            if (isBatch) {
                kProducer.getProducer()
                        .send(record);
            } else {
                kProducer.getProducer()
                        .send(record)
                        .get();
            }
            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message with header: ({}, {})", i, messageStr);
            }
        }

        // 2. use kafka consumer to consume.
        @Cleanup
        KConsumer kConsumer = new KConsumer(kafkaTopicName, getKafkaBrokerPort(),
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        List<TopicPartition> topicPartitions = IntStream.range(0, partitionNumber)
                .mapToObj(i -> new TopicPartition(kafkaTopicName, i)).collect(Collectors.toList());
        log.info("Partition size: {}", topicPartitions.size());
        kConsumer.getConsumer().assign(topicPartitions);

        int i = 0;
        while (i < totalMsgs) {
            if (log.isDebugEnabled()) {
                log.debug("start poll message: {}", i);
            }
            ConsumerRecords<String, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                Header[] headers = record.headers().toArray();
                for (int j = 1; j <= 2; j++) {
                    String k = headers[j - 1].key();
                    String v = new String(headers[j - 1].value(), UTF_8);

                    if (log.isDebugEnabled()) {
                        log.debug("headers key: {}, value:{}", k, v);
                    }
                    assertTrue(k.contains(key1) || k.contains(key2));
                    assertTrue(v.contains(value1) || v.contains(value2));
                }
                if (log.isDebugEnabled()) {
                    log.debug("Kafka consumer get message: key: {}, value: {} at offset {}",
                            record.key(), record.value(), record.offset());
                }
                i++;
            }
        }
        assertEquals(i, totalMsgs);

        // no more records
        ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
    }
}
