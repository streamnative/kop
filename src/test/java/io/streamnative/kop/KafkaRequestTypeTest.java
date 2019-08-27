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
package io.streamnative.kop;


import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import io.streamnative.kop.utils.MessageIdUtils;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for Different kafka request type.
 */
@Slf4j
public class KafkaRequestTypeTest extends MockKafkaServiceBaseTest {

    @DataProvider(name = "partitions")
    public static Object[][] partitions() {
        return new Object[][] { { 1 }, { 7 } };
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName,
                new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        } else {
            admin.clusters().updateCluster(configClusterName,
                new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        }

        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        } else {
            admin.tenants().updateTenant("public",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        }
        if (!admin.namespaces().getNamespaces("public").contains("public/default")) {
            admin.namespaces().createNamespace("public/default");
            admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/default",
                new RetentionPolicies(60, 1000));
        }
        if (!admin.namespaces().getNamespaces("public").contains("public/__kafka")) {
            admin.namespaces().createNamespace("public/__kafka");
            admin.namespaces().setNamespaceReplicationClusters("public/__kafka", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/__kafka",
                new RetentionPolicies(-1, -1));
        }
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000, dataProvider = "partitions")
    public void testKafkaProducePulsarConsume(int partitionNumber) throws Exception {
        String topicName = "kopKafkaProducePulsarConsume" + partitionNumber;
        String pulsarTopicName = "persistent://public/default/" + topicName;

        // create partitioned topic.
        kafkaService.getAdminClient().topics().createPartitionedTopic(topicName, partitionNumber);

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(pulsarTopicName)
            .subscriptionName("test_k_producer_k_pconsumer_sub")
            .subscribe();

        // 1. produce message with Kafka producer.
        @Cleanup
        KProducer kProducer = new KProducer(topicName, false, getKafkaBrokerPort());

        int totalMsgs = 10;
        String messageStrPrefix = "Message_Kop_KafkaProducePulsarConsume_"  + partitionNumber + "_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer()
                .send(new ProducerRecord<>(
                    topicName,
                    i,
                    messageStr))
                .get();
            log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
        }

        // 2. Consume messages use Pulsar client Consumer. verify content and key
        Message<byte[]> msg = null;
        for (int i = 0; i < totalMsgs; i++) {
            msg = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            Integer key = kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey()));
            assertEquals(messageStrPrefix + key.toString(), new String(msg.getValue()));

            log.debug("Pulsar consumer get message: {}, key: {}",
                new String(msg.getData()), kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey())).toString());
            consumer.acknowledge(msg);
        }

        // verify have received all messages
        msg = consumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);
    }

    @Test(timeOut = 20000, dataProvider = "partitions")
    public void testKafkaProduceKafkaConsume(int partitionNumber) throws Exception {
        String topicName = "kopKafkaProduceKafkaConsume" + partitionNumber;

        // create partitioned topic.
        kafkaService.getAdminClient().topics().createPartitionedTopic(topicName, partitionNumber);

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(topicName)
            .subscriptionName("test_k_producer_k_consumer_sub")
            .subscribe();

        // 1. produce message with Kafka producer.
        int totalMsgs = 10;
        String messageStrPrefix = "Message_Kop_KafkaProduceKafkaConsume_" + partitionNumber + "_";

        @Cleanup
        KProducer producer = new KProducer(topicName, false, getKafkaBrokerPort());

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            producer.getProducer()
                .send(new ProducerRecord<>(
                    topicName,
                    i,
                    messageStr))
                .get();
            log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
        }

        // 2. use kafka consumer to consume.
        @Cleanup
        KConsumer kConsumer = new KConsumer(topicName, getKafkaBrokerPort());
        List<TopicPartition> topicPartitions = IntStream.range(0, partitionNumber)
            .mapToObj(i -> new TopicPartition(topicName, i)).collect(Collectors.toList());
        log.info("Partition size: {}", topicPartitions.size());
        kConsumer.getConsumer().assign(topicPartitions);

        int i = 0;
        while (i < totalMsgs) {
            log.debug("start poll message: {}", i);
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                log.debug("Kafka consumer get message: {}, key: {} at offset {}",
                    record.key(), record.value(), record.offset());
                i++;
            }
        }
        assertEquals(i, totalMsgs);

        // no more records
        ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
    }

    @Test(timeOut = 20000, dataProvider = "partitions")
    public void testPulsarProduceKafkaConsume(int partitionNumber) throws Exception {
        String topicName = "kopPulsarProduceKafkaConsume" + partitionNumber;
        String pulsarTopicName = "persistent://public/default/" + topicName;

        // create partitioned topic.
        kafkaService.getAdminClient().topics().createPartitionedTopic(topicName, partitionNumber);

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

        // 2. use kafka consumer to consume.
        @Cleanup
        KConsumer kConsumer = new KConsumer(topicName, getKafkaBrokerPort());
        List<TopicPartition> topicPartitions = IntStream.range(0, partitionNumber)
            .mapToObj(i -> new TopicPartition(topicName, i)).collect(Collectors.toList());
        log.info("Partition size: {}", topicPartitions.size());
        kConsumer.getConsumer().assign(topicPartitions);

        int i = 0;
        while (i < totalMsgs) {
            log.debug("start poll message: {}", i);
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                log.debug("Kafka Consumer Received message: {}, {} at offset {}",
                    record.key(), record.value(), record.offset());
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
        kafkaService.getAdminClient().topics().createPartitionedTopic(topicName, partitionNumber);

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
            log.debug("start poll message: {}", i);
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                log.debug("Kafka Consumer Received message: {}, {} at offset {}",
                    record.key(), record.value(), record.offset());
                i++;
            }
        }
        kConsumer.close();

        // wait for offset commit complete
        Thread.sleep(1000);

        log.info("start another consumer, will consume from the left place of first consumer");
        KConsumer kConsumer2 = new KConsumer(topicName, getKafkaBrokerPort(), true);

        kConsumer2.getConsumer().subscribe(Collections.singletonList(topicName));
        while (i < totalMsgs) {
            log.debug("start poll message 2: {}", i);
            ConsumerRecords<Integer, String> records = kConsumer2.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                log.debug("Kafka Consumer Received message 2: {}, {} at offset {}",
                    record.key(), record.value(), record.offset());
                i++;
            }
        }

        assertEquals(i, totalMsgs);

        // no more records
        ConsumerRecords<Integer, String> records = kConsumer2.getConsumer().poll(Duration.ofSeconds(1));
        assertTrue(records.isEmpty());
    }


    // Test kafka topic consumer manager.
    // 1. topic has no entry, read no entry, tm has one cursor and target to read the first entry.
    // 2. produce entry, after read all entry. tm has one cursor, and target to read entry after lastEntry.
    // 3. has no entry to read again. tm has one cursor, and after each empty read, cursor read offset not changed.
    @Test(timeOut = 20000)
    public void testTopicConsumerManager() throws Exception {
        int partitionNumber = 1;
        String topicName = "testTopicConsumerManager" + partitionNumber;
        String pulsarTopicName = "persistent://public/default/" + topicName + PARTITIONED_TOPIC_SUFFIX + 0;

        // create partitioned topic.
        kafkaService.getAdminClient().topics().createPartitionedTopic(topicName, partitionNumber);

        int totalMsgs = 10;
        String messageStrPrefix = "Message_Kop_PulsarProduceKafkaConsume_" + partitionNumber + "_";

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
            .topic(pulsarTopicName)
            .enableBatching(false);
        @Cleanup
        Producer<byte[]> producer = producerBuilder.create();

        // above producer only created a topic, but with no data. consumer retry read but read no entry.
        @Cleanup
        KConsumer kConsumer = new KConsumer(topicName, getKafkaBrokerPort(), true);
        kConsumer.getConsumer().subscribe(Collections.singletonList(topicName));

        KafkaTopicConsumerManager tm = kafkaService
            .getKafkaTopicManager()
            .getTopicConsumerManager(pulsarTopicName)
            .get();

        // read empty entry will remove and add cursor each time.
        int i = 0;
        while (i < 7) {
            log.debug("start poll empty entry: {}", i);
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                log.debug("Kafka Consumer Received message: {}, {} at offset {}",
                    record.key(), record.value(), record.offset());
            }
            i++;
        }

        // expected tm only have one item. and entryId should be 0
        long size = tm.getConsumers().size();
        assertEquals(size, 1);
        tm.getConsumers().forEach((offset, cursor) -> {
            try {
                PositionImpl position = MessageIdUtils.getPosition(offset);
                long ledgerId = position.getLedgerId();
                assertNotEquals(ledgerId, 0);
                assertEquals(position.getEntryId(), 0);
                assertEquals(cursor.get().getRight(), Long.valueOf(offset));
            } catch (Exception e) {
                fail("should not throw exception");
            }
        });

        MessageId messageId = null;
        // produce some message
        for (i = 0; i < totalMsgs; i++) {
            String message = messageStrPrefix + i;
            messageId = producer.newMessage()
                .keyBytes(kafkaIntSerialize(Integer.valueOf(i)))
                .value(message.getBytes())
                .send();
        }

        i = 0;
        // receive all message.
        while (i < totalMsgs) {
            log.debug("start poll message: {}", i);
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                log.debug("Kafka Consumer Received message: {}, {} at offset {}",
                    record.key(), record.value(), record.offset());
                i++;
            }
        }

        // expect have one item, and offset equals to lastmessageId + 1
        size = tm.getConsumers().size();
        assertEquals(size, 1);

        MessageIdImpl lastMessageId = (MessageIdImpl) messageId;
        long ledgerId = lastMessageId.getLedgerId();
        long entryId = lastMessageId.getEntryId();
        long lastOffset = MessageIdUtils.getOffset(ledgerId, entryId + 1);
        CompletableFuture<Pair<ManagedCursor, Long>> cursor = tm.getConsumers().get(lastOffset);
        assertNotNull(cursor);
        assertEquals(cursor.get().getRight(), Long.valueOf(lastOffset));


        // After read all entry, read no entry again, this will remove and add cursor each time.
        i = 0;
        while (i < 7) {
            log.debug("start poll empty entry again: {}", i);
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                log.debug("Kafka Consumer Received message: {}, {} at offset {}",
                    record.key(), record.value(), record.offset());
            }
            i++;
        }

        // expect have one item, and offset equals to lastmessageId + 1
        size = tm.getConsumers().size();
        assertEquals(size, 1);
        cursor = tm.getConsumers().get(lastOffset);
        assertNotNull(cursor);
        assertEquals(cursor.get().getRight(), Long.valueOf(lastOffset));
    }

}
