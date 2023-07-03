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
package io.streamnative.pulsar.handlers.kop.admin;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataManager.BaseKey;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataManager.OffsetKey;
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests for handling out-of-range cases.
 **/
@Slf4j
public class OffsetResetTest extends KopProtocolHandlerTestBase {

    public OffsetResetTest() {
        super("kafka");
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 30000)
    public void testGreaterThanEndOffset() throws Exception {
        final String topic = "public/default/test-reset-offset-topic";
        final String group = "test-reset-offset-groupid";
        final int numPartitions = 1;

        // step1: create topic, produce some messages and consume until the end
        admin.topics().createPartitionedTopic(topic, numPartitions);

        KProducer kProducer = new KProducer(topic, false, getKafkaBrokerPort());

        int totalMsgs = 10;
        String messageStrPrefix = topic + "_message_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer().send(new ProducerRecord<>(topic, i, messageStr));
        }
        kProducer.close();
        log.info("finish producing");

        KConsumer kConsumer = new KConsumer(topic, getKafkaBrokerPort(), group);
        kConsumer.getConsumer().subscribe(Collections.singleton(topic));

        int msgs = 0;
        while (msgs < totalMsgs) {
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                msgs++;
            }
        }
        assertEquals(msgs, totalMsgs);
        kConsumer.getConsumer().commitSync();
        log.info("finish consuming");

        // no more records
        ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        kConsumer.close();

        // step2: delete the topic
        admin.topics().deletePartitionedTopic(topic);
        log.info("finish deleting");

        // step3: re-create the topic
        admin.topics().createPartitionedTopic(topic, numPartitions);
        log.info("finish re-creating");

        // step4: re-produce the half of total messages
        kProducer = new KProducer(topic, false, getKafkaBrokerPort());
        for (int i = 0; i < totalMsgs / 2; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer().send(new ProducerRecord<>(topic, i, messageStr));
        }
        log.info("finish re-producing");

        // step5: check offset info
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        AdminClient adminClient = AdminClient.create(properties);

        kConsumer = new KConsumer(topic, getKafkaBrokerPort(), group);
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        long offset = adminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get()
                .get(topicPartition).offset();
        long leo = (long) kConsumer.getConsumer().endOffsets(Collections.singletonList(topicPartition))
                .get(topicPartition);
        log.info("offset:{}, leo:{}, lag:{}", offset, leo, leo - offset);

        // step6: re-consume
        kConsumer.getConsumer().subscribe(Collections.singleton(topic));
        msgs = 0;
        while (msgs < totalMsgs / 2) {
            records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                msgs++;
            }
        }
        assertEquals(msgs, totalMsgs / 2);
        kConsumer.getConsumer().commitSync();
        log.info("finish re-consuming");

        offset = adminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get()
                .get(topicPartition).offset();
        leo = (long) kConsumer.getConsumer().endOffsets(Collections.singletonList(topicPartition))
                .get(topicPartition);
        log.info("offset:{}, leo:{}, lag:{}", offset, leo, leo - offset);

        kProducer.close();
        kConsumer.close();
        adminClient.close();
    }

    @Test(timeOut = 30000)
    @Ignore
    public void testLessThanStartOffset() throws Exception {
        final String topic = "persistent://public/default/test-reset-offset-topic";
        final String group = "test-reset-offset-groupid";
        final int numPartitions = 1;

        // step1: create topic, produce some messages and consume to the end
        admin.topics().createPartitionedTopic(topic, numPartitions);
        TopicPartition topicPartition = new TopicPartition(topic, 0);

        KProducer kProducer = new KProducer(topic, false, getKafkaBrokerPort());

        int firstLedgerMsgs = 10;
        String messageStrPrefix = topic + "_message_";

        for (int i = 0; i < firstLedgerMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer().send(new ProducerRecord<>(topic, i, messageStr));
        }
        log.info("finish producing first ledger messages");

        KConsumer kConsumer = new KConsumer(topic, getKafkaBrokerPort(), group);
        kConsumer.getConsumer().subscribe(Collections.singleton(topic));
        int msgs = 0;
        while (msgs < firstLedgerMsgs) {
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                msgs++;
            }
        }
        kConsumer.getConsumer().commitSync();
        assertEquals(msgs, firstLedgerMsgs);
        log.info("finish consuming first ledger messages");

        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        AdminClient adminClient = AdminClient.create(properties);

        long offset = adminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get()
                .get(topicPartition).offset();
        long leo = (long) kConsumer.getConsumer().endOffsets(Collections.singletonList(topicPartition))
                .get(topicPartition);
        log.info("offset:{}, leo:{}, lag:{}", offset, leo, leo - offset);

        // step2: force to trigger rollover and retention
        // Note: pulsar format topic name is required
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topic + "-partition-0").get().get();
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();

        ManagedLedgerConfig managedLedgerConfig = managedLedger.getConfig();
        int maxEntriesPerLedger = managedLedgerConfig.getMaxEntriesPerLedger();
        long minimumRolloverTime = managedLedgerConfig.getMinimumRolloverTimeMs();
        long maximumRolloverTime = managedLedgerConfig.getMaximumRolloverTimeMs();
        long retentionTime = managedLedgerConfig.getRetentionTimeMillis();

        managedLedgerConfig.setMaxEntriesPerLedger(1);
        managedLedgerConfig.setMinimumRolloverTime(0, TimeUnit.MILLISECONDS);
        managedLedgerConfig.setMaximumRolloverTime(2, TimeUnit.MILLISECONDS);
        managedLedgerConfig.setRetentionTime(2, TimeUnit.MILLISECONDS);
        managedLedger.setConfig(managedLedgerConfig);

        log.info("current ledger ids: {}", managedLedger.getLedgersInfo().keySet());

        if (log.isDebugEnabled()) {
            log.debug("minimumRolloverTimeMs:{}, maximumRolloverTimeMs:{}",
                    managedLedgerConfig.getMinimumRolloverTimeMs(), managedLedgerConfig.getMaximumRolloverTimeMs());

            Method currentLedgerIsFull = managedLedger.getClass().getDeclaredMethod("currentLedgerIsFull");
            currentLedgerIsFull.setAccessible(true);
            log.debug("Reflect rollCurrentLedgerIfFull:{}", currentLedgerIsFull.invoke(managedLedger));
        }

        managedLedger.rollCurrentLedgerIfFull();
        managedLedger.trimConsumedLedgersInBackground(Futures.NULL_PROMISE);
        Thread.sleep(1000);
        log.info("current ledger ids: {}", managedLedger.getLedgersInfo().keySet());
        log.info("finish deleting some ledgers");

        // recovery configurations
        managedLedgerConfig.setMaxEntriesPerLedger(maxEntriesPerLedger);
        managedLedgerConfig.setMaximumRolloverTime((int) maximumRolloverTime, TimeUnit.MILLISECONDS);
        managedLedgerConfig.setMinimumRolloverTime((int) minimumRolloverTime, TimeUnit.MILLISECONDS);
        managedLedgerConfig.setRetentionTime((int) retentionTime, TimeUnit.MILLISECONDS);
        managedLedger.setConfig(managedLedgerConfig);

        // step3: produce second ledger messages
        int secondLedgerMsgs = 5;
        for (int i = firstLedgerMsgs; i < firstLedgerMsgs + secondLedgerMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer().send(new ProducerRecord<>(topic, i, messageStr));
        }
        log.info("finish producing second ledger messages");

        // step4: consume second ledger messages from offset 0
        kConsumer.getConsumer().seek(topicPartition, 0);
        msgs = 0;
        while (msgs < secondLedgerMsgs) {
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                log.info("2nd mesg: {}", record.value());
                msgs++;
            }
        }
        assertEquals(msgs, secondLedgerMsgs);
        log.info("finish consuming second ledger messages");

        offset = adminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get()
                .get(topicPartition).offset();
        leo = (long) kConsumer.getConsumer().endOffsets(Collections.singletonList(topicPartition))
                .get(topicPartition);
        log.info("offset:{}, leo:{}, lag:{}", offset, leo, leo - offset);

        kProducer.close();
        kConsumer.close();
        adminClient.close();
    }

    private long describeGroups(String group, String topic) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        AdminClient adminClient = AdminClient.create(properties);

        KafkaConsumer<Integer, String> consumer = new KConsumer(topic, getKafkaBrokerPort(), group).getConsumer();
        consumer.subscribe(Collections.singleton(topic));

        long lag = 0;

        try {
            StringBuffer buffer = new StringBuffer("Consumer group details:\n");
            for (TopicPartition topicPartition : consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition())).collect(Collectors.toList())) {
                log.info("offset part: {}", adminClient.listConsumerGroupOffsets(group)
                        .partitionsToOffsetAndMetadata().get());
                Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap =
                        adminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
                long offset = topicPartitionOffsetAndMetadataMap.get(topicPartition).offset();
                long leo = consumer.endOffsets(Collections.singletonList(topicPartition))
                        .get(topicPartition);
                lag += (leo - offset);
                buffer.append(String.format("offset:%d, leo:%d, lag:%d%n", offset, leo, leo - offset));
            }

            log.info("{}", buffer.toString());
            return lag;
        } catch (Exception e) {
            log.info("Unable to describe groups", e);
            return -1;
        }
    }

    // Simulate the Kafka command tools using Kafka's Java client since the Scala Kafka class' version might be
    // different with kafka-clients' version.
    private void resetTo(String topic, String group, String pos) {
        @Cleanup
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(newKafkaConsumerProperties(group));
        // notice we use the raw topic name here
        final String rawTopic = topic.substring(topic.lastIndexOf('/') + 1);
        final List<TopicPartition> topicPartitions = consumer.partitionsFor(rawTopic).stream()
                .map(partitionInfo -> new TopicPartition(rawTopic, partitionInfo.partition()))
                .collect(Collectors.toList());
        if (pos.equals("earliest")) {
            consumer.commitSync(CoreUtils.mapValue(
                    consumer.beginningOffsets(topicPartitions),
                    offset -> new OffsetAndMetadata(offset, "")
            ));
        } else {
            throw new IllegalArgumentException("resetTo only support earliest yet");
        }
    }

    private void readFromOffsetMessagePulsar() throws Exception {
        Reader<ByteBuffer> reader = pulsarClient
                .newReader(Schema.BYTEBUFFER)
                .startMessageId(MessageId.earliest)
                .topic("persistent://public/default/__consumer_offsets" + PARTITIONED_TOPIC_SUFFIX + "0")
                .readCompacted(true)
                .create();

        log.info("start reading __consumer_offsets:{}", reader.getTopic());
        while (true) {
            Message<ByteBuffer> offsetMetadata = reader.readNext(1, TimeUnit.SECONDS);
            if (offsetMetadata == null) {
                break;
            }

            MemoryRecords memRecords = MemoryRecords.readableRecords(offsetMetadata.getValue());
            log.info("__consumer_offsets MessageId:{}", offsetMetadata.getMessageId());

            for (MutableRecordBatch batch : memRecords.batches()) {
                for (Record record : batch) {
                    BaseKey baseKey = GroupMetadataConstants.readMessageKey(record.key());
                    if (baseKey != null && (baseKey instanceof OffsetKey)) {
                        OffsetKey offsetKey = (OffsetKey) baseKey;
                        String formattedValue = String.valueOf(GroupMetadataConstants
                                .readOffsetMessageValue(record.value()));
                        log.info("__consumer_offsets - key:{}, value:{}", offsetKey, formattedValue);
                    }
                }
            }
        }

        reader.close();
    }

    @Test(timeOut = 30000)
    public void testCliReset() throws Exception {
        String topic = "public/default/test-reset-offset-topic";
        final String group = "test-reset-offset-groupid";
        final int numPartitions = 10;

        // step1: create topic, produce some messages and consume until the end
        admin.topics().createPartitionedTopic(topic, numPartitions);

        KProducer kProducer = new KProducer(topic, false, getKafkaBrokerPort());

        int totalMsgs = 10;
        String messageStrPrefix = topic + "_message_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer().send(new ProducerRecord<>(topic, i, messageStr));
        }
        kProducer.close();
        log.info("finish producing");

        KConsumer kConsumer = new KConsumer(topic, getKafkaBrokerPort(), group);
        kConsumer.getConsumer().subscribe(Collections.singleton(topic));

        int msgs = 0;
        while (msgs < totalMsgs) {
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                msgs++;
            }
        }
        assertEquals(msgs, totalMsgs);
        kConsumer.getConsumer().commitSync();
        readFromOffsetMessagePulsar();
        assertEquals(describeGroups(group, topic), 0);

        // simulate the consumer has closed
        kConsumer.close();
        log.info("finish consuming");

        // step2: reset to earliest or latest
        resetTo(topic, group, "earliest");
        readFromOffsetMessagePulsar();
        // Check whether offset-reset works
        assertTrue(describeGroups(group, topic) > 0);
    }
}
