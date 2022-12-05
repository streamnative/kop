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
import static org.testng.Assert.assertFalse;

import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionState;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionStateManager;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Transaction test.
 */
@Slf4j
public class TransactionTest extends KopProtocolHandlerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        this.conf.setDefaultNumberOfNamespaceBundles(4);
        this.conf.setOffsetsTopicNumPartitions(50);
        this.conf.setKafkaTxnLogTopicNumPartitions(50);
        this.conf.setKafkaTransactionCoordinatorEnabled(true);
        this.conf.setBrokerDeduplicationEnabled(true);
        super.internalSetup();
        log.info("success internal setup");
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "produceConfigProvider")
    protected static Object[][] produceConfigProvider() {
        // isBatch
        return new Object[][]{
                {true},
                {false}
        };
    }

    @Test(timeOut = 1000 * 10, dataProvider = "produceConfigProvider")
    public void readCommittedTest(boolean isBatch) throws Exception {
        basicProduceAndConsumeTest("read-committed-test", "txn-11", "read_committed", isBatch);
    }

    @Test(timeOut = 1000 * 10, dataProvider = "produceConfigProvider")
    public void readUncommittedTest(boolean isBatch) throws Exception {
        basicProduceAndConsumeTest("read-uncommitted-test", "txn-12", "read_uncommitted", isBatch);
    }

    @Test(timeOut = 1000 * 10)
    public void testInitTransaction() {
        final KafkaProducer<Integer, String> producer = buildTransactionProducer("prod-1");

        producer.initTransactions();
        producer.close();
    }

    @Test(timeOut = 1000 * 10)
    public void testMultiCommits() throws Exception {
        final String topic = "test-multi-commits";
        final KafkaProducer<Integer, String> producer1 = buildTransactionProducer("X1");
        final KafkaProducer<Integer, String> producer2 = buildTransactionProducer("X2");
        producer1.initTransactions();
        producer2.initTransactions();
        producer1.beginTransaction();
        producer2.beginTransaction();
        producer1.send(new ProducerRecord<>(topic, "msg-0")).get();
        producer2.send(new ProducerRecord<>(topic, "msg-1")).get();
        producer1.commitTransaction();
        producer2.commitTransaction();
        producer1.close();
        producer2.close();

        final TransactionStateManager stateManager = getProtocolHandler()
                .getTransactionCoordinator(conf.getKafkaTenant())
                .getTxnManager();
        final Function<String, TransactionState> getTransactionState = transactionalId ->
                Optional.ofNullable(stateManager.getTransactionState(transactionalId).getRight())
                        .map(optEpochAndMetadata -> optEpochAndMetadata.map(epochAndMetadata ->
                                epochAndMetadata.getTransactionMetadata().getState()).orElse(TransactionState.EMPTY))
                        .orElse(TransactionState.EMPTY);
        Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
                    assertEquals(getTransactionState.apply("X1"), TransactionState.COMPLETE_COMMIT);
                    assertEquals(getTransactionState.apply("X2"), TransactionState.COMPLETE_COMMIT);
                });
    }

    public void basicProduceAndConsumeTest(String topicName,
                                           String transactionalId,
                                           String isolation,
                                           boolean isBatch) throws Exception {
        @Cleanup
        KafkaProducer<Integer, String> producer = buildTransactionProducer(transactionalId);

        producer.initTransactions();

        int totalTxnCount = 10;
        int messageCountPerTxn = 10;

        String lastMessage = "";
        for (int txnIndex = 0; txnIndex < totalTxnCount; txnIndex++) {
            producer.beginTransaction();

            String contentBase;
            if (txnIndex % 2 != 0) {
                contentBase = "commit msg txnIndex %s messageIndex %s";
            } else {
                contentBase = "abort msg txnIndex %s messageIndex %s";
            }

            for (int messageIndex = 0; messageIndex < messageCountPerTxn; messageIndex++) {
                String msgContent = String.format(contentBase, txnIndex, messageIndex);
                log.info("send txn message {}", msgContent);
                lastMessage = msgContent;
                if (isBatch) {
                    producer.send(new ProducerRecord<>(topicName, messageIndex, msgContent));
                } else {
                    producer.send(new ProducerRecord<>(topicName, messageIndex, msgContent)).get();
                }
            }
            producer.flush();

            if (txnIndex % 2 != 0) {
                producer.commitTransaction();
            } else {
                producer.abortTransaction();
            }
        }

        consumeTxnMessage(topicName, totalTxnCount * messageCountPerTxn, lastMessage, isolation);
    }

    private void consumeTxnMessage(String topicName,
                                   int totalMessageCount,
                                   String lastMessage,
                                   String isolation) {
        @Cleanup
        KafkaConsumer<Integer, String> consumer = buildTransactionConsumer("test_consumer", isolation);
        consumer.subscribe(Collections.singleton(topicName));

        log.info("the last message is: {}", lastMessage);
        AtomicInteger receiveCount = new AtomicInteger(0);
        while (true) {
            ConsumerRecords<Integer, String> consumerRecords =
                    consumer.poll(Duration.of(100, ChronoUnit.MILLIS));

            boolean readFinish = false;
            for (ConsumerRecord<Integer, String> record : consumerRecords) {
                if (isolation.equals("read_committed")) {
                    assertFalse(record.value().contains("abort msg txnIndex"));
                }
                log.info("Fetch for receive record offset: {}, key: {}, value: {}",
                        record.offset(), record.key(), record.value());
                receiveCount.incrementAndGet();
                if (lastMessage.equalsIgnoreCase(record.value())) {
                    log.info("receive the last message");
                    readFinish = true;
                }
            }

            if (readFinish) {
                log.info("Fetch for read finish.");
                break;
            }
        }
        log.info("Fetch for receive message finish. isolation: {}, receive count: {}", isolation, receiveCount.get());

        if (isolation.equals("read_committed")) {
            Assert.assertEquals(receiveCount.get(), totalMessageCount / 2);
        } else {
            Assert.assertEquals(receiveCount.get(), totalMessageCount);
        }
        log.info("Fetch for finish consume messages. isolation: {}", isolation);
    }

    @Test(timeOut = 1000 * 15)
    public void offsetCommitTest() throws Exception {
        txnOffsetTest("txn-offset-commit-test", 10, true);
    }

    @Test(timeOut = 1000 * 10)
    public void offsetAbortTest() throws Exception {
        txnOffsetTest("txn-offset-abort-test", 10, false);
    }

    @Test
    public void basicRecoveryTestAfterTopicUnload() throws Exception {

        String topicName = "basicRecoveryTestAfterTopicUnload";
        String transactionalId = "myProducer";
        String isolation = "read_committed";
        boolean isBatch  = false;

        String namespace = TopicName.get(topicName).getNamespace();

        @Cleanup
        KafkaProducer<Integer, String> producer = buildTransactionProducer(transactionalId);

        producer.initTransactions();

        int totalTxnCount = 10;
        int messageCountPerTxn = 20;

        String lastMessage = "";
        for (int txnIndex = 0; txnIndex < totalTxnCount; txnIndex++) {
            producer.beginTransaction();

            String contentBase;
            if (txnIndex % 2 != 0) {
                contentBase = "commit msg txnIndex %s messageIndex %s";
            } else {
                contentBase = "abort msg txnIndex %s messageIndex %s";
            }

            for (int messageIndex = 0; messageIndex < messageCountPerTxn; messageIndex++) {
                String msgContent = String.format(contentBase, txnIndex, messageIndex);
                log.info("send txn message {}", msgContent);
                lastMessage = msgContent;
                if (isBatch) {
                    producer.send(new ProducerRecord<>(topicName, messageIndex, msgContent));
                } else {
                    producer.send(new ProducerRecord<>(topicName, messageIndex, msgContent)).get();
                }
            }
            producer.flush();

            if (txnIndex % 2 != 0) {
                producer.commitTransaction();
            } else {
                producer.abortTransaction();
            }
        }

        pulsar.getAdminClient().namespaces().unload(namespace);

        consumeTxnMessage(topicName, totalTxnCount * messageCountPerTxn, lastMessage, isolation);
    }

    public void txnOffsetTest(String topic, int messageCnt, boolean isCommit) throws Exception {
        String groupId = "my-group-id";

        List<String> sendMsgs = prepareData(topic, "first send message - ", messageCnt);

        // producer
        @Cleanup
        KafkaProducer<Integer, String> producer = buildTransactionProducer("12");

        // consumer
        @Cleanup
        KafkaConsumer<Integer, String> consumer = buildTransactionConsumer(groupId, "read_uncommitted");
        consumer.subscribe(Collections.singleton(topic));

        producer.initTransactions();
        producer.beginTransaction();

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        AtomicInteger msgCnt = new AtomicInteger(messageCnt);

        while (msgCnt.get() > 0) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
            for (ConsumerRecord<Integer, String> record : records) {
                log.info("receive message (first) - {}", record.value());
                Assert.assertEquals(sendMsgs.get(messageCnt - msgCnt.get()), record.value());
                msgCnt.decrementAndGet();
                offsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
            }
        }
        producer.sendOffsetsToTransaction(offsets, groupId);

        if (isCommit) {
            producer.commitTransaction();
            waitForTxnMarkerWriteComplete(offsets, consumer);
        } else {
            producer.abortTransaction();
        }

        resetToLastCommittedPositions(consumer);

        msgCnt = new AtomicInteger(messageCnt);
        while (msgCnt.get() > 0) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
            if (isCommit) {
                if (records.isEmpty()) {
                    msgCnt.decrementAndGet();
                } else {
                    Assert.fail("The transaction was committed, the consumer shouldn't receive any more messages.");
                }
            } else {
                for (ConsumerRecord<Integer, String> record : records) {
                    log.info("receive message (second) - {}", record.value());
                    Assert.assertEquals(sendMsgs.get(messageCnt - msgCnt.get()), record.value());
                    msgCnt.decrementAndGet();
                }
            }
        }
    }

    private List<String> prepareData(String sourceTopicName,
                                     String messageContent,
                                     int messageCount) throws ExecutionException, InterruptedException {
        // producer
        KafkaProducer<Integer, String> producer = buildIdempotenceProducer();

        List<String> sendMsgs = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            String msg = messageContent + i;
            sendMsgs.add(msg);
            producer.send(new ProducerRecord<>(sourceTopicName, i, msg)).get();
        }
        return sendMsgs;
    }

    private void waitForTxnMarkerWriteComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                                               KafkaConsumer<Integer, String> consumer) throws InterruptedException {
        AtomicBoolean flag = new AtomicBoolean();
        for (int i = 0; i < 5; i++) {
            flag.set(true);
            consumer.assignment().forEach(tp -> {
                OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
                if (offsetAndMetadata == null || !offsetAndMetadata.equals(offsets.get(tp))) {
                    flag.set(false);
                }
            });
            if (flag.get()) {
                break;
            }
            Thread.sleep(200);
        }
        if (!flag.get()) {
            Assert.fail("The txn markers are not wrote.");
        }
    }

    private static void resetToLastCommittedPositions(KafkaConsumer<Integer, String> consumer) {
        consumer.assignment().forEach(tp -> {
            OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
            if (offsetAndMetadata != null) {
                consumer.seek(tp, offsetAndMetadata.offset());
            } else {
                consumer.seekToBeginning(Collections.singleton(tp));
            }
        });
    }

    private KafkaProducer<Integer, String> buildTransactionProducer(String transactionalId) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaServerAdder());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        addCustomizeProps(producerProps);

        return new KafkaProducer<>(producerProps);
    }

    private KafkaConsumer<Integer, String> buildTransactionConsumer(String groupId, String isolation) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaServerAdder());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        addCustomizeProps(consumerProps);

        return new KafkaConsumer<>(consumerProps);
    }

    private KafkaProducer<Integer, String> buildIdempotenceProducer() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaServerAdder());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        addCustomizeProps(producerProps);
        return new KafkaProducer<>(producerProps);
    }

    /**
     * Get the Kafka server address.
     */
    private String getKafkaServerAdder() {
        return "localhost:" + getKafkaBrokerPort();
    }

    protected void addCustomizeProps(Properties producerProps) {
        // No-op
    }
}
