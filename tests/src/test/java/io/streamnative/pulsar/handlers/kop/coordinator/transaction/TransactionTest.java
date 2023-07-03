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
package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import com.google.common.collect.ImmutableMap;
import io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import io.streamnative.pulsar.handlers.kop.scala.Either;
import io.streamnative.pulsar.handlers.kop.storage.PartitionLog;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.protocol.Errors;
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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.testng.Assert.*;

/**
 * Transaction test.
 */
@Slf4j
public class TransactionTest extends KopProtocolHandlerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        this.conf.setDefaultNumberOfNamespaceBundles(4);
        this.conf.setOffsetsTopicNumPartitions(10);
        this.conf.setKafkaTxnLogTopicNumPartitions(10);
        this.conf.setKafkaTxnProducerStateTopicNumPartitions(10);
        this.conf.setKafkaTransactionCoordinatorEnabled(true);
        this.conf.setBrokerDeduplicationEnabled(true);

        // enable tx expiration, but producers have
        // a very long TRANSACTION_TIMEOUT_CONFIG
        // so they won't expire by default
        this.conf.setKafkaTransactionalIdExpirationMs(5000);
        this.conf.setKafkaTransactionalIdExpirationEnable(true);
        this.conf.setTopicLevelPoliciesEnabled(false);
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

    @Test(timeOut = 1000 * 30, dataProvider = "produceConfigProvider")
    public void readCommittedTest(boolean isBatch) throws Exception {
        basicProduceAndConsumeTest("read-committed-test", "txn-11", "read_committed", isBatch);
    }

    @Test(timeOut = 1000 * 30, dataProvider = "produceConfigProvider")
    public void readUncommittedTest(boolean isBatch) throws Exception {
        basicProduceAndConsumeTest("read-uncommitted-test", "txn-12", "read_uncommitted", isBatch);
    }

    @Test(timeOut = 1000 * 30)
    public void testInitTransaction() {
        final KafkaProducer<Integer, String> producer = buildTransactionProducer("prod-1");

        producer.initTransactions();
        producer.close();
    }

    @Test(timeOut = 1000 * 30)
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

    private void basicProduceAndConsumeTest(String topicName,
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

        final int expected;
        switch (isolation) {
            case "read_committed":
                expected = totalTxnCount * messageCountPerTxn / 2;
                break;
            case "read_uncommitted":
                expected = totalTxnCount * messageCountPerTxn;
                break;
            default:
                expected = -1;
                fail();
        }
        consumeTxnMessage(topicName, expected, lastMessage, isolation);
    }

    private List<String> consumeTxnMessage(String topicName,
                                           int totalMessageCount,
                                           String lastMessage,
                                           String isolation) throws InterruptedException {
        return consumeTxnMessage(topicName,
                totalMessageCount,
                lastMessage,
                isolation,
                "test_consumer");
    }

    private List<String> consumeTxnMessage(String topicName,
                                           int totalMessageCount,
                                           String lastMessage,
                                           String isolation,
                                           String group) throws InterruptedException {
        @Cleanup
        KafkaConsumer<Integer, String> consumer = buildTransactionConsumer(group, isolation);
        consumer.subscribe(Collections.singleton(topicName));

        List<String> messages = new ArrayList<>();

        log.info("waiting for message {} in topic {}", lastMessage, topicName);
        AtomicInteger receiveCount = new AtomicInteger(0);
        while (true) {
            ConsumerRecords<Integer, String> consumerRecords =
                    consumer.poll(Duration.of(100, ChronoUnit.MILLIS));

            boolean readFinish = false;
            for (ConsumerRecord<Integer, String> record : consumerRecords) {
                log.info("Fetch for receive record offset: {}, key: {}, value: {}",
                        record.offset(), record.key(), record.value());
                if (isolation.equals("read_committed")) {
                    assertFalse(record.value().contains("abort"), "in read_committed isolation "
                            + "we read a message that should have been aborted: " + record.value());
                }
                receiveCount.incrementAndGet();
                messages.add(record.value());
                if (lastMessage.equalsIgnoreCase(record.value())) {
                    log.info("received the last message");
                    readFinish = true;
                }
            }

            if (readFinish) {
                log.info("Fetch for read finish.");
                break;
            }
        }
        log.info("Fetch for receive message finish. isolation: {}, receive count: {} messages {}",
                isolation, receiveCount.get(), messages);
        Assert.assertEquals(receiveCount.get(), totalMessageCount, "messages: " + messages);
        log.info("Fetch for finish consume messages. isolation: {}", isolation);

        return messages;
    }

    @Test(timeOut = 1000 * 15)
    public void offsetCommitTest() throws Exception {
        txnOffsetTest("txn-offset-commit-test", 10, true);
    }

    @Test(timeOut = 3000 * 10)
    public void offsetAbortTest() throws Exception {
        txnOffsetTest("txn-offset-abort-test", 10, false);
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
                    fail("The transaction was committed, the consumer shouldn't receive any more messages.");
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

    @DataProvider(name = "basicRecoveryTestAfterTopicUnloadNumTransactions")
    protected static Object[][] basicRecoveryTestAfterTopicUnloadNumTransactions() {
        // isBatch
        return new Object[][]{
                {0},
                {3},
                {5}
        };
    }


    @Test(timeOut = 1000 * 30, dataProvider = "basicRecoveryTestAfterTopicUnloadNumTransactions")
    public void basicRecoveryTestAfterTopicUnload(int numTransactionsBetweenSnapshots) throws Exception {

        String topicName = "basicRecoveryTestAfterTopicUnload_" + numTransactionsBetweenSnapshots;
        String transactionalId = "myProducer_" + UUID.randomUUID();
        String isolation = "read_committed";
        boolean isBatch = false;

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

            if (numTransactionsBetweenSnapshots > 0
                    && (txnIndex % numTransactionsBetweenSnapshots) == 0) {
                // force take snapshot
                takeSnapshot(topicName);
            }

            if (txnIndex % 2 != 0) {
                producer.commitTransaction();
            } else {
                producer.abortTransaction();
            }
        }

        waitForTransactionsToBeInStableState(transactionalId);

        // unload the namespace, this will force a recovery
        pulsar.getAdminClient().namespaces().unload(namespace);

        final int expected =  totalTxnCount * messageCountPerTxn / 2;
        consumeTxnMessage(topicName, expected, lastMessage, isolation);
    }


    private TransactionState dumpTransactionState(String transactionalId) {
        KafkaProtocolHandler protocolHandler = (KafkaProtocolHandler)
                pulsar.getProtocolHandlers().protocol("kafka");
        TransactionCoordinator transactionCoordinator =
                protocolHandler.getTransactionCoordinator(tenant);
        Either<Errors, Optional<TransactionStateManager.CoordinatorEpochAndTxnMetadata>> transactionState =
                transactionCoordinator.getTxnManager().getTransactionState(transactionalId);
        log.debug("transactionalId {} status {}", transactionalId, transactionState);
        assertFalse(transactionState.isLeft(), "transaction "
                + transactionalId + " error " + transactionState.getLeft());
        return transactionState.getRight().get().getTransactionMetadata().getState();
    }

    private void waitForTransactionsToBeInStableState(String transactionalId) {
        KafkaProtocolHandler protocolHandler = (KafkaProtocolHandler)
                pulsar.getProtocolHandlers().protocol("kafka");
        TransactionCoordinator transactionCoordinator =
                protocolHandler.getTransactionCoordinator(tenant);
        Awaitility.await().untilAsserted(() -> {
            Either<Errors, Optional<TransactionStateManager.CoordinatorEpochAndTxnMetadata>> transactionState =
                    transactionCoordinator.getTxnManager().getTransactionState(transactionalId);
            log.debug("transactionalId {} status {}", transactionalId, transactionState);
            assertFalse(transactionState.isLeft());
            TransactionState state = transactionState.getRight()
                    .get().getTransactionMetadata().getState();
            boolean isStable;
            switch (state) {
                case COMPLETE_COMMIT:
                case COMPLETE_ABORT:
                case EMPTY:
                    isStable = true;
                    break;
                default:
                    isStable = false;
                    break;
            }
            assertTrue(isStable, "Transaction " + transactionalId
                    + " is not stable to reach a stable state, is it " + state);
        });
    }

    private void takeSnapshot(String topicName) throws Exception {
        KafkaProtocolHandler protocolHandler = (KafkaProtocolHandler)
                pulsar.getProtocolHandlers().protocol("kafka");

        int numPartitions =
                admin.topics().getPartitionedTopicMetadata(topicName).partitions;
        for (int i = 0; i < numPartitions; i++) {
            PartitionLog partitionLog = protocolHandler
                    .getReplicaManager()
                    .getPartitionLog(new TopicPartition(topicName, i), tenant + "/" + namespace);

            // we can only take the snapshot on the only thread that is allowed to process mutations
            // on the state
            partitionLog
                    .takeProducerSnapshot()
                    .get();

        }
    }

    @Test(timeOut = 1000 * 30, dataProvider = "basicRecoveryTestAfterTopicUnloadNumTransactions")
    public void basicTestWithTopicUnload(int numTransactionsBetweenUnloads) throws Exception {

        String topicName = "basicTestWithTopicUnload_" + numTransactionsBetweenUnloads;
        String transactionalId = "myProducer_" + UUID.randomUUID();
        String isolation = "read_committed";
        boolean isBatch = false;

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

            if (numTransactionsBetweenUnloads > 0
                    && (txnIndex % numTransactionsBetweenUnloads) == 0) {

                // dump the state before un load, this helps troubleshooting
                // problems in case of flaky test
                TransactionState transactionState = dumpTransactionState(transactionalId);
                assertEquals(TransactionState.ONGOING, transactionState);

                // unload the namespace, this will force a recovery
                pulsar.getAdminClient().namespaces().unload(namespace);
            }

            if (txnIndex % 2 != 0) {
                producer.commitTransaction();
            } else {
                producer.abortTransaction();
            }
        }


        final int expected = totalTxnCount * messageCountPerTxn / 2;
        consumeTxnMessage(topicName, expected, lastMessage, isolation);
    }

    @DataProvider(name = "takeSnapshotBeforeRecovery")
    protected static Object[][] takeSnapshotBeforeRecovery() {
        // isBatch
        return new Object[][]{
                {true},
                {false}
        };
    }

    @Test(timeOut = 1000 * 20, dataProvider = "takeSnapshotBeforeRecovery")
    public void basicRecoveryAbortedTransaction(boolean takeSnapshotBeforeRecovery) throws Exception {

        String topicName = "basicRecoveryAbortedTransaction_" + takeSnapshotBeforeRecovery;
        String transactionalId = "myProducer" + UUID.randomUUID();
        String isolation = "read_committed";

        String namespace = TopicName.get(topicName).getNamespace();

        @Cleanup
        KafkaProducer<Integer, String> producer = buildTransactionProducer(transactionalId);

        producer.initTransactions();


        producer.beginTransaction();

        String firstMessage = "aborted msg 1";

        producer.send(new ProducerRecord<>(topicName, 0, firstMessage)).get();
        producer.flush();
        // force take snapshot
        takeSnapshot(topicName);

        // recovery will re-process the topic from this point onwards
        String secondMessage = "aborted msg 2";
        producer.send(new ProducerRecord<>(topicName, 0, secondMessage)).get();

        producer.abortTransaction();

        producer.beginTransaction();
        String lastMessage = "committed mgs";
        producer.send(new ProducerRecord<>(topicName, 0, "foo")).get();
        producer.send(new ProducerRecord<>(topicName, 0, lastMessage)).get();
        producer.commitTransaction();

        if (takeSnapshotBeforeRecovery) {
            takeSnapshot(topicName);
        }

        // unload the namespace, this will force a recovery
        pulsar.getAdminClient().namespaces().unload(namespace);

        consumeTxnMessage(topicName, 2, lastMessage, isolation);
    }

    @Test(timeOut = 1000 * 30, dataProvider = "takeSnapshotBeforeRecovery")
    public void basicRecoveryAbortedTransactionDueToProducerFenced(boolean takeSnapshotBeforeRecovery)
            throws Exception {

        String topicName = "basicRecoveryAbortedTransactionDueToProducerFenced_" + takeSnapshotBeforeRecovery;
        String transactionalId = "myProducer" + UUID.randomUUID();
        String isolation = "read_committed";

        String namespace = TopicName.get(topicName).getNamespace();

        @Cleanup
        KafkaProducer<Integer, String> producer = buildTransactionProducer(transactionalId);

        producer.initTransactions();

        producer.beginTransaction();

        String firstMessage = "aborted msg 1";

        producer.send(new ProducerRecord<>(topicName, 0, firstMessage)).get();
        producer.flush();
        // force take snapshot
        takeSnapshot(topicName);

        // recovery will re-process the topic from this point onwards
        String secondMessage = "aborted msg 2";
        producer.send(new ProducerRecord<>(topicName, 0, secondMessage)).get();


        @Cleanup
        KafkaProducer<Integer, String> producer2 = buildTransactionProducer(transactionalId);
        producer2.initTransactions();

        // the transaction is automatically aborted, because the first instance of the
        // producer has been fenced
        expectThrows(ProducerFencedException.class, () -> {
            producer.commitTransaction();
        });


        producer2.beginTransaction();
        String lastMessage = "committed mgs";
        producer2.send(new ProducerRecord<>(topicName, 0, "foo")).get();
        producer2.send(new ProducerRecord<>(topicName, 0, lastMessage)).get();
        producer2.commitTransaction();

        if (takeSnapshotBeforeRecovery) {
            // force take snapshot
            takeSnapshot(topicName);
        }

        // unload the namespace, this will force a recovery
        pulsar.getAdminClient().namespaces().unload(namespace);

        consumeTxnMessage(topicName, 2, lastMessage, isolation);
    }


    @Test(timeOut = 1000 * 30, dataProvider = "takeSnapshotBeforeRecovery")
    public void basicRecoveryAbortedTransactionDueToProducerTimedOut(boolean takeSnapshotBeforeRecovery)
            throws Exception {

        String topicName = "basicRecoveryAbortedTransactionDueToProducerTimedOut_" + takeSnapshotBeforeRecovery;
        String transactionalId = "myProducer" + UUID.randomUUID();
        String isolation = "read_committed";

        String namespace = TopicName.get(topicName).getNamespace();

        @Cleanup
        KafkaProducer<Integer, String> producer = buildTransactionProducer(transactionalId, 1000);

        producer.initTransactions();

        producer.beginTransaction();

        String firstMessage = "aborted msg 1";

        producer.send(new ProducerRecord<>(topicName, 0, firstMessage)).get();
        producer.flush();
        // force take snapshot
        takeSnapshot(topicName);

        // recovery will re-process the topic from this point onwards
        String secondMessage = "aborted msg 2";
        producer.send(new ProducerRecord<>(topicName, 0, secondMessage)).get();

        Thread.sleep(conf.getKafkaTransactionalIdExpirationMs() + 5000);

        // the transaction is automatically aborted, because of producer timeout
        expectThrows(ProducerFencedException.class, () -> {
            producer.commitTransaction();
        });

        @Cleanup
        KafkaProducer<Integer, String> producer2 = buildTransactionProducer(transactionalId, 1000);
        producer2.initTransactions();
        producer2.beginTransaction();
        String lastMessage = "committed mgs";
        producer2.send(new ProducerRecord<>(topicName, 0, "foo")).get();
        producer2.send(new ProducerRecord<>(topicName, 0, lastMessage)).get();
        producer2.commitTransaction();

        if (takeSnapshotBeforeRecovery) {
            // force take snapshot
            takeSnapshot(topicName);
        }

        // unload the namespace, this will force a recovery
        pulsar.getAdminClient().namespaces().unload(namespace);

        consumeTxnMessage(topicName, 2, lastMessage, isolation);
    }

    /**
     * TODO: Disable for now, we need introduce UUID for topic.
     */
    @Test(timeOut = 1000 * 20, enabled = false)
    public void basicRecoveryAfterDeleteCreateTopic()
            throws Exception {

        String topicName = "basicRecoveryAfterDeleteCreateTopic";
        String transactionalId = "myProducer-deleteCreate";
        String isolation = "read_committed";

        TopicName fullTopicName = TopicName.get(topicName);

        String namespace = fullTopicName.getNamespace();

        // use Kafka API, this way we assign a topic UUID
        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(newKafkaAdminClientProperties());
        kafkaAdmin.createTopics(Arrays.asList(new NewTopic(topicName, 4, (short) 1)));

        @Cleanup
        KafkaProducer<Integer, String> producer = buildTransactionProducer(transactionalId, 1000);

        producer.initTransactions();

        KafkaProtocolHandler protocolHandler = (KafkaProtocolHandler)
                pulsar.getProtocolHandlers().protocol("kafka");

        producer.beginTransaction();

        producer.send(new ProducerRecord<>(topicName, 0, "deleted msg 1")).get();
        producer.send(new ProducerRecord<>(topicName, 0, "deleted msg 1")).get();
        producer.send(new ProducerRecord<>(topicName, 0, "deleted msg 1")).get();
        producer.send(new ProducerRecord<>(topicName, 0, "deleted msg 1")).get();
        producer.send(new ProducerRecord<>(topicName, 0, "deleted msg 1")).get();
        producer.send(new ProducerRecord<>(topicName, 0, "deleted msg 1")).get();
        producer.send(new ProducerRecord<>(topicName, 0, "deleted msg 1")).get();
        producer.send(new ProducerRecord<>(topicName, 0, "deleted msg 1")).get();
        producer.send(new ProducerRecord<>(topicName, 0, "deleted msg 1")).get();
        producer.flush();

        // force take snapshot
        takeSnapshot(topicName);

        String secondMessage = "deleted msg 2";
        producer.send(new ProducerRecord<>(topicName, 0, secondMessage)).get();
        producer.flush();

        // verify that a non-transactional consumer can read the messages
        consumeTxnMessage(topicName, 10, secondMessage, "read_uncommitted",
                "uncommitted_reader1");

        for (int i = 0; i < 10; i++) {
            log.info("************DELETE");
        }
        // delete/create
        pulsar.getAdminClient().namespaces().unload(namespace);
        admin.topics().deletePartitionedTopic(topicName, true);

        // unfortunately the PH is not notified of the deletion
        // so we unload the namespace in order to clear local references/caches
        pulsar.getAdminClient().namespaces().unload(namespace);

        protocolHandler.getReplicaManager().removePartitionLog(fullTopicName.getPartition(0).toString());
        protocolHandler.getReplicaManager().removePartitionLog(fullTopicName.getPartition(1).toString());
        protocolHandler.getReplicaManager().removePartitionLog(fullTopicName.getPartition(2).toString());
        protocolHandler.getReplicaManager().removePartitionLog(fullTopicName.getPartition(3).toString());

        // create the topic again, using the kafka APIs
        kafkaAdmin.createTopics(Arrays.asList(new NewTopic(topicName, 4, (short) 1)));

        // the snapshot now points to a offset that doesn't make sense in the new topic
        // because the new topic is empty

        @Cleanup
        KafkaProducer<Integer, String> producer2 = buildTransactionProducer(transactionalId, 1000);
        producer2.initTransactions();
        producer2.beginTransaction();
        String lastMessage = "committed mgs";

        // this "send" triggers recovery of the ProducerStateManager on the topic
        producer2.send(new ProducerRecord<>(topicName, 0, "good-message")).get();
        producer2.send(new ProducerRecord<>(topicName, 0, lastMessage)).get();
        producer2.commitTransaction();

        consumeTxnMessage(topicName, 2, lastMessage, isolation, "readcommitter-reader-1");
    }

    @Test(timeOut = 60000)
    public void testRecoverFromInvalidSnapshotAfterTrim() throws Exception {

        String topicName = "testRecoverFromInvalidSnapshotAfterTrim";
        String transactionalId = "myProducer_" + UUID.randomUUID();
        String isolation = "read_committed";

        TopicName fullTopicName = TopicName.get(topicName);

        pulsar.getAdminClient().topics().createPartitionedTopic(topicName, 1);

        String namespace = fullTopicName.getNamespace();
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        String namespacePrefix = namespace;

        KafkaProducer<Integer, String> producer = buildTransactionProducer(transactionalId);

        producer.initTransactions();

        producer.beginTransaction();
        producer.send(new ProducerRecord<>(topicName, 0, "aborted 1")).get(); // OFFSET 0
        producer.flush();
        producer.abortTransaction(); // OFFSET 1

        producer.beginTransaction();
        String lastMessage = "msg1b";
        producer.send(new ProducerRecord<>(topicName, 0, "msg1")).get(); // OFFSET 2
        producer.send(new ProducerRecord<>(topicName, 0, lastMessage)).get(); // OFFSET 3
        producer.commitTransaction(); // OFFSET 4

        assertEquals(
                consumeTxnMessage(topicName, 2, lastMessage, isolation, "first_group"),
                List.of("msg1", "msg1b"));

        waitForTransactionsToBeInStableState(transactionalId);

        // unload and reload in order to have at least 2 ledgers in the
        // topic, this way we can drop the head ledger
        admin.namespaces().unload(namespace);

        producer.beginTransaction();
        producer.send(new ProducerRecord<>(topicName, 0, "msg2")).get(); // OFFSET 5
        producer.send(new ProducerRecord<>(topicName, 0, "msg3")).get(); // OFFSET 6
        producer.commitTransaction();  // OFFSET 7

        // take a snapshot now, it refers to the offset of the last written record
        takeSnapshot(topicName);

        waitForTransactionsToBeInStableState(transactionalId);

        admin.namespaces().unload(namespace);
        admin.lookups().lookupTopic(fullTopicName.getPartition(0).toString());

        KafkaProtocolHandler protocolHandler = (KafkaProtocolHandler)
                pulsar.getProtocolHandlers().protocol("kafka");
        PartitionLog partitionLog = protocolHandler
                .getReplicaManager()
                .getPartitionLog(topicPartition, namespacePrefix);
        partitionLog.awaitInitialisation().get();
        assertEquals(0L, partitionLog.fetchOldestAvailableIndexFromTopic().get().longValue());

        // all the messages up to here will be trimmed

        trimConsumedLedgers(fullTopicName.getPartition(0).toString());

        admin.namespaces().unload(namespace);
        admin.lookups().lookupTopic(fullTopicName.getPartition(0).toString());

        // continue writing, this triggers recovery
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(topicName, 0, "msg4")).get();  // OFFSET 8
        producer.send(new ProducerRecord<>(topicName, 0, "msg5")).get();  // OFFSET 9
        producer.commitTransaction();  // OFFSET 10
        producer.close();

        partitionLog = protocolHandler
                .getReplicaManager()
                .getPartitionLog(topicPartition, namespacePrefix);
        partitionLog.awaitInitialisation().get();
        assertEquals(8L, partitionLog.fetchOldestAvailableIndexFromTopic().get().longValue());

        // use a new consumer group, it will read from the beginning of the topic
        assertEquals(
                consumeTxnMessage(topicName, 2, "msg5", isolation, "second_group"),
                List.of("msg4", "msg5"));

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
            fail("The txn markers are not wrote.");
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
        return buildTransactionProducer(transactionalId, -1);
    }

    private KafkaProducer<Integer, String> buildTransactionProducer(String transactionalId, int txTimeout) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaServerAdder());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        if (txTimeout > 0) {
            producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, txTimeout);
        } else {
            // very long time-out
            producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 600 * 1000);
        }
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
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
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


    @Test
    public void testProducerFencedWhileSendFirstRecord() throws Exception {
        final KafkaProducer<Integer, String> producer1 = buildTransactionProducer("prod-1");
        producer1.initTransactions();
        producer1.beginTransaction();

        final KafkaProducer<Integer, String> producer2 = buildTransactionProducer("prod-1");
        producer2.initTransactions();
        producer2.beginTransaction();
        producer2.send(new ProducerRecord<>("test", "test")).get();

        assertThat(
                expectThrows(ExecutionException.class, () -> {
                    producer1.send(new ProducerRecord<>("test", "test"))
                            .get();
                }).getCause(), instanceOf(ProducerFencedException.class));

        producer1.close();
        producer2.close();
    }

    @Test
    public void testProducerFencedWhileCommitTransaction() throws Exception {
        final KafkaProducer<Integer, String> producer1 = buildTransactionProducer("prod-1");
        producer1.initTransactions();
        producer1.beginTransaction();
        producer1.send(new ProducerRecord<>("test", "test"))
                .get();

        final KafkaProducer<Integer, String> producer2 = buildTransactionProducer("prod-1");
        producer2.initTransactions();
        producer2.beginTransaction();
        producer2.send(new ProducerRecord<>("test", "test")).get();


        // producer1 is still able to write (TODO: this should throw a InvalidProducerEpochException)
        producer1.send(new ProducerRecord<>("test", "test")).get();

        // but it cannot commit
        expectThrows(ProducerFencedException.class, () -> {
            producer1.commitTransaction();
        });

        // producer2 can commit
        producer2.commitTransaction();
        producer1.close();
        producer2.close();
    }

    @Test
    public void testProducerFencedWhileSendOffsets() throws Exception {
        final KafkaProducer<Integer, String> producer1 = buildTransactionProducer("prod-1");
        producer1.initTransactions();
        producer1.beginTransaction();
        producer1.send(new ProducerRecord<>("test", "test"))
                .get();

        final KafkaProducer<Integer, String> producer2 = buildTransactionProducer("prod-1");
        producer2.initTransactions();
        producer2.beginTransaction();
        producer2.send(new ProducerRecord<>("test", "test")).get();


        // producer1 cannot offsets
        expectThrows(ProducerFencedException.class, () -> {
            producer1.sendOffsetsToTransaction(ImmutableMap.of(new TopicPartition("test", 0),
                            new OffsetAndMetadata(0L)),
                    "testGroup");
        });

        // and it cannot commit
        expectThrows(ProducerFencedException.class, () -> {
            producer1.commitTransaction();
        });

        producer1.close();
        producer2.close();
    }

    @Test
    public void testProducerFencedWhileAbortAndBegin() throws Exception {
        final KafkaProducer<Integer, String> producer1 = buildTransactionProducer("prod-1");
        producer1.initTransactions();
        producer1.beginTransaction();
        producer1.send(new ProducerRecord<>("test", "test"))
                .get();

        final KafkaProducer<Integer, String> producer2 = buildTransactionProducer("prod-1");
        producer2.initTransactions();
        producer2.beginTransaction();
        producer2.send(new ProducerRecord<>("test", "test")).get();

        // producer1 cannot abort
        expectThrows(ProducerFencedException.class, () -> {
            producer1.abortTransaction();
        });

        // producer1 cannot start a new transaction
        expectThrows(ProducerFencedException.class, () -> {
            producer1.beginTransaction();
        });
        producer1.close();
        producer2.close();
    }

    @Test
    public void testNotFencedWithBeginTransaction() throws Exception {
        final KafkaProducer<Integer, String> producer1 = buildTransactionProducer("prod-1");
        producer1.initTransactions();

        final KafkaProducer<Integer, String> producer2 = buildTransactionProducer("prod-1");
        producer2.initTransactions();
        producer2.beginTransaction();
        producer2.send(new ProducerRecord<>("test", "test")).get();

        // beginTransaction doesn't do anything
        producer1.beginTransaction();

        producer1.close();
        producer2.close();
    }

    /**
     * Get the Kafka server address.
     */
    private String getKafkaServerAdder() {
        return "localhost:" + getClientPort();
    }

    protected void addCustomizeProps(Properties producerProps) {
        // No-op
    }

    @DataProvider(name = "isolationProvider")
    protected Object[][] isolationProvider() {
        return new Object[][]{
                {"read_committed"},
                {"read_uncommitted"},
        };
    }

    @Test(dataProvider = "isolationProvider", timeOut = 1000 * 30)
    public void readUnstableMessagesTest(String isolation) throws InterruptedException, ExecutionException {
        String topic = "unstable-message-test-" + RandomStringUtils.randomAlphabetic(5);

        KafkaConsumer<Integer, String> consumer = buildTransactionConsumer("unstable-read", isolation);
        consumer.subscribe(Collections.singleton(topic));

        String tnxId = "txn-" + RandomStringUtils.randomAlphabetic(5);
        KafkaProducer<Integer, String> producer = buildTransactionProducer(tnxId);
        producer.initTransactions();

        String baseMsg = "test msg commit - ";
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(topic, baseMsg + 0)).get();
        producer.send(new ProducerRecord<>(topic, baseMsg + 1)).get();
        producer.flush();

        AtomicInteger messageCount = new AtomicInteger(0);
        // make sure consumer can't receive unstable messages in `read_committed` mode
        readAndCheckMessages(consumer, baseMsg, messageCount, isolation.equals("read_committed") ? 0 : 2);

        producer.commitTransaction();
        producer.beginTransaction();
        // these two unstable message shouldn't be received in `read_committed` mode
        producer.send(new ProducerRecord<>(topic, baseMsg + 2)).get();
        producer.send(new ProducerRecord<>(topic, baseMsg + 3)).get();
        producer.flush();

        readAndCheckMessages(consumer, baseMsg, messageCount, isolation.equals("read_committed") ? 2 : 4);

        consumer.close();
        producer.close();
    }

    private void readAndCheckMessages(KafkaConsumer<Integer, String> consumer, String baseMsg,
                                      AtomicInteger messageCount, int expectedMessageCount) {
        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(3));
            if (records.isEmpty()) {
                break;
            }
            for (ConsumerRecord<Integer, String> record : records) {
                assertEquals(record.value(), baseMsg + messageCount.getAndIncrement());
            }
        }
        // make sure there is no message can be received
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(3));
        assertTrue(records.isEmpty());
        // make sure only receive the expected number of stable messages
        assertEquals(messageCount.get(), expectedMessageCount);
    }

}
