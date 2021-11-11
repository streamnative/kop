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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import io.streamnative.pulsar.handlers.kop.utils.timer.MockTime;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.mockito.ArgumentCaptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

/**
 * Transaction state manager test.
 */
@Slf4j
public class TransactionStateManagerTest extends KopProtocolHandlerTestBase {

    private static final Short producerEpoch = 0;
    private static final Integer transactionTimeoutMs = 1000;
    private static final int partitionId = 0;
    private static final int numPartitions = 2;
    private static final String transactionalId1 = "one";
    private static final String transactionalId2 = "two";
    private static final MockTime time = new MockTime();
    private static final TransactionConfig txnConfig = TransactionConfig
            .builder()
            .transactionLogNumPartitions(numPartitions)
            .build();
    private static final Map<String, Long> producerIds = new HashMap<String, Long>(){{
        put(transactionalId1, 1L);
        put(transactionalId2, 2L);
    }};
    private static final TransactionMetadata txnMetadata1 =
            transactionMetadata(transactionalId1, producerIds.get(transactionalId1), TransactionState.EMPTY,
                    transactionTimeoutMs);

    private static final TransactionMetadata txnMetadata2 =
            transactionMetadata(transactionalId2, producerIds.get(transactionalId2), TransactionState.EMPTY,
                    transactionTimeoutMs);

    private TransactionStateManager transactionManager;

    private OrderedScheduler scheduler;
    private SystemTopicClient systemTopicClient;

    private static TransactionMetadata transactionMetadata(String transactionalId,
                                                    Long producerId,
                                                    TransactionState state,
                                                    int txnTimeout) {
        return TransactionMetadata.builder()
                .transactionalId(transactionalId)
                .producerId(producerId)
                .producerEpoch((short) 0)
                .txnTimeoutMs(txnTimeout)
                .state(state)
                .build();
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        this.conf.setTxnLogTopicNumPartitions(numPartitions);
        internalSetup();
//
//        TenantInfoImpl tenantInfo =
//                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test"));
//        if (!admin.tenants().getTenants().contains("public")) {
//            admin.tenants().createTenant("public", tenantInfo);
//        } else {
//            admin.tenants().updateTenant("public", tenantInfo);
//        }
//        if (!admin.namespaces().getNamespaces("public").contains("public/default")) {
//            admin.namespaces().createNamespace("public/default");
//            admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
//            admin.namespaces().setRetention("public/default",
//                    new RetentionPolicies(60, 1000));
//        }
//        if (!admin.namespaces().getNamespaces("public").contains("public/__kafka")) {
//            admin.namespaces().createNamespace("public/__kafka");
//            admin.namespaces().setNamespaceReplicationClusters("public/__kafka", Sets.newHashSet("test"));
//            admin.namespaces().setRetention("public/__kafka",
//                    new RetentionPolicies(20, 100));
//        }
//        log.info("txn topic partition {}", admin.topics().getPartitionedTopicMetadata(
//                TransactionConfig.DefaultTransactionMetadataTopicName).partitions);
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @BeforeMethod
    protected void setUp() {
        systemTopicClient = new SystemTopicClient(pulsar, conf);
        scheduler = OrderedScheduler.newSchedulerBuilder()
                .name("test-txn-coordinator-scheduler")
                .numThreads(1)
                .build();
        transactionManager =
                spy(new TransactionStateManager(txnConfig, systemTopicClient, scheduler, time));
        transactionManager.startup(false);
        // make sure the transactional id hashes to the assigning partition id
        assertEquals(partitionId, transactionManager.partitionFor(transactionalId1));
        assertEquals(partitionId, transactionManager.partitionFor(transactionalId2));
    }

    @AfterMethod
    protected void tearDown() {
        transactionManager.shutdown();
        systemTopicClient.close();
        scheduler.shutdown();
    }


//    @Test(timeOut = 1000 * 10)
//    public void txnLogStoreAndTCImmigrationTest() throws Exception {
//        Map<String, Long> pidMappings = Maps.newHashMap();
//        pidMappings.put("zero", 0L);
//        pidMappings.put("one", 1L);
//        pidMappings.put("two", 2L);
//        pidMappings.put("three", 3L);
//        pidMappings.put("four", 4L);
//        pidMappings.put("five", 5L);
//
//        Map<Long, TransactionState> transactionStates = Maps.newHashMap();
//        transactionStates.put(0L, TransactionState.EMPTY);
//        transactionStates.put(1L, TransactionState.ONGOING);
//        transactionStates.put(2L, TransactionState.PREPARE_COMMIT);
//        transactionStates.put(3L, TransactionState.COMPLETE_COMMIT);
//        transactionStates.put(4L, TransactionState.PREPARE_ABORT);
//        transactionStates.put(5L, TransactionState.COMPLETE_ABORT);
//
//        // Make sure transaction state log already loaded.
//        this.loadTxnImmigration();
//        TransactionStateManager transactionStateManager = getTxnManager();
//
//        CountDownLatch countDownLatch = new CountDownLatch(pidMappings.size());
//        pidMappings.forEach((transactionalId, producerId) -> {
//            TransactionMetadata.TransactionMetadataBuilder txnMetadataBuilder = TransactionMetadata.builder()
//                    .transactionalId(transactionalId)
//                    .producerId(producerId)
//                    .lastProducerId(RecordBatch.NO_PRODUCER_ID)
//                    .producerEpoch(producerEpoch)
//                    .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
//                    .txnTimeoutMs(transactionTimeoutMs)
//                    .state(transactionStates.get(producerId))
//                    .pendingState(Optional.of(transactionStates.get(producerId)))
//                    .topicPartitions(Sets.newHashSet())
//                    .txnStartTimestamp(transactionStates.get(producerId) == TransactionState.EMPTY
//                            ? -1 : System.currentTimeMillis());
//
//            if (transactionStates.get(producerId).equals(TransactionState.COMPLETE_ABORT)
//                    || transactionStates.get(producerId).equals(TransactionState.COMPLETE_COMMIT)) {
//                txnMetadataBuilder.txnStartTimestamp(0);
//            }
//            TransactionMetadata txnMetadata = txnMetadataBuilder.build();
//
//            transactionStateManager.putTransactionStateIfNotExists(txnMetadata);
//            transactionStateManager.appendTransactionToLog(transactionalId, -1, txnMetadata.prepareNoTransit(),
//                    new TransactionStateManager.ResponseCallback() {
//                        @Override
//                        public void complete() {
//                            log.info("Success append transaction log.");
//                            countDownLatch.countDown();
//                        }
//
//                        @Override
//                        public void fail(Errors errors) {
//                            log.error("Failed append transaction log.", errors.exception());
//                            countDownLatch.countDown();
//                            Assert.fail("Failed append transaction log.");
//                        }
//                    }, errors -> false);
//        });
//        countDownLatch.await();
//
//        Map<Integer, Map<String, TransactionMetadata>> txnMetadataCache =
//                transactionStateManager.transactionMetadataCache;
//        // retain the transaction metadata cache
//        Map<Integer, Map<String, TransactionMetadata>> beforeTxnMetadataCache = new HashMap<>(txnMetadataCache);
//
//        BundlesData bundles = pulsar.getAdminClient().namespaces().getBundles(
//                conf.getKafkaTenant() + "/" + conf.getKafkaNamespace());
//        List<String> boundaries = bundles.getBoundaries();
//        for (int i = 0; i < boundaries.size() - 1; i++) {
//            String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
//            pulsar.getAdminClient().namespaces()
//                    .unloadNamespaceBundle(conf.getKafkaTenant() + "/" + conf.getKafkaNamespace(), bundle);
//        }
//
//        waitTCImmigrationComplete();
//
//        // verify the loaded transaction metadata
//        verifyImmigration(transactionStateManager, beforeTxnMetadataCache);
//    }

    @Test(timeOut = 1000 * 10)
    public void shouldRemoveCompleteCommitExpiredTransactionalIds() {
        setupAndRunTransactionalIdExpiration(Errors.NONE, TransactionState.COMPLETE_COMMIT);
        verifyMetadataDoesntExist(transactionalId1);
        verifyMetadataDoesExistAndIsUsable(transactionalId2);
    }

    private void verifyMetadataDoesntExist(String transactionalId) {
        ErrorsAndData<Optional<TransactionStateManager.CoordinatorEpochAndTxnMetadata>> transactionState =
                transactionManager.getTransactionState(transactionalId);
        if (transactionState.hasErrors()) {
            fail("shouldn't have been any errors");
            return;
        }
        if (transactionState.getData().isPresent()) {
            fail("metadata should have been removed");
        }
    }

    private void verifyMetadataDoesExistAndIsUsable(String transactionalId) {
        ErrorsAndData<Optional<TransactionStateManager.CoordinatorEpochAndTxnMetadata>> transactionState =
                transactionManager.getTransactionState(transactionalId);
        if (transactionState.hasErrors()) {
            fail("shouldn't have been any errors");
            return;
        }
        if (!transactionState.getData().isPresent()) {
            fail("metadata should have been removed");
            return;
        }
        TransactionStateManager.CoordinatorEpochAndTxnMetadata metadata = transactionState.getData().get();

        assertFalse("metadata shouldn't be in a pending state", metadata.getTransactionMetadata().getPendingState().isPresent());
    }

    private void setupAndRunTransactionalIdExpiration(Errors error, TransactionState txnState) {
        loadTransactionsForPartitions(0, numPartitions);
        txnMetadata1.setTxnLastUpdateTimestamp(time.milliseconds() - txnConfig.getTransactionalIdExpirationMs());
        txnMetadata1.setState(txnState);
        transactionManager.putTransactionStateIfNotExists(txnMetadata1);

        txnMetadata2.setTxnLastUpdateTimestamp(time.milliseconds());
        transactionManager.putTransactionStateIfNotExists(txnMetadata2);

        Map<Integer, List<MemoryRecords>> appendedRecords = Maps.newHashMap();
        expectTransactionalIdExpiration(error, appendedRecords);

        transactionManager.removeExpiredTransactionalIds();

        boolean stateAllowsExpiration = txnState.isExpirationAllowed();
        if (stateAllowsExpiration) {
            int partitionId = transactionManager.partitionFor(transactionalId1);
            SimpleRecord expectedTombstone =
                    new SimpleRecord(time.milliseconds(),
                            new TransactionLogKey(transactionalId1).toBytes(), null);
            MemoryRecords expectedRecords =
                    MemoryRecords.withRecords(txnConfig.getTransactionMetadataTopicCompressionType(),
                            expectedTombstone);
            assertTrue(appendedRecords.containsKey(partitionId));
            assertEquals(Lists.newArrayList(expectedRecords), appendedRecords.get(partitionId));

        } else {
            assertTrue(appendedRecords.isEmpty());
        }
    }

    private void expectTransactionalIdExpiration(Errors appendError,
                                                 Map<Integer, List<MemoryRecords>> capturedAppends){
        ArgumentCaptor<Integer> partitionCapture = ArgumentCaptor.forClass(Integer.class);

        ArgumentCaptor<MemoryRecords> recordsCapture = ArgumentCaptor.forClass(MemoryRecords.class);

        doAnswer(__ -> {
            Integer partition = partitionCapture.getValue();
            MemoryRecords memoryRecords = recordsCapture.getValue();
            List<MemoryRecords> batches = capturedAppends.getOrDefault(partition, Lists.newArrayList());
            batches.add(memoryRecords);
            capturedAppends.put(partition, batches);
            if (appendError == Errors.NONE) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<MessageId> completableFuture = new CompletableFuture<>();
            completableFuture.completeExceptionally(appendError.exception());
            return completableFuture;
        }).when(transactionManager)
                .appendTombstoneRecords(partitionCapture.capture(), recordsCapture.capture());
    }

    private void loadTransactionsForPartitions(int startPartitionNum, int endPartitionNum) {
        for (int partitionId = startPartitionNum; partitionId < endPartitionNum; partitionId++) {
            transactionManager.addLoadedTransactionsToCache(partitionId, Maps.newConcurrentMap());
        }
    }

    private void verifyImmigration(TransactionStateManager transactionStateManager,
                                   Map<Integer, Map<String, TransactionMetadata>> beforeTxnMetadataCache) {
        Map<Integer, Map<String, TransactionMetadata>> loadedTxnMetadataCache =
                transactionStateManager.transactionMetadataCache;

        for (int i = 0; i < conf.getTxnLogTopicNumPartitions(); i++) {
            Map<String, TransactionMetadata> txnMetadataMap = beforeTxnMetadataCache.get(i);
            Map<String, TransactionMetadata> loadedTxnMetadataMap = loadedTxnMetadataCache.get(i);
            if (txnMetadataMap == null) {
                assertNull(loadedTxnMetadataMap);
                continue;
            }
            assertEquals(txnMetadataMap.size(), loadedTxnMetadataMap.size());
            txnMetadataMap.forEach((txnId, txnMetadata) -> {
                TransactionMetadata loadedTxnMetadata = loadedTxnMetadataMap.get(txnId);
                assertEquals(txnMetadata.getTransactionalId(), loadedTxnMetadata.getTransactionalId());
                assertEquals(txnMetadata.getProducerId(), loadedTxnMetadata.getProducerId());
                assertEquals(txnMetadata.getLastProducerId(), loadedTxnMetadata.getLastProducerId());
                assertEquals(txnMetadata.getProducerEpoch(), loadedTxnMetadata.getProducerEpoch());
                assertEquals(txnMetadata.getLastProducerEpoch(), loadedTxnMetadata.getLastProducerEpoch());
                assertEquals(txnMetadata.getTxnTimeoutMs(), loadedTxnMetadata.getTxnTimeoutMs());
                assertEquals(txnMetadata.getTopicPartitions(), loadedTxnMetadata.getTopicPartitions());
                assertEquals(txnMetadata.getTxnStartTimestamp(), loadedTxnMetadata.getTxnStartTimestamp());
                if (txnMetadata.getState().equals(TransactionState.PREPARE_ABORT)) {
                    // to prepare state will complete
                    waitTxnComplete(loadedTxnMetadata, TransactionState.COMPLETE_ABORT);
                } else if (txnMetadata.getState().equals(TransactionState.PREPARE_COMMIT)) {
                    // to prepare state will complete
                    waitTxnComplete(loadedTxnMetadata, TransactionState.COMPLETE_COMMIT);
                } else {
                    assertEquals(txnMetadata.getState(), loadedTxnMetadata.getState());
                    assertEquals(txnMetadata.getTxnLastUpdateTimestamp(),
                            loadedTxnMetadata.getTxnLastUpdateTimestamp());
                }
            });
        }
    }

    private void waitTxnComplete(TransactionMetadata loadedTxnMetadata, TransactionState expectedState) {
        Awaitility.await()
                .pollDelay(Duration.ofMillis(500))
                .untilAsserted(() -> assertEquals(loadedTxnMetadata.getState(), expectedState));
        assertEquals(expectedState, loadedTxnMetadata.getState());
        assertTrue(loadedTxnMetadata.getTxnLastUpdateTimestamp() > 0);
    }

    private void waitTCImmigrationComplete() throws PulsarAdminException {
        admin.lookups().lookupTopic("public/default/__transaction_state-partition-0");
        TransactionStateManager txnStateManager = getTxnManager();
        // The lookup request will trigger topic on-load operation,
        // the TC partition log will recover when the namespace on-load, it's asynchronously,
        // so wait the TC partition log load complete.
        assertTrue(txnStateManager.isLoading());
        Awaitility.await()
                .pollDelay(Duration.ofMillis(500))
                .untilAsserted(() -> assertFalse(txnStateManager.isLoading()));
    }

    private TransactionCoordinator getTxnCoordinator() {
        return ((KafkaProtocolHandler) this.pulsar.getProtocolHandlers().protocol("kafka"))
                .getTransactionCoordinator("public");
    }

    private TransactionStateManager getTxnManager() {
        return getTxnCoordinator().getTxnManager();
    }

//    private void loadTxnImmigration() {
//        List<CompletableFuture<Void>> allFuture = Lists.newArrayList();
//        for (int i = 0; i < txnLogTopicNumPartitions; i++) {
//            allFuture.add(transactionCoordinator.handleTxnImmigration(i));
//        }
//        try {
//            FutureUtil.waitForAll(allFuture).get();
//        } catch (InterruptedException | ExecutionException e) {
//            log.error("Load txn immigration failed.", e);
//        }
//    }
}