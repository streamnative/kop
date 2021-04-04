package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.collect.Sets;

@Slf4j
public class TransactionStateManagerTest extends KopProtocolHandlerTestBase {

    Short producerEpoch = 0;
    Integer transactionTimeoutMs = 1000;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        this.conf.setEnableTransactionCoordinator(true);
        internalSetup();
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
                    new RetentionPolicies(20, 100));
        }
        log.info("txn topic partition {}", admin.topics().getPartitionedTopicMetadata(
                TransactionConfig.DefaultTransactionMetadataTopicName).partitions);
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
    }

    @Test
    public void loadTest() throws Exception {
        Map<String, Long> pidMappings = Maps.newHashMap();
        pidMappings.put("zero", 0L);
        pidMappings.put("one", 1L);
        pidMappings.put("two", 2L);
        pidMappings.put("three", 3L);
        pidMappings.put("four", 4L);
        pidMappings.put("five", 5L);

        Map<Long, TransactionState> transactionStates = Maps.newHashMap();
        transactionStates.put(0L, TransactionState.EMPTY);
        transactionStates.put(1L, TransactionState.ONGOING);
        transactionStates.put(2L, TransactionState.PREPARE_COMMIT);
        transactionStates.put(3L, TransactionState.COMPLETE_COMMIT);
        transactionStates.put(4L, TransactionState.PREPARE_ABORT);
        transactionStates.put(5L, TransactionState.COMPLETE_ABORT);

        TransactionConfig transactionConfig = TransactionConfig.builder()
                .transactionLogNumPartitions(conf.getTxnLogTopicNumPartitions())
                .build();

        ScheduledExecutorService executor = OrderedScheduler.newSchedulerBuilder()
                .name("transaction-coordinator-executor")
                .build();

        TransactionStateManager transactionStateManager =
                new TransactionStateManager(transactionConfig, pulsarClient, executor);

        List<CompletableFuture<Void>> loadFutureList = new ArrayList<>();
        for (int i = 0; i < this.conf.getTxnLogTopicNumPartitions(); i++) {
            CompletableFuture<Void> loadFuture = transactionStateManager.loadTransactionsForTxnTopicPartition(i,
                    (transactionResult, transactionMetadata, txnTransitMetadata) -> {

                    });
            loadFutureList.add(loadFuture);
        }
        CountDownLatch loadLatch = new CountDownLatch(1);
        FutureUtil.waitForAll(loadFutureList).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                log.error("Failed to load transaction metadata first.", throwable);
                Assert.fail("Failed to load transaction metadata first.");
            }
            loadLatch.countDown();
        });
        loadLatch.await();

        CountDownLatch countDownLatch = new CountDownLatch(pidMappings.size());
        pidMappings.forEach((transactionalId, producerId) -> {
            TransactionMetadata.TransactionMetadataBuilder txnMetadataBuilder = TransactionMetadata.builder()
                    .transactionalId(transactionalId)
                    .producerId(producerId)
                    .lastProducerId(RecordBatch.NO_PRODUCER_ID)
                    .producerEpoch(producerEpoch)
                    .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                    .txnTimeoutMs(transactionTimeoutMs)
                    .state(transactionStates.get(producerId))
                    .pendingState(Optional.of(transactionStates.get(producerId)))
                    .topicPartitions(Sets.newHashSet())
                    .txnStartTimestamp(-1);

            if (transactionStates.get(producerId).equals(TransactionState.COMPLETE_ABORT)
                    || transactionStates.get(producerId).equals(TransactionState.COMPLETE_COMMIT)) {
                txnMetadataBuilder.txnStartTimestamp(0);
            }
            TransactionMetadata txnMetadata = txnMetadataBuilder.build();

            transactionStateManager.putTransactionStateIfNotExists(txnMetadata);
            transactionStateManager.appendTransactionToLog(transactionalId, -1, txnMetadata.prepareNoTransit(),
                    new TransactionStateManager.ResponseCallback() {
                        @Override
                        public void complete() {
                            log.info("Success append transaction log.");
                            countDownLatch.countDown();
                        }

                        @Override
                        public void fail(Errors errors) {
                            log.error("Failed append transaction log.", errors.exception());
                            countDownLatch.countDown();
                            Assert.fail("Failed append transaction log.");
                        }
                    }, errors -> false);
        });
        countDownLatch.await();

        ScheduledExecutorService loadedExecutor = OrderedScheduler.newSchedulerBuilder()
                .name("loaded-transaction-coordinator-executor")
                .build();

        TransactionStateManager loadedTxnStateManager =
                new TransactionStateManager(transactionConfig, pulsarClient, loadedExecutor);

        loadFutureList.clear();
        for (int i = 0; i < this.conf.getTxnLogTopicNumPartitions(); i++) {
            CompletableFuture<Void> loadFuture = loadedTxnStateManager.loadTransactionsForTxnTopicPartition(i,
                    (transactionResult, transactionMetadata, txnTransitMetadata) -> {

            });
            loadFutureList.add(loadFuture);
        }
        CountDownLatch latch = new CountDownLatch(1);
        FutureUtil.waitForAll(loadFutureList).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                log.error("Failed to load transaction metadata.", throwable);
            }
            latch.countDown();
        });
        latch.await();

        Class<TransactionStateManager> stateManagerClass = TransactionStateManager.class;
        Field txnMetadataCacheField = stateManagerClass.getDeclaredField("transactionMetadataCache");
        txnMetadataCacheField.setAccessible(true);
        Map<Integer, Map<String, TransactionMetadata>> txnMetadataCache =
                (Map<Integer, Map<String, TransactionMetadata>>) txnMetadataCacheField.get(loadedTxnStateManager);

        txnMetadataCache.size();
    }

}
