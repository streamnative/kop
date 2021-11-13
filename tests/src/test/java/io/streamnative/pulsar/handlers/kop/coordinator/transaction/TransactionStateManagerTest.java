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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import io.streamnative.pulsar.handlers.kop.utils.timer.MockTime;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.kafka.common.protocol.Errors;
import org.apache.pulsar.client.api.MessageId;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

/**
 * Unit test {@link TransactionStateManager}.
 */
@Slf4j
public class TransactionStateManagerTest extends KopProtocolHandlerTestBase {

    protected final long defaultTestTimeout = 20000;
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
    private TransactionMetadata txnMetadata1 =
            transactionMetadata(transactionalId1, producerIds.get(transactionalId1), TransactionState.EMPTY,
                    transactionTimeoutMs);
    private TransactionMetadata txnMetadata2 =
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
                .producerEpoch(producerEpoch)
                .txnTimeoutMs(txnTimeout)
                .state(state)
                .build();
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        this.conf.setTxnLogTopicNumPartitions(numPartitions);
        internalSetup();
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
        txnMetadata1 = transactionMetadata(transactionalId1, producerIds.get(transactionalId1), TransactionState.EMPTY,
                transactionTimeoutMs);
        txnMetadata2 = transactionMetadata(transactionalId2, producerIds.get(transactionalId2), TransactionState.EMPTY,
                transactionTimeoutMs);
    }

    @AfterMethod
    protected void tearDown() {
        transactionManager.shutdown();
        systemTopicClient.close();
        scheduler.shutdown();
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRemoveCompleteCommitExpiredTransactionalIds() {
        setupAndRunTransactionalIdExpiration(Errors.NONE, TransactionState.COMPLETE_COMMIT);
        verifyMetadataDoesntExist(transactionalId1);
        verifyMetadataDoesExistAndIsUsable(transactionalId2);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRemoveCompleteAbortExpiredTransactionalIds() {
        setupAndRunTransactionalIdExpiration(Errors.NONE, TransactionState.COMPLETE_ABORT);
        verifyMetadataDoesntExist(transactionalId1);
        verifyMetadataDoesExistAndIsUsable(transactionalId2);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRemoveEmptyExpiredTransactionalIds() {
        setupAndRunTransactionalIdExpiration(Errors.NONE, TransactionState.EMPTY);
        verifyMetadataDoesntExist(transactionalId1);
        verifyMetadataDoesExistAndIsUsable(transactionalId2);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldNotRemoveExpiredTransactionalIdsIfLogAppendFails() {
        setupAndRunTransactionalIdExpiration(Errors.NOT_ENOUGH_REPLICAS, TransactionState.COMPLETE_ABORT);
        verifyMetadataDoesExistAndIsUsable(transactionalId1);
        verifyMetadataDoesExistAndIsUsable(transactionalId2);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldNotRemoveOngoingTransactionalIds() {
        setupAndRunTransactionalIdExpiration(Errors.NONE, TransactionState.ONGOING);
        verifyMetadataDoesExistAndIsUsable(transactionalId1);
        verifyMetadataDoesExistAndIsUsable(transactionalId2);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldNotRemovePrepareAbortTransactionalIds() {
        setupAndRunTransactionalIdExpiration(Errors.NONE, TransactionState.PREPARE_ABORT);
        verifyMetadataDoesExistAndIsUsable(transactionalId1);
        verifyMetadataDoesExistAndIsUsable(transactionalId2);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldNotRemovePrepareCommitTransactionalIds() {
        setupAndRunTransactionalIdExpiration(Errors.NONE, TransactionState.PREPARE_COMMIT);
        verifyMetadataDoesExistAndIsUsable(transactionalId1);
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

        assertFalse("metadata shouldn't be in a pending state",
                metadata.getTransactionMetadata().getPendingState().isPresent());
    }

    private void setupAndRunTransactionalIdExpiration(Errors error, TransactionState txnState) {
        loadTransactionsForPartitions(0, numPartitions);
        txnMetadata1.setTxnLastUpdateTimestamp(time.milliseconds() - txnConfig.getTransactionalIdExpirationMs());
        txnMetadata1.setState(txnState);
        transactionManager.putTransactionStateIfNotExists(txnMetadata1);

        txnMetadata2.setTxnLastUpdateTimestamp(time.milliseconds());
        transactionManager.putTransactionStateIfNotExists(txnMetadata2);

        Map<Integer, List<String>> appendedRecords = Maps.newHashMap();
        expectTransactionalIdExpiration(error, appendedRecords);

        transactionManager.removeExpiredTransactionalIds();

        boolean stateAllowsExpiration = txnState.isExpirationAllowed();
        if (stateAllowsExpiration) {
            int partitionId = transactionManager.partitionFor(transactionalId1);
            List<String> expectedTombstone = Lists.newArrayList(transactionalId1);
            assertTrue(appendedRecords.containsKey(partitionId));
            assertEquals(Lists.newArrayList(expectedTombstone), appendedRecords.get(partitionId));

        } else {
            assertTrue(appendedRecords.isEmpty());
        }
    }

    private void expectTransactionalIdExpiration(Errors appendError,
                                                 Map<Integer, List<String>> capturedAppends){
        ArgumentCaptor<Integer> partitionCapture = ArgumentCaptor.forClass(Integer.class);

        ArgumentCaptor<byte[]> recordsCapture = ArgumentCaptor.forClass(byte[].class);

        doAnswer(__ -> {
            Integer partition = partitionCapture.getValue();
            byte[] transactionIdBytes = recordsCapture.getValue();
            List<String> batches = capturedAppends.getOrDefault(partition, Lists.newArrayList());
            batches.add(TransactionLogKey
                    .decode(ByteBuffer.wrap(transactionIdBytes),
                            TransactionLogKey.HIGHEST_SUPPORTED_VERSION).getTransactionId());
            capturedAppends.put(partition, batches);
            if (appendError == Errors.NONE) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<MessageId> completableFuture = new CompletableFuture<>();
            completableFuture.completeExceptionally(appendError.exception());
            return completableFuture;
        }).when(transactionManager)
                .appendTombstone(partitionCapture.capture(), recordsCapture.capture());
    }

    private void loadTransactionsForPartitions(int startPartitionNum, int endPartitionNum) {
        for (int partitionId = startPartitionNum; partitionId < endPartitionNum; partitionId++) {
            transactionManager.addLoadedTransactionsToCache(partitionId, Maps.newConcurrentMap());
        }
    }
}