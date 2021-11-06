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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.atLeastOnce;
import static org.testng.Assert.assertNotEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import io.streamnative.pulsar.handlers.kop.utils.ProducerIdAndEpoch;
import io.streamnative.pulsar.handlers.kop.utils.timer.MockTime;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link TransactionCoordinator}.
 */
@Slf4j
public class TransactionCoordinatorTest extends KopProtocolHandlerTestBase {

    protected final long defaultTestTimeout = 20000;
    public static final long DefaultAbortTimedOutTransactionsIntervalMs = TimeUnit.SECONDS.toMillis(1);

    private TransactionCoordinator transactionCoordinator;
    private ProducerIdManager producerIdManager;
    private TransactionStateManager transactionManager;
    private TransactionCoordinator.InitProducerIdResult result = null;
    private Errors error = Errors.NONE;
    ArgumentCaptor<TransactionMetadata> capturedTxn = ArgumentCaptor.forClass(TransactionMetadata.class);
    ArgumentCaptor<TransactionStateManager.ResponseCallback> capturedErrorsCallback =
            ArgumentCaptor.forClass(TransactionStateManager.ResponseCallback.class);

    private final AtomicLong nextPid = new AtomicLong(0L);
    private final MockTime time = new MockTime();
    private final Set<TopicPartition> partitions = Sets.newHashSet(new TopicPartition("topic1", 0));
    private final String transactionalId = "known";
    private final long producerId = 10L;
    private final short producerEpoch = 1;
    private final int txnTimeoutMs = 1;
    private final int coordinatorEpoch = 0;

    private final Consumer<TransactionCoordinator.InitProducerIdResult> initProducerIdMockCallback = (ret) -> {
        result = ret;
    };

    private final Consumer<Errors> errorsCallback = (ret) -> {
        error = ret;
    };

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setEnableTransactionCoordinator(false);
        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        transactionCoordinator.shutdown();
        super.internalCleanup();
    }

    @BeforeMethod
    protected void initializeState() {
        ProtocolHandler handler = pulsar.getProtocolHandlers().protocol("kafka");
        KafkaProtocolHandler kafkaProtocolHandler = (KafkaProtocolHandler) handler;

        OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder()
                .name("test-txn-coordinator-scheduler")
                .numThreads(1)
                .build();

        initMockPidManager();
        initTransactionManager();

        transactionCoordinator = new TransactionCoordinator(
                TransactionConfig.builder()
                        .abortTimedOutTransactionsIntervalMs(DefaultAbortTimedOutTransactionsIntervalMs)
                        .build(),
                kafkaProtocolHandler.getKopBrokerLookupManager(),
                scheduler,
                producerIdManager,
                transactionManager,
                time);
        result = null;
        error = Errors.NONE;
        capturedTxn = ArgumentCaptor.forClass(TransactionMetadata.class);
        capturedErrorsCallback = ArgumentCaptor.forClass(TransactionStateManager.ResponseCallback.class);
    }

    private void initMockPidManager() {
        this.producerIdManager = mock(ProducerIdManager.class);
        doAnswer(__ -> getNextPid()).when(producerIdManager).generateProducerId();
        doReturn(CompletableFuture.completedFuture(null)).when(producerIdManager).initialize();
    }

    private void initTransactionManager() {
        this.transactionManager = mock(TransactionStateManager.class);
    }

    private void initPidGenericMocks() {
        doReturn(true).when(transactionManager).validateTransactionTimeoutMs(anyInt());
    }

    private CompletableFuture<Long> getNextPid() {
        CompletableFuture<Long> future = new CompletableFuture<>();
        future.complete(nextPid.getAndIncrement());
        return future;
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldReturnInvalidRequestWhenTransactionalIdIsEmpty() {
        transactionCoordinator.handleInitProducerId(
                "",
                txnTimeoutMs,
                Optional.empty(),
                initProducerIdMockCallback);
        assertEquals(
                new TransactionCoordinator.InitProducerIdResult(-1L, (short) -1, Errors.INVALID_REQUEST),
                result);
        transactionCoordinator.handleInitProducerId(
                "",
                txnTimeoutMs,
                Optional.empty(),
                initProducerIdMockCallback);
        assertEquals(
                new TransactionCoordinator.InitProducerIdResult(-1L, (short) -1, Errors.INVALID_REQUEST),
                result);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldAcceptInitPidAndReturnNextPidWhenTransactionalIdIsNull() {
        transactionCoordinator.handleInitProducerId(
                null,
                txnTimeoutMs,
                Optional.empty(),
                initProducerIdMockCallback);
        assertEquals(
                new TransactionCoordinator.InitProducerIdResult(0L, (short) 0, Errors.NONE),
                result);
        transactionCoordinator.handleInitProducerId(
                null,
                txnTimeoutMs,
                Optional.empty(),
                initProducerIdMockCallback);
        assertEquals(
                new TransactionCoordinator.InitProducerIdResult(1L, (short) 0, Errors.NONE),
                result);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldInitPidWithEpochZeroForNewTransactionalId() {
        initPidGenericMocks();
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.empty()))
                .when(transactionManager).getTransactionState(eq(transactionalId));

        doAnswer(__ -> {
            assertNotNull(capturedTxn.getValue());
            return new ErrorsAndData<>(
                    Optional.of(
                            new TransactionStateManager
                                    .CoordinatorEpochAndTxnMetadata(coordinatorEpoch, capturedTxn.getValue())));
        }).when(transactionManager).putTransactionStateIfNotExists(capturedTxn.capture());

        doAnswer(__ -> {
            capturedErrorsCallback.getValue().complete();
            return null;
        }).when(transactionManager)
                .appendTransactionToLog(
                        eq(transactionalId),
                        eq(coordinatorEpoch),
                        any(TransactionMetadata.TxnTransitMetadata.class),
                        capturedErrorsCallback.capture(),
                        any(TransactionStateManager.RetryOnError.class)
                );

        transactionCoordinator.handleInitProducerId(transactionalId, txnTimeoutMs, Optional.empty(),
                initProducerIdMockCallback);

        assertEquals(new TransactionCoordinator
                .InitProducerIdResult(nextPid.get() - 1, (short) 0, Errors.NONE), result);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldGenerateNewProducerIdIfNoStateAndProducerIdAndEpochProvided() {
        initPidGenericMocks();
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.empty()))
                .when(transactionManager).getTransactionState(eq(transactionalId));
        doAnswer(__ -> {
            assertNotNull(capturedTxn.getValue());
            return new ErrorsAndData<>(
                    Optional.of(
                            new TransactionStateManager
                                    .CoordinatorEpochAndTxnMetadata(coordinatorEpoch, capturedTxn.getValue())));
        }).when(transactionManager).putTransactionStateIfNotExists(capturedTxn.capture());

        doAnswer(__ -> {
            capturedErrorsCallback.getValue().complete();
            return null;
        }).when(transactionManager)
                .appendTransactionToLog(
                        eq(transactionalId),
                        eq(coordinatorEpoch),
                        any(TransactionMetadata.TxnTransitMetadata.class),
                        capturedErrorsCallback.capture(),
                        any(TransactionStateManager.RetryOnError.class)
                );

        transactionCoordinator.handleInitProducerId(
                transactionalId,
                txnTimeoutMs,
                Optional.of(new ProducerIdAndEpoch(producerId, producerEpoch)),
                initProducerIdMockCallback
        );

        assertEquals(new TransactionCoordinator
                .InitProducerIdResult(nextPid.get() - 1, (short) 0, Errors.NONE), result);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldGenerateNewProducerIdIfEpochsExhausted() {
        long now = time.milliseconds();
        initPidGenericMocks();
        TransactionMetadata txnMetadata = new TransactionMetadata(
                transactionalId,
                producerId,
                producerId,
                (short) (Short.MAX_VALUE - 1),
                (short) (Short.MAX_VALUE - 2),
                txnTimeoutMs,
                TransactionState.EMPTY,
                Sets.newHashSet(),
                now,
                now,
                Optional.empty(),
                false);

        doReturn(new ErrorsAndData<>(Optional.of(new TransactionStateManager
                .CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
                .when(transactionManager).getTransactionState(eq(transactionalId));

        doAnswer(__ -> {
            capturedErrorsCallback.getValue().complete();
            return null;
        }).when(transactionManager)
                .appendTransactionToLog(
                        eq(transactionalId),
                        eq(coordinatorEpoch),
                        any(TransactionMetadata.TxnTransitMetadata.class),
                        capturedErrorsCallback.capture(),
                        any(TransactionStateManager.RetryOnError.class)
                );
        transactionCoordinator.handleInitProducerId(
                transactionalId,
                txnTimeoutMs,
                Optional.empty(),
                initProducerIdMockCallback
        );
        assertNotEquals(producerId, result.getProducerId());
        assertEquals(Short.valueOf((short) 0), result.getProducerEpoch());
        assertEquals(Errors.NONE, result.getError());
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithNotCoordinatorOnInitPidWhenNotCoordinator() {
        initPidGenericMocks();
        doReturn(new ErrorsAndData<>(Errors.NOT_COORDINATOR, Optional.empty()))
                .when(transactionManager).getTransactionState(eq(transactionalId));

        transactionCoordinator.handleInitProducerId(
                transactionalId,
                txnTimeoutMs,
                Optional.empty(),
                initProducerIdMockCallback
        );
        assertEquals(new TransactionCoordinator
                .InitProducerIdResult(-1L, (short) -1, Errors.NOT_COORDINATOR), result);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithCoordinatorLoadInProgressOnInitPidWhenCoordinatorLoading() {
        initPidGenericMocks();
        doReturn(new ErrorsAndData<>(Errors.COORDINATOR_LOAD_IN_PROGRESS, Optional.empty()))
                .when(transactionManager).getTransactionState(eq(transactionalId));

        transactionCoordinator.handleInitProducerId(
                transactionalId,
                txnTimeoutMs,
                Optional.empty(),
                initProducerIdMockCallback
        );
        assertEquals(new TransactionCoordinator
                .InitProducerIdResult(-1L, (short) -1, Errors.COORDINATOR_LOAD_IN_PROGRESS), result);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithInvalidPidMappingOnAddPartitionsToTransactionWhenTransactionalIdNotPresent() {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.empty()))
                .when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleAddPartitionsToTransaction(
                transactionalId,
                0L,
                (short) 1,
                partitions,
                errorsCallback
        );
        assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithInvalidRequestAddPartitionsToTransactionWhenTransactionalIdIsEmpty() {
        transactionCoordinator.handleAddPartitionsToTransaction(
                "",
                0L,
                (short) 1,
                partitions,
                errorsCallback);
        assertEquals(Errors.INVALID_REQUEST, error);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithInvalidRequestAddPartitionsToTransactionWhenTransactionalIdIsNull() {
        transactionCoordinator.handleAddPartitionsToTransaction(
                null,
                0L,
                (short) 1,
                partitions,
                errorsCallback);
        assertEquals(Errors.INVALID_REQUEST, error);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithNotCoordinatorOnAddPartitionsWhenNotCoordinator() {
        doReturn(new ErrorsAndData<>(Errors.NOT_COORDINATOR, Optional.empty()))
                .when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleAddPartitionsToTransaction(
                transactionalId,
                0L,
                (short) 1,
                partitions,
                errorsCallback
        );
        assertEquals(Errors.NOT_COORDINATOR, error);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithCoordinatorLoadInProgressOnAddPartitionsWhenCoordinatorLoading() {
        doReturn(new ErrorsAndData<>(Errors.COORDINATOR_LOAD_IN_PROGRESS, Optional.empty()))
                .when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleAddPartitionsToTransaction(
                transactionalId,
                0L,
                (short) 1,
                partitions,
                errorsCallback
        );
        assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, error);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithConcurrentTransactionsOnAddPartitionsWhenStateIsPrepareCommit() {
        validateConcurrentTransactions(TransactionState.PREPARE_COMMIT);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithConcurrentTransactionOnAddPartitionsWhenStateIsPrepareAbort() {
        validateConcurrentTransactions(TransactionState.PREPARE_ABORT);
    }

    private void validateConcurrentTransactions(TransactionState state) {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
                        TransactionMetadata.builder()
                                .transactionalId(transactionalId)
                                .producerId(0)
                                .lastProducerId(0)
                                .producerEpoch((short) 0)
                                .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                                .txnTimeoutMs(0)
                                .state(state)
                                .topicPartitions(Collections.emptySet())
                                .txnStartTimestamp(0)
                                .txnLastUpdateTimestamp(0)
                                .build()
                )))).when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleAddPartitionsToTransaction(
                transactionalId,
                0L,
                (short) 1,
                partitions,
                errorsCallback
        );
        // TODO: Should have PRODUCER_FENCED
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, error);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithProducerFencedOnAddPartitionsWhenEpochsAreDifferent() {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
                        TransactionMetadata.builder()
                                .transactionalId(transactionalId)
                                .producerId(0)
                                .lastProducerId(0)
                                .producerEpoch((short) 10)
                                .lastProducerEpoch((short) 9)
                                .txnTimeoutMs(0)
                                .state(TransactionState.PREPARE_COMMIT)
                                .topicPartitions(Collections.emptySet())
                                .txnStartTimestamp(0)
                                .txnLastUpdateTimestamp(0)
                                .build()
                )))).when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleAddPartitionsToTransaction(
                transactionalId,
                0L,
                (short) 1,
                partitions,
                errorsCallback
        );
        // TODO: Should have PRODUCER_FENCED
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, error);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldAppendNewMetadataToLogOnAddPartitionsWhenPartitionsAdded() {
        validateSuccessfulAddPartitions(TransactionState.EMPTY);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithSuccessOnAddPartitionsWhenStateIsOngoing() {
        validateSuccessfulAddPartitions(TransactionState.ONGOING);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithSuccessOnAddPartitionsWhenStateIsCompleteCommit() {
        validateSuccessfulAddPartitions(TransactionState.COMPLETE_COMMIT);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithSuccessOnAddPartitionsWhenStateIsCompleteAbort() {
        validateSuccessfulAddPartitions(TransactionState.COMPLETE_ABORT);
    }

    private void validateSuccessfulAddPartitions(TransactionState previousState) {
        long now = time.milliseconds();
        TransactionMetadata txnMetadata = new TransactionMetadata(
                transactionalId,
                producerId,
                producerId,
                producerEpoch,
                (short) (producerEpoch - 1),
                txnTimeoutMs,
                previousState,
                Collections.emptySet(),
                now,
                now,
                Optional.empty(),
                false);
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)))
        ).when(transactionManager).getTransactionState(eq(transactionalId));

        transactionCoordinator.handleAddPartitionsToTransaction(
                transactionalId,
                producerId,
                producerEpoch,
                partitions,
                errorsCallback);
        verify(transactionManager, atLeastOnce()).appendTransactionToLog(
                eq(transactionalId),
                eq(coordinatorEpoch),
                any(TransactionMetadata.TxnTransitMetadata.class),
                capturedErrorsCallback.capture(),
                any(TransactionStateManager.RetryOnError.class)
        );
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithErrorsNoneOnAddPartitionWhenNoErrorsAndPartitionsTheSame() {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
                        TransactionMetadata.builder()
                                .transactionalId(transactionalId)
                                .producerId(0)
                                .lastProducerId(0)
                                .producerEpoch((short) 0)
                                .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                                .txnTimeoutMs(0)
                                .state(TransactionState.EMPTY)
                                .topicPartitions(partitions)
                                .txnStartTimestamp(0)
                                .txnLastUpdateTimestamp(0)
                                .build()
                )))).when(transactionManager).getTransactionState(eq(transactionalId));

        transactionCoordinator.handleAddPartitionsToTransaction(
                transactionalId, 0L, (short) 0, partitions, errorsCallback);
        assertEquals(Errors.NONE, error);
        verify(transactionManager, atLeastOnce())
                .getTransactionState(eq(transactionalId));
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldReplyWithInvalidPidMappingOnEndTxnWhenTxnIdDoesntExist() {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.empty()))
                .when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleEndTransaction(
                transactionalId,
                0,
                (short) 0,
                TransactionResult.COMMIT,
                errorsCallback);
        assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error);
        verify(transactionManager, atLeastOnce())
                .getTransactionState(eq(transactionalId));
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldReplyWithInvalidPidMappingOnEndTxnWhenPidDosentMatchMapped() {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
                        TransactionMetadata.builder()
                                .transactionalId(transactionalId)
                                .producerId(10)
                                .lastProducerId(10)
                                .producerEpoch((short) 0)
                                .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                                .txnTimeoutMs(0)
                                .state(TransactionState.EMPTY)
                                .topicPartitions(Collections.emptySet())
                                .txnStartTimestamp(0)
                                .txnLastUpdateTimestamp(time.milliseconds())
                                .build()
                )))).when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleEndTransaction(
                transactionalId,
                0,
                (short) 0,
                TransactionResult.COMMIT,
                errorsCallback);
        assertEquals(Errors.INVALID_PRODUCER_ID_MAPPING, error);
        verify(transactionManager, atLeastOnce())
                .getTransactionState(eq(transactionalId));
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldReplyWithProducerFencedOnEndTxnWhenEpochIsNotSameAsTransaction() {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
                        TransactionMetadata.builder()
                                .transactionalId(transactionalId)
                                .producerId(producerId)
                                .lastProducerId(producerId)
                                .producerEpoch(producerEpoch)
                                .lastProducerEpoch((short) (producerEpoch - 1))
                                .txnTimeoutMs(1)
                                .state(TransactionState.ONGOING)
                                .topicPartitions(Collections.emptySet())
                                .txnStartTimestamp(0)
                                .txnLastUpdateTimestamp(time.milliseconds())
                                .build()
                )))).when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleEndTransaction(
                transactionalId,
                producerId,
                (short) 0,
                TransactionResult.COMMIT,
                errorsCallback);
        // TODO: Should have PRODUCER_FENCED
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, error);
        verify(transactionManager, atLeastOnce())
                .getTransactionState(eq(transactionalId));
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldReturnOkOnEndTxnWhenStatusIsCompleteCommitAndResultIsCommit() {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
                        TransactionMetadata.builder()
                                .transactionalId(transactionalId)
                                .producerId(producerId)
                                .lastProducerId(producerId)
                                .producerEpoch(producerEpoch)
                                .lastProducerEpoch((short) (producerEpoch - 1))
                                .txnTimeoutMs(1)
                                .state(TransactionState.COMPLETE_COMMIT)
                                .topicPartitions(Collections.emptySet())
                                .txnStartTimestamp(0)
                                .txnLastUpdateTimestamp(time.milliseconds())
                                .build()
                )))).when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleEndTransaction(
                transactionalId,
                producerId,
                (short) 1,
                TransactionResult.COMMIT,
                errorsCallback);
        assertEquals(Errors.NONE, error);
        verify(transactionManager, atLeastOnce())
                .getTransactionState(eq(transactionalId));
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldReturnOkOnEndTxnWhenStatusIsCompleteAbortAndResultIsAbort() {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
                        TransactionMetadata.builder()
                                .transactionalId(transactionalId)
                                .producerId(producerId)
                                .lastProducerId(producerId)
                                .producerEpoch(producerEpoch)
                                .lastProducerEpoch((short) (producerEpoch - 1))
                                .txnTimeoutMs(1)
                                .state(TransactionState.COMPLETE_ABORT)
                                .topicPartitions(Collections.emptySet())
                                .txnStartTimestamp(0)
                                .txnLastUpdateTimestamp(time.milliseconds())
                                .build()
                )))).when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleEndTransaction(
                transactionalId,
                producerId,
                (short) 1,
                TransactionResult.ABORT,
                errorsCallback);
        assertEquals(Errors.NONE, error);
        verify(transactionManager, atLeastOnce())
                .getTransactionState(eq(transactionalId));
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteAbortAndResultIsNotAbort() {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
                        TransactionMetadata.builder()
                                .transactionalId(transactionalId)
                                .producerId(producerId)
                                .lastProducerId(producerId)
                                .producerEpoch(producerEpoch)
                                .lastProducerEpoch((short) (producerEpoch - 1))
                                .txnTimeoutMs(1)
                                .state(TransactionState.COMPLETE_ABORT)
                                .topicPartitions(Collections.emptySet())
                                .txnStartTimestamp(0)
                                .txnLastUpdateTimestamp(time.milliseconds())
                                .build()
                )))).when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleEndTransaction(
                transactionalId,
                producerId,
                (short) 1,
                TransactionResult.COMMIT,
                errorsCallback);
        assertEquals(Errors.INVALID_TXN_STATE, error);
        verify(transactionManager, atLeastOnce())
                .getTransactionState(eq(transactionalId));
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsCompleteCommitAndResultIsNotCommit() {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
                        TransactionMetadata.builder()
                                .transactionalId(transactionalId)
                                .producerId(producerId)
                                .lastProducerId(producerId)
                                .producerEpoch(producerEpoch)
                                .lastProducerEpoch((short) (producerEpoch - 1))
                                .txnTimeoutMs(1)
                                .state(TransactionState.COMPLETE_COMMIT)
                                .topicPartitions(Collections.emptySet())
                                .txnStartTimestamp(0)
                                .txnLastUpdateTimestamp(time.milliseconds())
                                .build()
                )))).when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleEndTransaction(
                transactionalId,
                producerId,
                (short) 1,
                TransactionResult.ABORT,
                errorsCallback);
        assertEquals(Errors.INVALID_TXN_STATE, error);
        verify(transactionManager, atLeastOnce())
                .getTransactionState(eq(transactionalId));
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldReturnConcurrentTxnRequestOnEndTxnRequestWhenStatusIsPrepareCommit() {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
                        TransactionMetadata.builder()
                                .transactionalId(transactionalId)
                                .producerId(producerId)
                                .lastProducerId(producerId)
                                .producerEpoch(producerEpoch)
                                .lastProducerEpoch((short) (producerEpoch - 1))
                                .txnTimeoutMs(1)
                                .state(TransactionState.PREPARE_COMMIT)
                                .topicPartitions(Collections.emptySet())
                                .txnStartTimestamp(0)
                                .txnLastUpdateTimestamp(time.milliseconds())
                                .build()
                )))).when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleEndTransaction(
                transactionalId,
                producerId,
                (short) 1,
                TransactionResult.COMMIT,
                errorsCallback);
        assertEquals(Errors.CONCURRENT_TRANSACTIONS, error);
        verify(transactionManager, atLeastOnce())
                .getTransactionState(eq(transactionalId));
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldReturnInvalidTxnRequestOnEndTxnRequestWhenStatusIsPrepareAbort() {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
                        TransactionMetadata.builder()
                                .transactionalId(transactionalId)
                                .producerId(producerId)
                                .lastProducerId(producerId)
                                .producerEpoch((short) 1)
                                .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                                .txnTimeoutMs(1)
                                .state(TransactionState.PREPARE_ABORT)
                                .topicPartitions(Collections.emptySet())
                                .txnStartTimestamp(0)
                                .txnLastUpdateTimestamp(time.milliseconds())
                                .build()
                )))).when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleEndTransaction(
                transactionalId,
                producerId,
                (short) 1,
                TransactionResult.COMMIT,
                errorsCallback);
        assertEquals(Errors.INVALID_TXN_STATE, error);
        verify(transactionManager, atLeastOnce())
                .getTransactionState(eq(transactionalId));
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldAppendPrepareCommitToLogOnEndTxnWhenStatusIsOngoingAndResultIsCommit() {
        mockPrepare(TransactionState.PREPARE_COMMIT);
        transactionCoordinator.handleEndTransaction(
                transactionalId,
                producerId,
                producerEpoch,
                TransactionResult.COMMIT,
                errorsCallback);
        verify(transactionManager, atLeastOnce())
                .getTransactionState(eq(transactionalId));
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldAppendPrepareAbortToLogOnEndTxnWhenStatusIsOngoingAndResultIsAbort() {
        mockPrepare(TransactionState.PREPARE_ABORT);
        transactionCoordinator.handleEndTransaction(
                transactionalId,
                producerId,
                producerEpoch,
                TransactionResult.ABORT,
                errorsCallback);
        verify(transactionManager, atLeastOnce())
                .getTransactionState(eq(transactionalId));
    }

    private void mockPrepare(TransactionState transactionState) {
        long now = time.milliseconds();
        TransactionMetadata originalMetadata = TransactionMetadata.builder()
                .transactionalId(transactionalId)
                .producerId(producerId)
                .lastProducerId(producerId)
                .producerEpoch(producerEpoch)
                .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                .txnTimeoutMs(txnTimeoutMs)
                .state(TransactionState.ONGOING)
                .topicPartitions(partitions)
                .txnStartTimestamp(now)
                .txnLastUpdateTimestamp(now)
                .build();

        TransactionMetadata.TxnTransitMetadata transition = TransactionMetadata.TxnTransitMetadata.builder()
                .producerId(producerId)
                .lastProducerId(producerId)
                .producerEpoch(producerEpoch)
                .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                .txnTimeoutMs(txnTimeoutMs)
                .txnState(transactionState)
                .topicPartitions(partitions)
                .txnStartTimestamp(now)
                .txnLastUpdateTimestamp(now)
                .build();
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch, originalMetadata))))
                .when(transactionManager).getTransactionState(eq(transactionalId));
        doAnswer(__ -> null).when(transactionManager)
                .appendTransactionToLog(
                        eq(transactionalId),
                        eq(coordinatorEpoch),
                        eq(transition),
                        capturedErrorsCallback.capture(),
                        any(TransactionStateManager.RetryOnError.class)
                );
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithInvalidRequestOnEndTxnWhenTransactionalIdIsNull() {
        transactionCoordinator.handleEndTransaction(
                null,
                0,
                (short) 0,
                TransactionResult.COMMIT,
                errorsCallback);
        assertEquals(Errors.INVALID_REQUEST, error);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithInvalidRequestOnEndTxnWhenTransactionalIdIsEmpty() {
        doReturn(new ErrorsAndData<>(Errors.NOT_COORDINATOR, Optional.empty()))
                .when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleEndTransaction(
                "",
                0,
                (short) 0,
                TransactionResult.COMMIT,
                errorsCallback);
        assertEquals(Errors.INVALID_REQUEST, error);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithNotCoordinatorOnEndTxnWhenIsNotCoordinatorForId() {
        doReturn(new ErrorsAndData<>(Errors.NOT_COORDINATOR, Optional.empty()))
                .when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleEndTransaction(
                transactionalId,
                0,
                (short) 0,
                TransactionResult.COMMIT,
                errorsCallback);
        assertEquals(Errors.NOT_COORDINATOR, error);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldRespondWithCoordinatorLoadInProgressOnEndTxnWhenCoordinatorIsLoading() {
        doReturn(new ErrorsAndData<>(Errors.COORDINATOR_LOAD_IN_PROGRESS, Optional.empty()))
                .when(transactionManager).getTransactionState(eq(transactionalId));
        transactionCoordinator.handleEndTransaction(
                transactionalId,
                0,
                (short) 0,
                TransactionResult.COMMIT,
                errorsCallback);
        assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, error);
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldReturnInvalidEpochOnEndTxnWhenEpochIsLarger() {
        short serverProducerEpoch = 1;
        verifyEndTxnEpoch(serverProducerEpoch, (short) (serverProducerEpoch + 1));
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldReturnInvalidEpochOnEndTxnWhenEpochIsSmaller() {
        short serverProducerEpoch = 1;
        verifyEndTxnEpoch(serverProducerEpoch, (short) (serverProducerEpoch - 1));
    }


    private void verifyEndTxnEpoch(short metadataEpoch, short requestEpoch) {
        doReturn(new ErrorsAndData<>(Errors.NONE, Optional.of(
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(coordinatorEpoch,
                        TransactionMetadata.builder()
                                .transactionalId(transactionalId)
                                .producerId(producerId)
                                .lastProducerId(producerId)
                                .producerEpoch(metadataEpoch)
                                .lastProducerEpoch((short) 0)
                                .txnTimeoutMs(1)
                                .state(TransactionState.COMPLETE_COMMIT)
                                .topicPartitions(Collections.emptySet())
                                .txnStartTimestamp(0)
                                .txnLastUpdateTimestamp(time.milliseconds())
                                .build()
                )))).when(transactionManager).getTransactionState(eq(transactionalId));

        transactionCoordinator.handleEndTransaction(
                transactionalId,
                producerId,
                requestEpoch,
                TransactionResult.COMMIT,
                errorsCallback);
        // TODO: Should have PRODUCER_FENCED
        assertEquals(Errors.UNKNOWN_SERVER_ERROR, error);
        verify(transactionManager, atLeastOnce())
                .getTransactionState(eq(transactionalId));
    }


    @Test(timeOut = defaultTestTimeout)
    public void shouldAbortExpiredTransactionsInOngoingStateAndBumpEpoch() {
        long now = time.milliseconds();
        TransactionMetadata txnMetadata = new TransactionMetadata(
                transactionalId,
                producerId,
                producerId,
                producerEpoch,
                RecordBatch.NO_PRODUCER_EPOCH,
                txnTimeoutMs,
                TransactionState.ONGOING,
                partitions,
                now,
                now,
                Optional.empty(),
                false);

        doReturn(Lists.newArrayList(
                new TransactionStateManager
                        .TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
                .when(transactionManager).timedOutTransactions();

        doReturn(new ErrorsAndData<>(Optional.of(new TransactionStateManager
                .CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata))))
                .when(transactionManager).getTransactionState(eq(transactionalId));

        short bumpedEpoch = producerEpoch + 1;
        TransactionMetadata.TxnTransitMetadata expectedTransition =
                new TransactionMetadata.TxnTransitMetadata(
                        producerId,
                        producerId,
                        bumpedEpoch,
                        (short) -1,
                        txnTimeoutMs,
                        TransactionState.PREPARE_ABORT,
                        partitions,
                        now,
                        now + DefaultAbortTimedOutTransactionsIntervalMs);
        time.sleep(DefaultAbortTimedOutTransactionsIntervalMs);
        transactionCoordinator.abortTimedOutTransactions();
        verify(transactionManager, atLeastOnce()).timedOutTransactions();
        verify(transactionManager, times(2)).getTransactionState(eq(transactionalId));
        verify(transactionManager, times(1)).appendTransactionToLog(
                eq(transactionalId),
                eq(coordinatorEpoch),
                eq(expectedTransition),
                any(),
                any()
        );
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldNotAcceptSmallerEpochDuringTransactionExpiration() {
        long now = time.milliseconds();
        TransactionMetadata txnMetadata = new TransactionMetadata(
                transactionalId,
                producerId,
                producerId,
                producerEpoch,
                RecordBatch.NO_PRODUCER_EPOCH,
                txnTimeoutMs,
                TransactionState.ONGOING,
                partitions,
                now,
                now,
                Optional.empty(),
                false);

        doReturn(Lists.newArrayList(
                new TransactionStateManager
                        .TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
                .when(transactionManager).timedOutTransactions();

        TransactionMetadata bumpedTxnMetadata = new TransactionMetadata(
                transactionalId,
                producerId,
                producerId,
                (short) (producerEpoch + 2),
                RecordBatch.NO_PRODUCER_EPOCH,
                txnTimeoutMs,
                TransactionState.ONGOING,
                partitions,
                now,
                now,
                Optional.empty(),
                false);

        AtomicInteger times = new AtomicInteger(0);
        doAnswer(__ -> {
            if (times.getAndIncrement() == 0) {
                return new ErrorsAndData<>(Optional.of(new TransactionStateManager
                        .CoordinatorEpochAndTxnMetadata(coordinatorEpoch, txnMetadata)));
            }
            return new ErrorsAndData<>(Optional.of(new TransactionStateManager
                    .CoordinatorEpochAndTxnMetadata(coordinatorEpoch, bumpedTxnMetadata)));
        }).when(transactionManager).getTransactionState(eq(transactionalId));

        AtomicBoolean isCalledOnComplete = new AtomicBoolean(false);
        transactionCoordinator.abortTimedOutTransactions((__, error) -> {
                isCalledOnComplete.set(true);
                assertEquals(Errors.UNKNOWN_SERVER_ERROR, error);
                // We can't test in here, because current API don't support PRODUCER_FENCED,
                // we need upgrade kafka dependency.

                // assertEquals(error.message(), "There is a newer producer with the same transactionalId "
                //        + "which fences the current one.");
        });
        assertTrue(isCalledOnComplete.get());

        verify(transactionManager, atLeastOnce()).timedOutTransactions();
        verify(transactionManager, atLeastOnce()).getTransactionState(eq(transactionalId));
    }

    @Test(timeOut = defaultTestTimeout)
    public void shouldNotAbortExpiredTransactionsThatHaveAPendingStateTransition() {
        long now = time.milliseconds();
        TransactionMetadata metadata = new TransactionMetadata(
                transactionalId,
                producerId,
                producerId,
                producerEpoch,
                RecordBatch.NO_PRODUCER_EPOCH,
                txnTimeoutMs,
                TransactionState.ONGOING,
                partitions,
                now,
                now,
                Optional.empty(),
                false);
        metadata.prepareAbortOrCommit(TransactionState.PREPARE_ABORT, now);
        doReturn(Lists.newArrayList(
                new TransactionStateManager
                        .TransactionalIdAndProducerIdEpoch(transactionalId, producerId, producerEpoch)))
                .when(transactionManager).timedOutTransactions();

        doReturn(new ErrorsAndData<>(Optional.of(new TransactionStateManager
                .CoordinatorEpochAndTxnMetadata(coordinatorEpoch, metadata))))
                .when(transactionManager).getTransactionState(eq(transactionalId));

        time.sleep(DefaultAbortTimedOutTransactionsIntervalMs);
        transactionCoordinator.abortTimedOutTransactions();
        verify(transactionManager, atLeastOnce()).timedOutTransactions();
        verify(transactionManager, atLeastOnce()).getTransactionState(eq(transactionalId));
    }
}
