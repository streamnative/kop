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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.atLeastOnce;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import io.streamnative.pulsar.handlers.kop.utils.timer.MockTime;
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
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setEnableTransactionCoordinator(false);
        super.internalSetup();

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
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        transactionCoordinator.shutdown();
        super.internalCleanup();
    }

    private void initMockPidManager() {
        this.producerIdManager = mock(ProducerIdManager.class);
        doAnswer(__ -> getNextPid()).when(producerIdManager).generateProducerId();
        doReturn(CompletableFuture.completedFuture(null)).when(producerIdManager).initialize();
    }

    private void initTransactionManager() {
        this.transactionManager = mock(TransactionStateManager.class);
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
