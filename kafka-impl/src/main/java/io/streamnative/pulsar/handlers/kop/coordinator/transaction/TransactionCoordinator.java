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

import static io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionState.DEAD;
import static io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionState.ONGOING;
import static io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionState.PREPARE_ABORT;
import static io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionState.PREPARE_COMMIT;
import static io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionState.PREPARE_EPOCH_FENCE;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.KopBrokerLookupManager;
import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionMetadata.TxnTransitMetadata;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionStateManager.CoordinatorEpochAndTxnMetadata;
import io.streamnative.pulsar.handlers.kop.scala.Either;
import io.streamnative.pulsar.handlers.kop.storage.ProducerStateManagerSnapshotBuffer;
import io.streamnative.pulsar.handlers.kop.storage.PulsarPartitionedTopicProducerStateManagerSnapshotBuffer;
import io.streamnative.pulsar.handlers.kop.utils.MetadataUtils;
import io.streamnative.pulsar.handlers.kop.utils.ProducerIdAndEpoch;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;


/**
 * Transaction coordinator.
 */
@Slf4j
public class TransactionCoordinator {

    private final String namespacePrefixForMetadata;
    private final String namespacePrefixForUserTopics;
    private final TransactionConfig transactionConfig;
    private final ProducerIdManager producerIdManager;
    @Getter
    private final TransactionStateManager txnManager;
    private final TransactionMarkerChannelManager transactionMarkerChannelManager;

    @Getter
    private ProducerStateManagerSnapshotBuffer producerStateManagerSnapshotBuffer;

    private final ScheduledExecutorService scheduler;

    private final Time time;

    private final AtomicBoolean isActive = new AtomicBoolean(false);

    private static final BiConsumer<TransactionStateManager.TransactionalIdAndProducerIdEpoch, Errors>
            onEndTransactionComplete =
            (txnIdAndPidEpoch, errors) -> {
                switch (errors) {
                    case NONE:
                        log.info("Completed rollback of ongoing transaction for"
                                        + " transactionalId {} due to timeout",
                                txnIdAndPidEpoch.getTransactionalId());
                        break;
                    case INVALID_PRODUCER_ID_MAPPING:
                    case PRODUCER_FENCED:
                    case CONCURRENT_TRANSACTIONS:
                        if (log.isDebugEnabled()) {
                            log.debug("Rollback of ongoing transaction for transactionalId {} "
                                            + "has been cancelled due to error {}",
                                    txnIdAndPidEpoch.getTransactionalId(), errors);
                        }
                        break;
                    default:
                        log.warn("Rollback of ongoing transaction for transactionalId {} "
                                        + "failed due to error {}",
                                txnIdAndPidEpoch.getTransactionalId(), errors);
                        break;
                }
            };

    protected TransactionCoordinator(TransactionConfig transactionConfig,
                                     TransactionMarkerChannelManager transactionMarkerChannelManager,
                                     ScheduledExecutorService scheduler,
                                     ProducerIdManager producerIdManager,
                                     TransactionStateManager txnManager,
                                     Time time,
                                     String namespacePrefixForMetadata,
                                     String namespacePrefixForUserTopics,
                                     Function<TransactionConfig, ProducerStateManagerSnapshotBuffer>
                                             producerStateManagerSnapshotBufferFactory) {
        this.namespacePrefixForMetadata = namespacePrefixForMetadata;
        this.namespacePrefixForUserTopics = namespacePrefixForUserTopics;
        this.transactionConfig = transactionConfig;
        this.txnManager = txnManager;
        this.producerIdManager = producerIdManager;
        this.transactionMarkerChannelManager = transactionMarkerChannelManager;
        this.scheduler = scheduler;
        this.time = time;
        this.producerStateManagerSnapshotBuffer = producerStateManagerSnapshotBufferFactory.apply(transactionConfig);
    }

    public static TransactionCoordinator of(String tenant,
                                            KafkaServiceConfiguration kafkaConfig,
                                            TransactionConfig transactionConfig,
                                            SystemTopicClient txnTopicClient,
                                            MetadataStoreExtended metadataStore,
                                            KopBrokerLookupManager kopBrokerLookupManager,
                                            ScheduledExecutorService scheduler,
                                            Time time,
                                            Executor recoveryExecutor) throws Exception {
        String namespacePrefixForMetadata = MetadataUtils.constructMetadataNamespace(tenant, kafkaConfig);
        String namespacePrefixForUserTopics = MetadataUtils.constructUserTopicsNamespace(tenant, kafkaConfig);
        TransactionStateManager transactionStateManager =
                new TransactionStateManager(transactionConfig, txnTopicClient, scheduler, time);
        ProducerIdManager producerIdManager;
        if (kafkaConfig.isKafkaTransactionProducerIdsStoredOnPulsar()) {
            producerIdManager = new PulsarStorageProducerIdManagerImpl(
                    transactionConfig.getTransactionProducerIdTopicName(), txnTopicClient.getPulsarClient());
        } else {
            producerIdManager = new ProducerIdManagerImpl(transactionConfig.getBrokerId(), metadataStore);
        }
        return new TransactionCoordinator(
                transactionConfig,
                new TransactionMarkerChannelManager(tenant, kafkaConfig, transactionStateManager,
                        kopBrokerLookupManager, kafkaConfig.isKopTlsEnabledWithBroker(), namespacePrefixForUserTopics,
                        scheduler),
                scheduler,
                producerIdManager,
                transactionStateManager,
                time,
                namespacePrefixForMetadata,
                namespacePrefixForUserTopics,
                (config) -> new PulsarPartitionedTopicProducerStateManagerSnapshotBuffer(
                        config.getTransactionProducerStateSnapshotTopicName(), txnTopicClient, recoveryExecutor,
                        config.getProducerStateTopicNumPartitions())
                );
    }

    /**
     * Load state from the given partition and begin handling requests for groups which map to this partition.
     *
     * @param partition The partition that we are now leading
     */
    public CompletableFuture<Void> handleTxnImmigration(int partition) {
        log.info("Elected as the txn coordinator for partition {} for {}.", partition, namespacePrefixForMetadata);
        // The operations performed during immigration must be resilient to any previous errors we saw or partial state
        // we left off during the unloading phase. Ensure we remove all associated state for this partition before we
        // continue loading it.
        transactionMarkerChannelManager.removeMarkersForTxnTopicPartition(partition);

        return txnManager.loadTransactionsForTxnTopicPartition(partition,
                (transactionResult, transactionMetadata, txnTransitMetadata) -> {
                    transactionMarkerChannelManager.addTxnMarkersToSend(
                            -1, transactionResult, transactionMetadata, txnTransitMetadata,
                            namespacePrefixForUserTopics);
                });
    }

    /**
     * Clear coordinator caches for the given partition after giving up leadership.
     *
     * @param partition The partition that we are no longer leading
     */
    public void handleTxnEmigration(int partition) {
        log.info("Resigned as the txn coordinator for partition {} for {}.", partition, namespacePrefixForMetadata);
        txnManager.removeTransactionsForTxnTopicPartition(partition);
        transactionMarkerChannelManager.removeMarkersForTxnTopicPartition(partition);
    }

    public int partitionFor(String transactionalId) {
        return partitionFor(transactionalId, transactionConfig.getTransactionLogNumPartitions());
    }

    public static int partitionFor(String transactionalId, int transactionLogNumPartitions) {
        return MathUtils.signSafeMod(
                transactionalId.hashCode(),
                transactionLogNumPartitions
        );
    }

    public String getTopicPartitionName() {
        return transactionConfig.getTransactionMetadataTopicName();
    }

    public String getTopicPartitionName(int partitionId) {
        return getTopicPartitionName(getTopicPartitionName(), partitionId);
    }

    public static String getTopicPartitionName(String topicPartitionName, int partitionId) {
        return topicPartitionName + PARTITIONED_TOPIC_SUFFIX + partitionId;
    }

    public ListTransactionsResponseData handleListTransactions(List<String> filteredStates,
                                                               List<Long> filteredProducerIds) {
        // https://github.com/apache/kafka/blob/915991445fde106d02e61a70425ae2601c813db0/core/
        // src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala#L259
        if (!isActive.get()) {
            log.warn("The transaction coordinator is not active, so it will reject list transaction request");
            return new ListTransactionsResponseData().setErrorCode(Errors.NOT_COORDINATOR.code());
        }
        return this.txnManager.listTransactionStates(filteredProducerIds, filteredStates);
    }

    public DescribeTransactionsResponseData handleDescribeTransactions(List<String> transactionalIds) {
        DescribeTransactionsResponseData response = new DescribeTransactionsResponseData();
        if (transactionalIds != null) {
            transactionalIds.forEach(transactionalId -> {
                DescribeTransactionsResponseData.TransactionState transactionState =
                        handleDescribeTransactions(transactionalId);
                response.transactionStates().add(transactionState);
            });
        }
        return response;
    }

    private DescribeTransactionsResponseData.TransactionState handleDescribeTransactions(String transactionalId) {
        // https://github.com/apache/kafka/blob/915991445fde106d02e61a70425ae2601c813db0/core/
        // src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala#L270
        if (transactionalId == null) {
            throw new IllegalArgumentException("Invalid null transactionalId");
        }

        DescribeTransactionsResponseData.TransactionState transactionState =
                new DescribeTransactionsResponseData.TransactionState()
                .setTransactionalId(transactionalId);

        if (!isActive.get()) {
            transactionState.setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());
        } else if (transactionalId.isEmpty()) {
            transactionState.setErrorCode(Errors.INVALID_REQUEST.code());
        } else {
            Either<Errors, Optional<CoordinatorEpochAndTxnMetadata>> tState =
                    txnManager.getTransactionState(transactionalId);
            if (tState.isLeft()) {
                transactionState.setErrorCode(tState.getLeft().code());
            } else {
                Optional<CoordinatorEpochAndTxnMetadata> right = tState.getRight();
                if (!right.isPresent()) {
                    transactionState.setErrorCode(Errors.TRANSACTIONAL_ID_NOT_FOUND.code());
                } else {
                    CoordinatorEpochAndTxnMetadata coordinatorEpochAndMetadata = right.get();
                    TransactionMetadata txnMetadata = coordinatorEpochAndMetadata.getTransactionMetadata();
                    txnMetadata.inLock(() -> {
                        if (txnMetadata.getState() == DEAD) {
                            // The transaction state is being expired, so ignore it
                            transactionState.setErrorCode(Errors.TRANSACTIONAL_ID_NOT_FOUND.code());
                        } else {
                            txnMetadata.getTopicPartitions().forEach(topicPartition -> {
                                var topicData = transactionState.topics().find(topicPartition.topic());
                                if (topicData == null) {
                                    topicData = new DescribeTransactionsResponseData.TopicData()
                                            .setTopic(topicPartition.topic());
                                    transactionState.topics().add(topicData);
                                }
                                topicData.partitions().add(topicPartition.partition());
                            });

                            transactionState
                                    .setErrorCode(Errors.NONE.code())
                                    .setProducerId(txnMetadata.getProducerId())
                                    .setProducerEpoch(txnMetadata.getProducerEpoch())
                                    .setTransactionState(txnMetadata.getState().toAdminState().toString())
                                    .setTransactionTimeoutMs(txnMetadata.getTxnTimeoutMs())
                                    .setTransactionStartTimeMs(txnMetadata.getTxnStartTimestamp());
                        }
                        return null;
                    });
                }
            }
        }
        return transactionState;
    }

    @Data
    @EqualsAndHashCode
    @AllArgsConstructor
    public static class InitProducerIdResult {
        private Long producerId;
        private Short producerEpoch;
        private Errors error;
    }

    public void handleInitProducerId(String transactionalId,
                                     int transactionTimeoutMs,
                                     Optional<ProducerIdAndEpoch> expectedProducerIdAndEpoch,
                                     Consumer<InitProducerIdResult> responseCallback) {
        if (transactionalId == null) {
            // if the transactional id is null, then always blindly accept the request
            // and return a new producerId from the producerId manager
            producerIdManager.generateProducerId().whenComplete((pid, throwable) -> {
                short producerEpoch = 0;
                if (throwable != null) {
                    log.error("Failed to generate producer id for idempotent producer", throwable);
                    responseCallback.accept(new InitProducerIdResult(pid, producerEpoch, Errors.UNKNOWN_SERVER_ERROR));
                    return;
                }
                if (log.isDebugEnabled()) {
                    log.debug("Generate producer id {} for idempotent producer", pid);
                }
                responseCallback.accept(new InitProducerIdResult(pid, producerEpoch, Errors.NONE));
            });
        } else if (StringUtils.isEmpty(transactionalId)) {
            // if transactional id is empty then return error as invalid request. This is
            // to make TransactionCoordinator's behavior consistent with producer client
            responseCallback.accept(initTransactionError(Errors.INVALID_REQUEST));
        } else if (!txnManager.validateTransactionTimeoutMs(transactionTimeoutMs)){
            // check transactionTimeoutMs is not larger than the broker configured maximum allowed value
            responseCallback.accept(initTransactionError(Errors.INVALID_TRANSACTION_TIMEOUT));
        } else {
            final CompletableFuture<Either<Errors, CoordinatorEpochAndTxnMetadata>>
                    epochAndTxnMetaFuture = new CompletableFuture<>();
            txnManager.getTransactionState(transactionalId).match(errors -> {
                epochAndTxnMetaFuture.complete(Either.left(errors));
            }, optEpochAndTxnMetadata -> {
                if (optEpochAndTxnMetadata.isPresent()) {
                    epochAndTxnMetaFuture.complete(Either.right(optEpochAndTxnMetadata.get()));
                } else {
                    producerIdManager.generateProducerId().whenComplete((pid, throwable) -> {
                        if (throwable != null) {
                            log.error("Failed to generate producer id for {}", transactionalId, throwable);
                            epochAndTxnMetaFuture.complete(Either.left(Errors.UNKNOWN_SERVER_ERROR));
                            return;
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("Generate producer id {} for {}", pid, transactionalId);
                        }
                        TransactionMetadata newMetadata = TransactionMetadata.builder()
                                .transactionalId(transactionalId)
                                .producerId(pid)
                                .lastProducerId(RecordBatch.NO_PRODUCER_ID)
                                .producerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                                .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                                .state(TransactionState.EMPTY)
                                .topicPartitions(Sets.newHashSet())
                                .txnLastUpdateTimestamp(time.milliseconds())
                                .build();
                        epochAndTxnMetaFuture.complete(txnManager.putTransactionStateIfNotExists(newMetadata));
                    });
                }
            });

            epochAndTxnMetaFuture.thenAccept(either -> {
                either.match(errors -> {
                    responseCallback.accept(initTransactionError(errors));
                }, epochAndTxnMetadata -> {
                    int coordinatorEpoch = epochAndTxnMetadata.getCoordinatorEpoch();
                    TransactionMetadata txnMetadata = epochAndTxnMetadata.getTransactionMetadata();

                    txnMetadata.inLock(() -> {
                        prepareInitProducerIdTransit(transactionalId,
                                transactionTimeoutMs,
                                coordinatorEpoch,
                                txnMetadata,
                                expectedProducerIdAndEpoch
                        ).whenComplete((errorsOrEpochAndTxnTransitMetadata, prepareThrowable) -> {
                            completeInitProducer(
                                    transactionalId,
                                    coordinatorEpoch,
                                    errorsOrEpochAndTxnTransitMetadata,
                                    prepareThrowable,
                                    responseCallback);
                        });
                        return null;
                    });
                });
            });
        }
    }

    private void completeInitProducer(String transactionalId,
                                      int coordinatorEpoch,
                                      Either<Errors, EpochAndTxnTransitMetadata> errorsOrEpochAndTransitMetadata,
                                      Throwable prepareInitPidThrowable,
                                      Consumer<InitProducerIdResult> responseCallback) {
        if (prepareInitPidThrowable != null) {
            log.error("Failed to init producerId.", prepareInitPidThrowable);
            responseCallback.accept(initTransactionError(Errors.forException(prepareInitPidThrowable)));
            return;
        }
        if (errorsOrEpochAndTransitMetadata.isLeft()) {
            log.error("Failed to init producerId {} : {}", transactionalId, errorsOrEpochAndTransitMetadata.getLeft());
            responseCallback.accept(initTransactionError(errorsOrEpochAndTransitMetadata.getLeft()));
            return;
        }
        TxnTransitMetadata newMetadata = errorsOrEpochAndTransitMetadata.getRight().getTxnTransitMetadata();
        if (newMetadata.getTxnState() == PREPARE_EPOCH_FENCE) {
            endTransaction(transactionalId,
                    newMetadata.getProducerId(),
                    newMetadata.getProducerEpoch(),
                    TransactionResult.ABORT,
                    false,
                    errors -> {
                        if (errors != Errors.NONE) {
                            log.error("Cannot initProducer {} due to {} error", transactionalId, errors);
                            responseCallback.accept(initTransactionError(errors));
                        } else {
                            // reply to client and let it backoff and retry
                            responseCallback.accept(initTransactionError(Errors.CONCURRENT_TRANSACTIONS));
                        }
                    });
        } else {
            txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata,
                    new TransactionStateManager.ResponseCallback() {
                        @Override
                        public void complete() {
                            log.info("{} Initialized transactionalId {} with producerId {} and producer "
                                            + "epoch {} on partition {}-{}",
                                    namespacePrefixForMetadata,
                                    transactionalId,
                                    newMetadata.getProducerId(), newMetadata.getProducerEpoch(),
                                    Topic.TRANSACTION_STATE_TOPIC_NAME,
                                    txnManager.partitionFor(transactionalId));
                            responseCallback.accept(new InitProducerIdResult(
                                    newMetadata.getProducerId(),
                                    newMetadata.getProducerEpoch(),
                                    Errors.NONE));
                        }

                        @Override
                        public void fail(Errors errors) {
                            log.info("{} Returning {} error code to client for {}'s InitProducerId "
                                    + "request", namespacePrefixForMetadata, errors, transactionalId);
                            responseCallback.accept(initTransactionError(errors));
                        }
                    }, errors -> true);
        }
    }

    private InitProducerIdResult initTransactionError(Errors error) {
        return new InitProducerIdResult(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, error);
    }

    @Data
    @AllArgsConstructor
    private static class EpochAndTxnTransitMetadata {
        private final int coordinatorEpoch;
        private final TxnTransitMetadata txnTransitMetadata;
    }

    private boolean isValidProducerId(TransactionMetadata txnMetadata, ProducerIdAndEpoch producerIdAndEpoch) {
        // If a producer ID and epoch are provided by the request, fence the producer unless one of the following is
        // true:
        //   1. The producer epoch is equal to -1, which implies that the metadata was just created. This is the case
        //      of a producer recovering from an UNKNOWN_PRODUCER_ID error, and it is safe to return the newly-generated
        //      producer ID.
        //   2. The expected producer ID matches the ID in current metadata (the epoch will be checked when we try to
        //      increment it)
        //   3. The expected producer ID matches the previous one and the expected epoch is exhausted, in which case
        //      this could be a retry after a valid epoch bump that the producer never received the response for
        return txnMetadata.getProducerEpoch() == RecordBatch.NO_PRODUCER_EPOCH
                || producerIdAndEpoch.producerId == txnMetadata.getProducerId()
                || (producerIdAndEpoch.producerId == txnMetadata.getLastProducerId()
                && txnMetadata.isEpochExhausted(producerIdAndEpoch.epoch));
    }

    private CompletableFuture<Either<Errors, EpochAndTxnTransitMetadata>> prepareInitProducerIdTransit(
            String transactionalId,
            Integer transactionTimeoutMs,
            Integer coordinatorEpoch,
            TransactionMetadata txnMetadata,
            Optional<ProducerIdAndEpoch> expectedProducerIdAndEpoch) {
        CompletableFuture<Either<Errors, EpochAndTxnTransitMetadata>> resultFuture = new CompletableFuture<>();
        if (txnMetadata.pendingTransitionInProgress()) {
            // return a retryable exception to let the client backoff and retry
            // it is okay to log this here, this is not on the write path
            // the client calls initProducer only at bootstrap
            log.info("{} Failed initProducer for {}, pending transition to {}. {}",
                    namespacePrefixForMetadata,
                    transactionalId, txnMetadata.getPendingState(),
                    txnMetadata);
            resultFuture.complete(Either.left(Errors.CONCURRENT_TRANSACTIONS));
            return resultFuture;
        }

        if (expectedProducerIdAndEpoch.isPresent()
                && !isValidProducerId(txnMetadata, expectedProducerIdAndEpoch.get())) {
            resultFuture.complete(Either.left(producerEpochFenceErrors()));
            return resultFuture;
        } else {
            // caller should have synchronized on txnMetadata already
            switch (txnMetadata.getState()) {
                case PREPARE_ABORT:
                case PREPARE_COMMIT:
                    // reply to client and let it backoff and retry
                    resultFuture.complete(Either.left(Errors.CONCURRENT_TRANSACTIONS));
                    break;
                case COMPLETE_ABORT:
                case COMPLETE_COMMIT:
                case EMPTY:
                    final CompletableFuture<Either<Errors, TxnTransitMetadata>> transitMetadata =
                            new CompletableFuture<>();
                    // If the epoch is exhausted and the expected epoch (if provided) matches it, generate a new
                    // producer ID
                    if (txnMetadata.isProducerEpochExhausted()) {
                        CompletableFuture<Long> newProducerId = producerIdManager.generateProducerId();
                        newProducerId.thenAccept(newPid -> {
                            transitMetadata.complete(Either.right(txnMetadata.prepareProducerIdRotation(
                                    newPid,
                                    transactionTimeoutMs,
                                    time.milliseconds(),
                                    expectedProducerIdAndEpoch.isPresent())));
                        });
                    } else {
                        transitMetadata.complete(
                                txnMetadata.prepareIncrementProducerEpoch(
                                        transactionTimeoutMs,
                                        expectedProducerIdAndEpoch.map(ProducerIdAndEpoch::getEpoch),
                                        time.milliseconds()));
                    }
                    transitMetadata.thenAccept(txnTransitMetadata -> resultFuture.complete(txnTransitMetadata.map(__ ->
                            new EpochAndTxnTransitMetadata(coordinatorEpoch, __))));
                    break;
                case ONGOING:
                    // indicate to abort the current ongoing txn first. Note that this epoch is never returned to the
                    // user. We will abort the ongoing transaction and return CONCURRENT_TRANSACTIONS to the client.
                    // This forces the client to retry, which will ensure that the epoch is bumped a second time. In
                    // particular, if fencing the current producer exhausts the available epochs for the current
                    // producerId, then when the client retries, we will generate a new producerId.
                    resultFuture.complete(Either.right(
                            new EpochAndTxnTransitMetadata(coordinatorEpoch, txnMetadata.prepareFenceProducerEpoch())));
                    break;
                case DEAD:
                case PREPARE_EPOCH_FENCE:
                default:
                    String errorMsg = String.format("Found transactionalId %s with state %s. "
                                    + "This is illegal as we should never have transitioned to this state.",
                            transactionalId, txnMetadata.getState());
                    resultFuture.completeExceptionally(new IllegalStateException(errorMsg));
                    break;
            }
        }
        return resultFuture;
    }

    public void handleAddPartitionsToTransaction(String transactionalId,
                                                 long producerId,
                                                 short producerEpoch,
                                                 Set<TopicPartition> partitionList,
                                                 Consumer<Errors> responseCallback) {
        if (transactionalId == null || transactionalId.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Returning {} error code to client for {}'s AddPartitions request",
                        Errors.INVALID_REQUEST, transactionalId);
            }
            responseCallback.accept(Errors.INVALID_REQUEST);
            return;
        }

        // try to update the transaction metadata and append the updated metadata to txn log;
        // if there is no such metadata treat it as invalid producerId mapping error.
        Either<Errors, Optional<CoordinatorEpochAndTxnMetadata>> errorsOrMetadata =
                txnManager.getTransactionState(transactionalId);
        if (errorsOrMetadata.isLeft()) {
            responseCallback.accept(errorsOrMetadata.getLeft());
            return;
        }
        if (!errorsOrMetadata.getRight().isPresent()) {
            responseCallback.accept(Errors.INVALID_PRODUCER_ID_MAPPING);
            return;
        }

        CoordinatorEpochAndTxnMetadata epochAndTxnMetadata = errorsOrMetadata.getRight().get();
        int coordinatorEpoch = epochAndTxnMetadata.getCoordinatorEpoch();
        TransactionMetadata txnMetadata = epochAndTxnMetadata.getTransactionMetadata();

        Either<Errors, EpochAndTxnTransitMetadata> errorsOrTransitMetadata = txnMetadata.inLock(() -> {
            if (txnMetadata.getProducerId() != producerId) {
                return Either.left(Errors.INVALID_PRODUCER_ID_MAPPING);
            } else if (txnMetadata.getProducerEpoch() != producerEpoch) {
                return Either.left(producerEpochFenceErrors());
            } else if (txnMetadata.getPendingState().isPresent()) {
                // return a retriable exception to let the client backoff and retry
                if (log.isDebugEnabled()) {
                    log.debug("Producer {} is in pending state {}, responding CONCURRENT_TRANSACTIONS",
                            transactionalId, txnMetadata.getPendingState());
                }
                return Either.left(Errors.CONCURRENT_TRANSACTIONS);
            } else if (txnMetadata.getState() == PREPARE_COMMIT || txnMetadata.getState() == PREPARE_ABORT) {
                if (log.isDebugEnabled()) {
                    log.debug("Producer {} is in state {}, responding CONCURRENT_TRANSACTIONS",
                            transactionalId, txnMetadata.getState()
                    );
                }
                return Either.left(Errors.CONCURRENT_TRANSACTIONS);
            } else if (txnMetadata.getState() == ONGOING
                    && txnMetadata.getTopicPartitions().containsAll(partitionList)) {
                // this is an optimization: if the partitions are already in the metadata reply OK immediately
                return Either.left(Errors.NONE);
            } else {
                return Either.right(new EpochAndTxnTransitMetadata(
                        coordinatorEpoch, txnMetadata.prepareAddPartitions(
                        ImmutableSet.copyOf(partitionList), time.milliseconds())));
            }
        });

        if (errorsOrTransitMetadata.getLeft() != null) {
            responseCallback.accept(errorsOrTransitMetadata.getLeft());
            return;
        }
        EpochAndTxnTransitMetadata transitMetadata = errorsOrTransitMetadata.getRight();
        txnManager.appendTransactionToLog(
                transactionalId, transitMetadata.getCoordinatorEpoch(), transitMetadata.getTxnTransitMetadata(),
                new TransactionStateManager.ResponseCallback() {
                    @Override
                    public void complete() {
                        responseCallback.accept(Errors.NONE);
                    }

                    @Override
                    public void fail(Errors e) {
                        log.error("Error writing to TX log for {}, answer {}", transactionalId, e);
                        responseCallback.accept(e);
                    }
                }, errors -> true);
    }

    private Errors producerEpochFenceErrors() {
        if (log.isDebugEnabled()) {
            log.debug("There is a newer producer with the same transactionalId which fences the current one.");
        }
        return Errors.PRODUCER_FENCED;
    }

    public void handleEndTransaction(String transactionalId,
                                     long producerId,
                                     short producerEpoch,
                                     TransactionResult transactionResult,
                                     Consumer<Errors> responseCallback) {
        endTransaction(transactionalId, producerId, producerEpoch, transactionResult, true,
                responseCallback);
    }

    @AllArgsConstructor
    @Data
    private static class PreSendResult {
        private TransactionMetadata transactionMetadata;
        private TxnTransitMetadata txnTransitMetadata;
    }

    private void endTransaction(String transactionalId,
                                Long producerId,
                                Short producerEpoch,
                                TransactionResult txnMarkerResult,
                                boolean isFromClient,
                                Consumer<Errors> callback) {
        AtomicBoolean isEpochFence = new AtomicBoolean(false);
        if (transactionalId == null || transactionalId.isEmpty()) {
            callback.accept(Errors.INVALID_REQUEST);
            return;
        }

        Either<Errors, Optional<CoordinatorEpochAndTxnMetadata>> transactionState =
                txnManager.getTransactionState(transactionalId);
        if (transactionState.isLeft()) {
            callback.accept(transactionState.getLeft());
            return;
        }

        Optional<CoordinatorEpochAndTxnMetadata> epochAndMetadata = transactionState.getRight();
        if (!epochAndMetadata.isPresent()) {
            callback.accept(Errors.INVALID_PRODUCER_ID_MAPPING);
            return;
        }

        if (!isFromClient) {
            log.info("{} endTransaction - before endTxnPreAppend {} metadata {}",
                    namespacePrefixForMetadata, transactionalId, epochAndMetadata.get().getTransactionMetadata());
        }

        Either<Errors, TxnTransitMetadata> preAppendResult = endTxnPreAppend(
                epochAndMetadata.get(), transactionalId, producerId, isFromClient, producerEpoch,
                txnMarkerResult, isEpochFence);

        if (preAppendResult.isLeft()) {
            log.error("Aborting append of {} to transaction log with coordinator and returning {} error to client "
                    + "for {}'s EndTransaction request", txnMarkerResult, preAppendResult.getLeft(), transactionalId);
            callback.accept(preAppendResult.getLeft());
            return;
        }

        int coordinatorEpoch = epochAndMetadata.get().getCoordinatorEpoch();
        txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, preAppendResult.getRight(),
                new TransactionStateManager.ResponseCallback() {
                    @Override
                    public void complete() {
                        completeEndTxn(transactionalId, coordinatorEpoch, producerId, producerEpoch,
                                txnMarkerResult, callback);
                    }

                    @Override
                    public void fail(Errors errors) {

                        if (!isFromClient) {
                            log.info("{} endTransaction - AFTER failed appendTransactionToLog {} metadata {}"
                                            +  "isEpochFence {}",
                                    namespacePrefixForMetadata,
                                    transactionalId, epochAndMetadata.get().getTransactionMetadata(), isEpochFence);
                        }

                        log.info("{} Aborting sending of transaction markers and returning {} error to client for {}'s "
                                + "EndTransaction request of {}, since appending {} to transaction log with "
                                + "coordinator epoch {} failed",
                                namespacePrefixForMetadata, errors, transactionalId, txnMarkerResult,
                                preAppendResult.getRight(), coordinatorEpoch);

                        if (isEpochFence.get()) {
                            Either<Errors, Optional<CoordinatorEpochAndTxnMetadata>>
                                    errorsAndData = txnManager.getTransactionState(transactionalId);
                            if (errorsAndData.isLeft()) {
                                log.error("Cannot get transaction metadata for {}, status {}", transactionalId,
                                        errorsAndData.getLeft());
                            } else if (errorsAndData.isLeft() || !errorsAndData.getRight().isPresent()) {
                                log.error("The coordinator still owns the transaction partition for {}, but there "
                                        + "is no metadata in the cache; this is not expected", transactionalId);
                            } else {
                                CoordinatorEpochAndTxnMetadata epochAndMetadata = errorsAndData.getRight().get();
                                if (epochAndMetadata.getCoordinatorEpoch() == coordinatorEpoch) {
                                    // This was attempted epoch fence that failed, so mark this state on the metadata
                                    epochAndMetadata.getTransactionMetadata().setHasFailedEpochFence(true);

                                    // this line is not present in Kafka code base ?
                                    epochAndMetadata.getTransactionMetadata().setPendingState(Optional.empty());

                                    log.warn("The coordinator failed to write an epoch fence transition for producer "
                                                    + "{} to the transaction log with error {}. "
                                                    + "The epoch was increased to {} "
                                                    + "but not returned to the client", transactionalId, errors,
                                            preAppendResult.getRight().getProducerEpoch());
                                }
                            }
                        } else {
                            Either<Errors, Optional<CoordinatorEpochAndTxnMetadata>>
                                    errorsAndData = txnManager.getTransactionState(transactionalId);
                            if (errorsAndData.isLeft()) {
                                log.error("Cannot get transaction metadata for {}, status {}", transactionalId,
                                        errorsAndData.getLeft());
                            } else if (errorsAndData.getRight().isPresent()) {
                                log.error("Resetting transactionalId {} pendingState to EMPTY, status {}",
                                        transactionalId,
                                        errorsAndData.getLeft());
                                CoordinatorEpochAndTxnMetadata epochAndMetadata = errorsAndData.getRight().get();
                                epochAndMetadata.getTransactionMetadata().setPendingState(Optional.empty());
                            }
                        }

                        callback.accept(errors);
                    }
                }, retryErrors -> true);
    }

    private Either<Errors, TxnTransitMetadata> endTxnPreAppend(
                                                        CoordinatorEpochAndTxnMetadata epochAndMetadata,
                                                        String transactionalId,
                                                        long producerId,
                                                        boolean isFromClient,
                                                        short producerEpoch,
                                                        TransactionResult txnMarkerResult,
                                                        AtomicBoolean isEpochFence) {
        TransactionMetadata txnMetadata = epochAndMetadata.getTransactionMetadata();

        return txnMetadata.inLock(() -> {
            if (txnMetadata.getProducerId() != producerId) {
                return Either.left(Errors.INVALID_PRODUCER_ID_MAPPING);
            }
            if ((isFromClient && producerEpoch != txnMetadata.getProducerEpoch())
                    || producerEpoch < txnMetadata.getProducerEpoch()) {
                return Either.left(producerEpochFenceErrors());
            }
            if (txnMetadata.getPendingState().isPresent()
                    && txnMetadata.getPendingState().get() != PREPARE_EPOCH_FENCE) {
                return Either.left(Errors.CONCURRENT_TRANSACTIONS);
            }

            return endTxnByStatus(transactionalId, txnMarkerResult, txnMetadata, isEpochFence, producerEpoch);
        });
    }

    private Either<Errors, TxnTransitMetadata> endTxnByStatus(String transactionalId,
                                                              TransactionResult txnMarkerResult,
                                                              TransactionMetadata txnMetadata,
                                                              AtomicBoolean isEpochFence,
                                                              short producerEpoch) {
        switch(txnMetadata.getState()) {
            case ONGOING:
                return Either.right(endTxnOnGoingResult(txnMarkerResult, txnMetadata, isEpochFence, producerEpoch));
            case COMPLETE_COMMIT:
                return Either.left(getPreEndTxnErrors(txnMarkerResult, TransactionResult.COMMIT, Errors.NONE,
                        transactionalId, txnMetadata));
            case COMPLETE_ABORT:
                return Either.left(getPreEndTxnErrors(txnMarkerResult, TransactionResult.ABORT, Errors.NONE,
                        transactionalId, txnMetadata));
            case PREPARE_COMMIT:
                return Either.left(getPreEndTxnErrors(txnMarkerResult, TransactionResult.COMMIT,
                        Errors.CONCURRENT_TRANSACTIONS, transactionalId, txnMetadata));
            case PREPARE_ABORT:
                return Either.left(getPreEndTxnErrors(txnMarkerResult, TransactionResult.ABORT,
                        Errors.CONCURRENT_TRANSACTIONS, transactionalId, txnMetadata));
            case EMPTY:
                return Either.left(logInvalidStateTransitionAndReturnError(
                        transactionalId, txnMetadata.getState(), txnMarkerResult));
            case DEAD:
            case PREPARE_EPOCH_FENCE:
            default:
                String errorMsg = String.format("Found transactionalId %s with state %s. "
                                + "This is illegal as we should never have transitioned to this state.",
                        transactionalId, txnMetadata.getState());
                log.error(errorMsg);
                throw new IllegalStateException(errorMsg);
        }
    }

    private TxnTransitMetadata endTxnOnGoingResult(TransactionResult txnMarkerResult,
                                                   TransactionMetadata txnMetadata,
                                                   AtomicBoolean isEpochFence,
                                                   short producerEpoch) {
        TransactionState nextState;
        if (txnMarkerResult == TransactionResult.COMMIT) {
            nextState = PREPARE_COMMIT;
        } else {
            nextState = PREPARE_ABORT;
        }

        if (nextState == PREPARE_ABORT && txnMetadata.getPendingState().isPresent()
                && txnMetadata.getPendingState().get().equals(PREPARE_EPOCH_FENCE)) {
            // We should clear the pending state to make way for the transition to PrepareAbort and also
            // bump the epoch in the transaction metadata we are about to append.
            isEpochFence.set(true);
            txnMetadata.setPendingState(Optional.empty());
            txnMetadata.setProducerEpoch(producerEpoch);
            txnMetadata.setLastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH);
        }

        return txnMetadata.prepareAbortOrCommit(nextState, time.milliseconds());
    }

    private Errors getPreEndTxnErrors(TransactionResult txnMarkerResult, TransactionResult compareResult,
                                      Errors errors, String transactionalId, TransactionMetadata txnMetadata) {
        if (txnMarkerResult.equals(compareResult)) {
            return errors;
        } else {
            return logInvalidStateTransitionAndReturnError(
                    transactionalId, txnMetadata.getState(), txnMarkerResult);
        }
    }

    private void completeEndTxn(String transactionalId,
                                int coordinatorEpoch,
                                long producerId,
                                int producerEpoch,
                                TransactionResult txnMarkerResult,
                                Consumer<Errors> callback) {

        Either<Errors, Optional<CoordinatorEpochAndTxnMetadata>> errorsOrOptEpochAndTxnMetadata =
                txnManager.getTransactionState(transactionalId);

        if (!errorsOrOptEpochAndTxnMetadata.getRight().isPresent()) {
            String errorMsg = String.format("The coordinator still owns the transaction partition for "
                            + "%s, but there is no metadata in the cache; this is not expected",
                    transactionalId);
            log.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }

        CoordinatorEpochAndTxnMetadata epochAndTxnMetadata = errorsOrOptEpochAndTxnMetadata.getRight().get();
        final Either<Errors, PreSendResult> errorsOrPreSendResult;
        if (epochAndTxnMetadata.getCoordinatorEpoch() == coordinatorEpoch) {
            TransactionMetadata txnMetadata = epochAndTxnMetadata.getTransactionMetadata();
            errorsOrPreSendResult = txnMetadata.inLock(() -> {
                if (txnMetadata.getProducerId() != producerId) {
                    return Either.left(Errors.INVALID_PRODUCER_ID_MAPPING);
                } else if (txnMetadata.getProducerEpoch() != producerEpoch) {
                    return Either.left(producerEpochFenceErrors());
                } else if (txnMetadata.getPendingState().isPresent()) {
                    return Either.left(Errors.CONCURRENT_TRANSACTIONS);
                } else {
                    switch (txnMetadata.getState()) {
                        case EMPTY:
                        case ONGOING:
                        case COMPLETE_ABORT:
                        case COMPLETE_COMMIT:
                            return Either.left(logInvalidStateTransitionAndReturnError(
                                    transactionalId, txnMetadata.getState(), txnMarkerResult));
                        case PREPARE_COMMIT:
                            if (txnMarkerResult != TransactionResult.COMMIT) {
                                return Either.left(logInvalidStateTransitionAndReturnError(
                                        transactionalId, txnMetadata.getState(), txnMarkerResult));
                            } else {
                                TxnTransitMetadata txnTransitMetadata =
                                        txnMetadata.prepareComplete(time.milliseconds());
                                return Either.right(
                                        new PreSendResult(txnMetadata, txnTransitMetadata));
                            }
                        case PREPARE_ABORT:
                            if (txnMarkerResult != TransactionResult.ABORT) {
                                return Either.left(logInvalidStateTransitionAndReturnError(
                                        transactionalId, txnMetadata.getState(), txnMarkerResult));

                            } else {
                                TxnTransitMetadata txnTransitMetadata =
                                        txnMetadata.prepareComplete(time.milliseconds());
                                return Either.right(
                                        new PreSendResult(txnMetadata, txnTransitMetadata));
                            }
                        case DEAD:
                        case PREPARE_EPOCH_FENCE:
                        default:
                            String errorMsg = String.format("Found transactionalId %s with state %s. "
                                    + "This is illegal as we should never have transitioned to "
                                    + "this state.", transactionalId, txnMetadata.getState());
                            log.error(errorMsg);
                            throw new IllegalStateException(errorMsg);
                    }
                }
            });
        } else {
            if (log.isDebugEnabled()) {
                log.debug("The transaction coordinator epoch has changed to {} after {} was "
                                + "successfully appended to the log for {} with old epoch {}",
                        epochAndTxnMetadata.getCoordinatorEpoch(), txnMarkerResult, transactionalId,
                        coordinatorEpoch);
            }
            errorsOrPreSendResult = Either.left(Errors.NOT_COORDINATOR);
        }

        if (errorsOrPreSendResult.isLeft()) {
            log.info("{} Aborting sending of transaction markers after appended {} to transaction log "
                            + "and returning {} error to client for {}'s EndTransaction request",
                    namespacePrefixForMetadata,
                    transactionalId, txnMarkerResult, errorsOrPreSendResult.getLeft());
            callback.accept(errorsOrPreSendResult.getLeft());
            return;
        }

        callback.accept(Errors.NONE);
        transactionMarkerChannelManager.addTxnMarkersToSend(
                coordinatorEpoch, txnMarkerResult, epochAndTxnMetadata.getTransactionMetadata(),
                errorsOrPreSendResult.getRight().getTxnTransitMetadata(), namespacePrefixForUserTopics);
    }

    private Errors logInvalidStateTransitionAndReturnError(String transactionalId,
                                                         TransactionState transactionState,
                                                         TransactionResult transactionResult) {
        log.debug("TransactionalId: {}'s state is {}, but received transaction marker result to send: {}",
                transactionalId, transactionState, transactionResult);
        return Errors.INVALID_TXN_STATE;
    }

    @VisibleForTesting
    protected void abortTimedOutTransactions(
            BiConsumer<TransactionStateManager.TransactionalIdAndProducerIdEpoch, Errors> onComplete) {
        for (TransactionStateManager.TransactionalIdAndProducerIdEpoch txnIdAndPidEpoch :
                txnManager.timedOutTransactions()) {
            txnManager.getTransactionState(txnIdAndPidEpoch.getTransactionalId())
                    .map(option -> option.map(epochAndTxnMetadata -> {
                        TransactionMetadata txnMetadata = epochAndTxnMetadata.getTransactionMetadata();
                        Either<Errors, TxnTransitMetadata> transitMetadata = txnMetadata.inLock(() -> {
                            if (txnMetadata.getProducerId() != txnIdAndPidEpoch.getProducerId()) {
                                log.error("Found incorrect producerId when expiring transactionalId: {}. "
                                                + "Expected producerId: {}. Found producerId: {}",
                                        txnIdAndPidEpoch.getTransactionalId(),
                                        txnIdAndPidEpoch.getProducerId(),
                                        txnMetadata.getProducerId());
                                return Either.left(Errors.INVALID_PRODUCER_ID_MAPPING);
                            } else if (txnMetadata.pendingTransitionInProgress()) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Skipping abort of timed out transaction {} since there "
                                                    + "is a pending state transition",
                                            txnIdAndPidEpoch.getTransactionalId());
                                }
                                return Either.left(Errors.CONCURRENT_TRANSACTIONS);
                            } else {
                                return Either.right(txnMetadata.prepareFenceProducerEpoch());
                            }
                        });
                        if (transitMetadata.getRight() != null) {
                            TxnTransitMetadata txnTransitMetadata = transitMetadata.getRight();
                            endTransaction(txnMetadata.getTransactionalId(),
                                    txnTransitMetadata.getProducerId(),
                                    txnTransitMetadata.getProducerEpoch(),
                                    TransactionResult.ABORT,
                                    false,
                                    errors -> onComplete.accept(txnIdAndPidEpoch, errors));
                        }
                        return null;
                    }));
        }
    }

    @VisibleForTesting
    protected void abortTimedOutTransactions() {
        this.abortTimedOutTransactions(onEndTransactionComplete);
    }

    /**
     * Startup logic executed at the same time when the server starts up.
     */
    public CompletableFuture<Void> startup(boolean enableTransactionalIdExpiration) {
        log.info("{} Starting up transaction coordinator ...", namespacePrefixForMetadata);

        // Abort timeout transactions
        scheduler.scheduleAtFixedRate(
                SafeRunnable.safeRun(this::abortTimedOutTransactions,
                        ex -> log.error("Uncaught exception in scheduled task transaction-abort", ex)),
                transactionConfig.getAbortTimedOutTransactionsIntervalMs(),
                transactionConfig.getAbortTimedOutTransactionsIntervalMs(),
                TimeUnit.MILLISECONDS);

        txnManager.startup(enableTransactionalIdExpiration);

        return this.producerIdManager.initialize().thenCompose(ignored -> {
            log.info("{} Startup transaction coordinator complete.", namespacePrefixForMetadata);
            isActive.set(true);
            return CompletableFuture.completedFuture(null);
        });
    }

    /**
     * Shutdown logic executed at the same time when server shuts down.
     * Ordering of actions should be reversed from the startup process.
     */
    public void shutdown() {
        log.info("{} Shutting down transaction coordinator ...", namespacePrefixForMetadata);
        producerIdManager.shutdown();
        txnManager.shutdown();
        transactionMarkerChannelManager.close();
        producerStateManagerSnapshotBuffer.shutdown();
        scheduler.shutdown();
        log.info("{} Shutdown transaction coordinator complete.", namespacePrefixForMetadata);
    }

}
