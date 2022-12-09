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


import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import io.streamnative.pulsar.handlers.kop.scala.Either;
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Transaction state manager.
 */
@Slf4j
public class TransactionStateManager {

    private final TransactionConfig transactionConfig;
    private final SystemTopicClient txnTopicClient;
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    // Number of partitions for the transaction log topic.
    private final int transactionTopicPartitionCount;

    // Partitions of transaction topic that are being loaded, state lock should be called BEFORE accessing this set.
    @VisibleForTesting
    protected final Set<Integer> loadingPartitions = Sets.newHashSet();

    // partitions of transaction topic that are being removed, state lock should be called BEFORE accessing this set.
    @VisibleForTesting
    protected final Set<Integer> leavingPartitions = Sets.newHashSet();

    private final Map<Integer, CompletableFuture<Producer<ByteBuffer>>> txnLogProducerMap = Maps.newConcurrentMap();
    private final Map<Integer, CompletableFuture<Reader<ByteBuffer>>> txnLogReaderMap = Maps.newConcurrentMap();

    // Transaction metadata cache indexed by assigned transaction topic partition ids
    // Map <partitionId, <transactionId, TransactionMetadata>>
    @VisibleForTesting
    protected final Map<Integer, Map<String, TransactionMetadata>> transactionMetadataCache = Maps.newConcurrentMap();

    private final ScheduledExecutorService scheduler;

    private final Time time;

    public TransactionStateManager(TransactionConfig transactionConfig,
                                   SystemTopicClient txnTopicClient,
                                   ScheduledExecutorService scheduler,
                                   Time time) {
        this.transactionConfig = transactionConfig;
        this.txnTopicClient = txnTopicClient;
        this.scheduler = scheduler;
        this.transactionTopicPartitionCount = transactionConfig.getTransactionLogNumPartitions();
        this.time = time;
    }

    // this is best-effort expiration of an ongoing transaction which has been open for more than its
    // txn timeout value, we do not need to grab the lock on the metadata object upon checking its state
    // since the timestamp is volatile and we will get the lock when actually trying to transit the transaction
    // metadata to abort later.
    protected List<TransactionalIdAndProducerIdEpoch> timedOutTransactions() {
        long now = time.milliseconds();
        return CoreUtils.inReadLock(stateLock, () -> transactionMetadataCache.entrySet()
                .stream()
                .filter(entry -> !leavingPartitions.contains(entry.getKey()))
                .flatMap(entry -> entry.getValue().entrySet().stream().filter(txnMetadataEntry -> {
                    TransactionMetadata txnMetadata = txnMetadataEntry.getValue();
                    if (txnMetadata.pendingTransitionInProgress()) {
                        return false;
                    } else {
                        if (txnMetadata.getState().equals(TransactionState.ONGOING)) {
                            return txnMetadata.getTxnStartTimestamp() + txnMetadata.getTxnTimeoutMs() < now;
                        } else {
                            return false;
                        }
                    }
                }).map(txnMetadataEntry -> {
                    String txnId = txnMetadataEntry.getKey();
                    TransactionMetadata txnMetadata = txnMetadataEntry.getValue();
                    return new TransactionalIdAndProducerIdEpoch(txnId, txnMetadata
                            .getProducerId(), txnMetadata.getProducerEpoch());
                }))
                .collect(Collectors.toList())
        );
    }

    public void startup(boolean enableTransactionalIdExpiration) {
        if (enableTransactionalIdExpiration){
            this.enableTransactionalIdExpiration();
        }
    }

    private void enableTransactionalIdExpiration() {
        scheduler.scheduleAtFixedRate(
                this::removeExpiredTransactionalIds,
                transactionConfig.getRemoveExpiredTransactionalIdsIntervalMs(),
                transactionConfig.getRemoveExpiredTransactionalIdsIntervalMs(),
                TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    protected CompletableFuture<Void> removeExpiredTransactionalIds() {
        return CoreUtils.inReadLock(stateLock, () -> {
            List<CompletableFuture<Void>> collect = transactionMetadataCache.entrySet().stream().map(entry -> {
                Integer partitionId = entry.getKey();
                Map<String, TransactionMetadata> partitionCacheEntry = entry.getValue();
                TopicPartition transactionPartition =
                        new TopicPartition(transactionConfig.getTransactionMetadataTopicName(), partitionId);
                return removeExpiredTransactionalIds(transactionPartition, partitionCacheEntry);
            }).collect(Collectors.toList());
            return FutureUtils.collect(collect).thenAccept(__ -> {});
        });
    }

    private CompletableFuture<Void> removeExpiredTransactionalIds(TopicPartition transactionPartition,
                                               Map<String, TransactionMetadata> txnMetadataCacheEntry) {
        return CoreUtils.inReadLock(stateLock, () -> {
            long currentTimeMs = time.milliseconds();
            List<CompletableFuture<Void>> removeExpiredTransactionalFutures = Lists.newArrayList();

            txnMetadataCacheEntry.values().forEach(txnMetadata -> {
                String transactionalId = txnMetadata.getTransactionalId();
                txnMetadata.inLock(() -> {
                    if (!txnMetadata.getPendingState().isPresent() && shouldExpire(txnMetadata, currentTimeMs)) {
                        byte[] tombstone = new TransactionLogKey(txnMetadata.getTransactionalId()).toBytes();
                        TransactionMetadata.TxnTransitMetadata transitMetadata = txnMetadata.prepareDead();
                        removeExpiredTransactionalFutures.add(
                                writeTombstoneForExpiredTransactionalIds(
                                        transactionPartition,
                                        new TransactionalIdCoordinatorEpochAndMetadata(transactionalId,
                                                transitMetadata),
                                        tombstone
                                ));
                    }
                    return null;
                });
            });
            return FutureUtils.collect(removeExpiredTransactionalFutures).thenAccept(__ -> {}).exceptionally(ex -> {
                log.error("Error to remove tombstones for expired transactional Ids.", ex);
                return null;
            });
        });
    }

    private CompletableFuture<Void> writeTombstoneForExpiredTransactionalIds(
            TopicPartition transactionPartition,
            TransactionalIdCoordinatorEpochAndMetadata expiredForPartition,
            byte[] tombstone) {
        return CoreUtils.inReadLock(stateLock, () -> appendTombstone(
                transactionPartition.partition(), tombstone)
                .whenComplete((__, ex) -> {
                    CoreUtils.inReadLock(stateLock, () -> {
                        Map<String, TransactionMetadata> partitionCacheEntry =
                                transactionMetadataCache.get(transactionPartition.partition());
                        if (partitionCacheEntry != null) {
                            String transactionalId = expiredForPartition.getTransactionalId();
                            TransactionMetadata txnMetadata = partitionCacheEntry.get(transactionalId);
                            txnMetadata.inLock(() -> {
                                if (txnMetadata.getPendingState().isPresent()
                                        && txnMetadata.getPendingState().get().equals(TransactionState.DEAD)
                                        && txnMetadata.getProducerEpoch()
                                        == expiredForPartition.getTransitMetadata().getProducerEpoch()
                                        && ex == null
                                ) {
                                    partitionCacheEntry.remove(transactionalId);
                                } else {
                                    log.warn("Failed to remove expired transactionalId: {} from cache. "
                                                    + "Tombstone append error: {}"
                                                    + "pendingState: {}, producerEpoch: {}, "
                                                    + "expected producerEpoch: {}",
                                            transactionalId, ex, txnMetadata.getPendingState(),
                                            txnMetadata.getProducerEpoch(),
                                            expiredForPartition.getTransitMetadata().getProducerEpoch());
                                    txnMetadata.setPendingState(Optional.empty());
                                }
                                return null;
                            });
                        }
                        return null;
                    });
                }));
    }

    protected CompletableFuture<Void> appendTombstone(int partition, byte[] tombstone) {
        return getProducer(partition).thenComposeAsync(producer ->
                producer.newMessage()
                        .keyBytes(tombstone)
                        .value(null)
                        .sendAsync(), scheduler).thenAcceptAsync(messageId -> {
                            if (log.isDebugEnabled()) {
                                log.debug("Append tombstone success, msgId: [{}], tombstone: [{}]",
                                        messageId,
                                        TransactionLogKey.decode(ByteBuffer.wrap(tombstone),
                                                TransactionLogKey.HIGHEST_SUPPORTED_VERSION));
                            }
                        }, scheduler);
    }

    private boolean shouldExpire(TransactionMetadata txnMetadata, Long currentTimeMs){
            return txnMetadata.getState().isExpirationAllowed() && txnMetadata.getTxnLastUpdateTimestamp()
                    <= (currentTimeMs - transactionConfig.getTransactionalIdExpirationMs());
    }

    @Data
    @AllArgsConstructor
    private static class TransactionalIdCoordinatorEpochAndMetadata {
        private String transactionalId;
        private TransactionMetadata.TxnTransitMetadata transitMetadata;
    }

    @Data
    @AllArgsConstructor
    protected static class TransactionalIdAndProducerIdEpoch {
        private String transactionalId;
        private Long producerId;
        private Short producerEpoch;
    }

    /**
     * TxnMetadataCacheEntry.
     */
    @AllArgsConstructor
    private static class TxnMetadataCacheEntry {
        private Integer coordinatorEpoch;
        private Map<String, TransactionMetadata> metadataPerTransactionalId;

        @Override
        public String toString() {
            return "TxnMetadataCacheEntry{"
                    + "coordinatorEpoch=" + coordinatorEpoch
                    + ", numTransactionalEntries=" + metadataPerTransactionalId.size()
                    + '}';
        }
    }

    /**
     * CoordinatorEpoch and TxnMetadata.
     */
    @Data
    @AllArgsConstructor
    public static class CoordinatorEpochAndTxnMetadata {
        private Integer coordinatorEpoch;
        private TransactionMetadata transactionMetadata;
    }

    /**
     * TransactionalId, coordinatorEpoch and TransitMetadata.
     */
    @AllArgsConstructor
    private static class TransactionalIdAndTransitMetadata {
        private final String transactionalId;
        private TransactionResult result;
        private TransactionMetadata txnMetadata;
        private TransactionMetadata.TxnTransitMetadata transitMetadata;
    }

    public void appendTransactionToLog(String transactionalId,
                                       int coordinatorEpoch,
                                       TransactionMetadata.TxnTransitMetadata newMetadata,
                                       ResponseCallback responseCallback,
                                       RetryOnError retryOnError) {

        // generate the message for this transaction metadata
        TopicPartition topicPartition = new TopicPartition(
                transactionConfig.getTransactionMetadataTopicName(), partitionFor(transactionalId));

        CoreUtils.inReadLock(stateLock, () -> {
            // we need to hold the read lock on the transaction metadata cache until appending to local log returns;
            // this is to avoid the case where an emigration followed by an immigration could have completed after the
            // check returns and before appendRecords() is called, since otherwise entries with a high coordinator epoch
            // could have been appended to the log in between these two events, and therefore appendRecords() would
            // append entries with an old coordinator epoch that can still be successfully replicated on followers
            // and make the log in a bad state.
            Either<Errors, Optional<CoordinatorEpochAndTxnMetadata>> errorsAndData =
                    getTransactionState(transactionalId);

            if (errorsAndData.isLeft()) {
                responseCallback.fail(errorsAndData.getLeft());
                return null;
            }

            if (!errorsAndData.getRight().isPresent()) {
                responseCallback.fail(Errors.NOT_COORDINATOR);
                return null;
            }

            CoordinatorEpochAndTxnMetadata epochAndMetadata = errorsAndData.getRight().get();
            TransactionMetadata metadata = epochAndMetadata.getTransactionMetadata();
            metadata.inLock(() -> {
                if (epochAndMetadata.getCoordinatorEpoch() != coordinatorEpoch) {
                    // the coordinator epoch has changed, reply to client immediately with NOT_COORDINATOR
                    responseCallback.fail(Errors.NOT_COORDINATOR);
                    return null;
                }
                storeTxnLog(transactionalId, newMetadata).thenAccept(messageId -> {
                    Map<TopicPartition, ProduceResponse.PartitionResponse> partitionResponseMap = new HashMap<>();
                    partitionResponseMap.put(topicPartition, new ProduceResponse.PartitionResponse(Errors.NONE));
                    updateCacheCallback(transactionalId, newMetadata, topicPartition, coordinatorEpoch,
                            partitionResponseMap, responseCallback, retryOnError);
                    if (log.isDebugEnabled()) {
                        log.debug("Appending new metadata {} for transaction id {} to the local transaction log with "
                                + "messageId {}", newMetadata, transactionalId, messageId);
                    }
                }).exceptionally(ex -> {
                    log.error("Store transactional log failed, transactionalId : {}, metadata: [{}].",
                            transactionalId, newMetadata, ex);
                    responseCallback.fail(Errors.forException(ex));
                    return null;
                });
                return null;
            });
            return null;
        });
    }

    // set the callback function to update transaction status in cache after log append completed
    private void updateCacheCallback(String transactionalId,
                                     TransactionMetadata.TxnTransitMetadata newMetadata,
                                     TopicPartition topicPartition,
                                     int coordinatorEpoch,
                                     Map<TopicPartition, ProduceResponse.PartitionResponse> responseStatus,
                                     ResponseCallback responseCallback,
                                     RetryOnError retryOnError) {
        // the append response should only contain the topics partition
        if (responseStatus.size() != 1 || !responseStatus.containsKey(topicPartition)) {
            throw new IllegalStateException(String.format("Append status %s should only have one partition %s",
                    responseStatus, topicPartition));
        }

        ProduceResponse.PartitionResponse status = responseStatus.get(topicPartition);
        Errors errors = statusCheck(transactionalId, newMetadata, status);

        if (errors == Errors.NONE) {
            errors = validStatus(transactionalId, newMetadata, errors, coordinatorEpoch);
        } else {
            invalidStatus(transactionalId, newMetadata, coordinatorEpoch, errors, retryOnError);
        }

        if (errors != Errors.NONE) {
            responseCallback.fail(errors);
        } else {
            responseCallback.complete();
        }
    }

    private Errors statusCheck(String transactionalId,
                               TransactionMetadata.TxnTransitMetadata newMetadata,
                               ProduceResponse.PartitionResponse status) {
        if (status.error == Errors.NONE) {
            return Errors.NONE;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Appending {}'s new metadata {} failed due to {}",
                        transactionalId, newMetadata, status.error.exceptionName());
            }

            // transform the log append error code to the corresponding coordinator error code
            switch (status.error) {
                case UNKNOWN_TOPIC_OR_PARTITION:
                case NOT_ENOUGH_REPLICAS:
                case NOT_ENOUGH_REPLICAS_AFTER_APPEND:
                case REQUEST_TIMED_OUT:
                    // note that for timed out request we return NOT_AVAILABLE error code to let client retry
                    return Errors.COORDINATOR_NOT_AVAILABLE;
                case KAFKA_STORAGE_ERROR:
//                case Errors.NOT_LEADER_OR_FOLLOWER:
                    return Errors.NOT_COORDINATOR;
                case MESSAGE_TOO_LARGE:
                case RECORD_LIST_TOO_LARGE:
                default:
                    return Errors.UNKNOWN_SERVER_ERROR;
            }
        }
    }

    private Errors validStatus(String transactionalId,
                               TransactionMetadata.TxnTransitMetadata newMetadata,
                               Errors errors,
                               int coordinatorEpoch) {
        // now try to update the cache: we need to update the status in-place instead of
        // overwriting the whole object to ensure synchronization
        Either<Errors, Optional<CoordinatorEpochAndTxnMetadata>> errorsAndData =
                getTransactionState(transactionalId);

        if (errorsAndData.isLeft()) {
            log.info("Accessing the cached transaction metadata for {} returns {} error; "
                            + "aborting transition to the new metadata and setting the error in the callback",
                    transactionalId, errorsAndData.getLeft());
            return errorsAndData.getLeft();
        } else if (!errorsAndData.getRight().isPresent()) {
            // this transactional id no longer exists, maybe the corresponding partition has already been migrated
            // out. return NOT_COORDINATOR to let the client re-discover the transaction coordinator
            log.info("The cached coordinator metadata does not exist in the cache anymore for {} after appended "
                            + "its new metadata {} to the transaction log (txn topic partition {}) while it was {}"
                            + " before appending; " + "aborting transition to the new metadata and returning {} "
                            + "in the callback",
                    transactionalId, newMetadata, partitionFor(transactionalId), coordinatorEpoch,
                    Errors.NOT_COORDINATOR);
            return Errors.NOT_COORDINATOR;
        } else {
            TransactionMetadata metadata = errorsAndData.getRight().get().transactionMetadata;

            return metadata.inLock(() -> {
                if (errorsAndData.getRight().get().coordinatorEpoch != coordinatorEpoch) {
                    // the cache may have been changed due to txn topic partition emigration and immigration,
                    // in this case directly return NOT_COORDINATOR to client and let it to re-discover the
                    // transaction coordinator
                    log.info("The cached coordinator epoch for {} has changed to {} after appended its new "
                                    + "metadata {} to the transaction log (txn topic partition {}) while it was "
                                    + "{} before appending; aborting transition to the new metadata and returning "
                                    + "{} in the callback",
                            transactionalId, coordinatorEpoch, newMetadata, partitionFor(transactionalId),
                            coordinatorEpoch, Errors.NOT_CONTROLLER);
                    return Errors.NOT_COORDINATOR;
                } else {
                    try {
                        if (log.isDebugEnabled()) {
                            log.debug("Updating {}'s transaction state to {} with coordinator epoch {} for {} "
                                    + "successed", transactionalId, newMetadata, coordinatorEpoch, transactionalId);
                        }
                        metadata.completeTransitionTo(newMetadata);
                        return errors;
                    } catch (IllegalStateException ex) {
                        log.error("Failed to complete transition.", ex);
                        return Errors.UNKNOWN_SERVER_ERROR;
                    }
                }
            });
        }
    }

    private void invalidStatus(String transactionalId,
                               TransactionMetadata.TxnTransitMetadata newMetadata,
                               int coordinatorEpoch,
                               Errors errors,
                               RetryOnError retryOnError) {
        Either<Errors, Optional<CoordinatorEpochAndTxnMetadata>> errorsAndData =
                getTransactionState(transactionalId);

        // Reset the pending state when returning an error, since there is no active transaction for the
        // transactional id at this point.
        if (errorsAndData.isLeft()) {
            // Do nothing here, since we want to return the original append error to the user.
            log.info("TransactionalId {} append transaction log for {} transition failed due to {}, aborting state "
                    + "transition and returning the error in the callback since retrieving metadata "
                    + "returned {}", transactionalId, newMetadata, errors, errorsAndData.getLeft());

        } else if (!errorsAndData.getRight().isPresent()) {
            // Do nothing here, since we want to return the original append error to the user.
            log.info("TransactionalId {} append transaction log for {} transition failed due to {}, aborting state "
                    + "transition and returning the error in the callback since metadata is not available in the "
                    + "cache anymore", transactionalId, newMetadata, errors);
        } else {
            TransactionMetadata metadata = errorsAndData.getRight().get().transactionMetadata;
            metadata.inLock(() -> {
                if (errorsAndData.getRight().get().coordinatorEpoch == coordinatorEpoch) {
                    if (retryOnError.retry(errors)) {
                        log.info("TransactionalId {} append transaction log for {} transition failed due to {}, "
                                        + "not resetting pending state {} but just returning the error in the callback "
                                        + "to let the caller retry",
                                metadata.getTransactionalId(), newMetadata, errors,
                                metadata.getPendingState());
                    } else {
                        log.info("TransactionalId {} append transaction log for {} transition failed due to {}, "
                                    + "resetting pending state from {}, aborting state transition and returning {} in "
                                    + "the callback",
                                metadata.getTransactionalId(), newMetadata, errors,
                                metadata.getPendingState(), errorsAndData.getLeft());
                        metadata.setPendingState(Optional.empty());
                    }
                } else {
                    log.info("TransactionalId {} append transaction log for {} transition failed due to {}, "
                                    + "aborting state transition and returning the error in the callback since the "
                                    + "coordinator epoch has changed from {} to {}", metadata.getTransactionalId(),
                            newMetadata, errors, errorsAndData.getRight().get().coordinatorEpoch,
                            coordinatorEpoch);
                }
                return null;
            });
        }
    }

    /**
     * Response callback interface.
     */
    public interface ResponseCallback {
        void complete();
        void fail(Errors errors);
    }

    /**
     * Retry on error.
     */
    public interface RetryOnError {
        boolean retry(Errors errors);
    }

    public Either<Errors, Optional<CoordinatorEpochAndTxnMetadata>> getTransactionState(String transactionalId) {
        return getAndMaybeAddTransactionState(transactionalId, Optional.empty());
    }

    public Either<Errors, CoordinatorEpochAndTxnMetadata> putTransactionStateIfNotExists(
            TransactionMetadata metadata) {
        return getAndMaybeAddTransactionState(metadata.getTransactionalId(), Optional.of(metadata))
                .map(option -> option.orElseThrow(() -> new IllegalStateException(
                        "Unexpected empty transaction metadata returned while putting " + metadata)));
    }

    /**
     * Validate the given transaction timeout value.
     */
    public boolean validateTransactionTimeoutMs(int txnTimeoutMs) {
        return txnTimeoutMs <= transactionConfig.getTransactionMaxTimeoutMs() && txnTimeoutMs > 0;
    }

    /**
     * Get the transaction metadata associated with the given transactional id, or an error if
     * the coordinator does not own the transaction partition or is still loading it; if not found
     * either return None or create a new metadata and added to the cache.
     * This function is covered by the state read lock.
     */
    private Either<Errors, Optional<CoordinatorEpochAndTxnMetadata>> getAndMaybeAddTransactionState(
            String transactionalId,
            Optional<TransactionMetadata> createdTxnMetadataOpt) {
        return CoreUtils.inReadLock(stateLock, () -> {
            int partitionId = partitionFor(transactionalId);
            if (loadingPartitions.contains(partitionId)) {
                return Either.left(Errors.COORDINATOR_LOAD_IN_PROGRESS);
            } else if (leavingPartitions.contains(partitionId)) {
                return Either.left(Errors.NOT_COORDINATOR);
            } else {
                Map<String, TransactionMetadata> metadataMap = transactionMetadataCache.get(partitionId);
                if (metadataMap == null) {
                    return Either.left(Errors.NOT_COORDINATOR);
                }

                final Optional<TransactionMetadata> txnMetadata;
                TransactionMetadata txnMetadataCache = metadataMap.get(transactionalId);
                if (txnMetadataCache == null) {
                    createdTxnMetadataOpt.ifPresent(metadata -> metadataMap.put(transactionalId, metadata));
                    txnMetadata = createdTxnMetadataOpt;
                } else {
                    txnMetadata = Optional.of(txnMetadataCache);
                }
                return Either.right(txnMetadata.map(metadata -> new CoordinatorEpochAndTxnMetadata(-1, metadata)));
            }
        });
    }

    public int partitionFor(String transactionalId) {
        return TransactionCoordinator.partitionFor(transactionalId, transactionTopicPartitionCount);
    }

    /**
     * When this broker becomes a leader for a transaction log partition, load this partition and populate the
     * transaction metadata cache with the transactional ids. This operation must be resilient to any partial state
     * left off from the previous loading / unloading operation.
     */
    public CompletableFuture<Void> loadTransactionsForTxnTopicPartition(
            int partitionId, SendTxnMarkersCallback sendTxnMarkers) {
        TopicPartition topicPartition =
                new TopicPartition(transactionConfig.getTransactionMetadataTopicName(), partitionId);

        boolean alreadyLoading = CoreUtils.inWriteLock(stateLock, () -> {
            // The leavingPartitions of partitionId should have been removed in removeTransactionsForTxnTopicPartition,
            // If loadTransactionsForTxnTopicPartition success remove this partition, we just print a warning log,
            // ensure this operates have be recorded in logs, for future debugging.
            if (leavingPartitions.remove(partitionId)) {
                log.warn("Leaving partition: {} should have been removed.", partitionId);
            }
            boolean partitionAlreadyLoading = !loadingPartitions.add(partitionId);
            addLoadedTransactionsToCache(topicPartition.partition(), Maps.newConcurrentMap());
            return partitionAlreadyLoading;
        });
        if (alreadyLoading) {
            log.error("Partition {} is already loading", partitionId);
            return FutureUtil
                    .failedFuture(new IllegalStateException("Partition " + partitionId + " is already loading"));
        }
        log.info("Partition {} start loading", partitionId);

        long startTimeMs = SystemTime.SYSTEM.milliseconds();
        return getProducer(topicPartition.partition())
                .thenComposeAsync(producer ->
                        producer.newMessage().value(null).sendAsync(), scheduler)
                .thenComposeAsync(lastMsgId -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Successfully write a placeholder record into {} @ {}",
                                topicPartition, lastMsgId);
                    }
                    return getReader(topicPartition.partition()).thenComposeAsync(reader ->
                            loadTransactionMetadata(topicPartition.partition(), reader, lastMsgId), scheduler);
                }, scheduler).thenAcceptAsync(__ ->
                        completeLoadedTransactions(topicPartition, startTimeMs, sendTxnMarkers), scheduler)
                .exceptionally(ex -> {
                    log.error("Error to load transactions exceptions : [{}]", ex.getMessage());
                    loadingPartitions.remove(partitionId);
                    return null;
                });
    }

    private CompletableFuture<Void> loadTransactionMetadata(int partition,
                                                            Reader<ByteBuffer> reader,
                                                            MessageId lastMessageId) {
        if (log.isDebugEnabled()) {
            log.debug("Start load transaction metadata for partition {} till messageId {}", partition, lastMessageId);
        }
        CompletableFuture<Void> loadFuture = new CompletableFuture<>();
        Map<String, TransactionMetadata> transactionMetadataMap = new HashMap<>();
        loadNextTransaction(partition, reader, lastMessageId, loadFuture, transactionMetadataMap);
        return loadFuture;
    }

    private void loadNextTransaction(int partition,
                                     Reader<ByteBuffer> reader,
                                     MessageId lastMessageId,
                                     CompletableFuture<Void> loadFuture,
                                     Map<String, TransactionMetadata> transactionMetadataMap) {

        if (shuttingDown.get()) {
            loadFuture.completeExceptionally(
                    new IllegalStateException("Transaction metadata manager is shutting down."));
            return;
        }

        reader.readNextAsync().whenCompleteAsync((message, throwable) -> {
            if (throwable != null) {
                log.error("Failed to load transaction log.", throwable);
                loadFuture.completeExceptionally(throwable);
            }
            if (message.getMessageId().compareTo(lastMessageId) >= 0) {
                // reach the end of partition
                addLoadedTransactionsToCache(partition, transactionMetadataMap);
                loadFuture.complete(null);
                return;
            }

            // skip place holder
            if (!message.hasKey()) {
                loadNextTransaction(partition, reader, lastMessageId, loadFuture, transactionMetadataMap);
                return;
            }

            try {
                TransactionLogKey logKey = TransactionLogKey.decode(
                        ByteBuffer.wrap(message.getKeyBytes()), TransactionLogKey.HIGHEST_SUPPORTED_VERSION);
                String transactionId = logKey.getTransactionId();
                TransactionMetadata transactionMetadata =
                        TransactionLogValue.readTxnRecordValue(transactionId, message.getValue());
                if (transactionMetadata == null) {
                    // Should remove from transactionMetadataMap when it's tombstone message
                    transactionMetadataMap.remove(transactionId);
                } else {
                    transactionMetadataMap.put(logKey.getTransactionId(), transactionMetadata);
                }
                loadNextTransaction(partition, reader, lastMessageId, loadFuture, transactionMetadataMap);
            } catch (SchemaException | BufferUnderflowException ex) {
                log.error("Failed to decode transaction log with message {} for partition {}.",
                        message.getMessageId(), partition, ex);
                loadFuture.completeExceptionally(ex);
            }
        }, scheduler);
    }

    @VisibleForTesting
    protected void addLoadedTransactionsToCache(int txnTopicPartition,
                                              Map<String, TransactionMetadata> loadedTransactions) {
        Map<String, TransactionMetadata> previousTxnMetadataCacheEntry =
                transactionMetadataCache.put(txnTopicPartition, loadedTransactions);
        if (previousTxnMetadataCacheEntry != null && !previousTxnMetadataCacheEntry.isEmpty()) {
            log.warn("Unloaded transaction metadata {} from {} as part of loading metadata.",
                    previousTxnMetadataCacheEntry, txnTopicPartition);
        }
    }

    private void completeLoadedTransactions(TopicPartition topicPartition, long startTimeMs,
                                                               SendTxnMarkersCallback sendTxnMarkersCallback) {
        Map<String, TransactionMetadata> loadedTransactions = transactionMetadataCache.get(topicPartition.partition());
        long endTimeMs = SystemTime.SYSTEM.milliseconds();
        long totalLoadingTimeMs = endTimeMs - startTimeMs;
        log.info("Finished loading transaction metadata {} from {} in {} milliseconds",
                loadedTransactions.size(), topicPartition, totalLoadingTimeMs);

        CoreUtils.inWriteLock(stateLock, () -> {
            if (loadingPartitions.contains(topicPartition.partition())) {
                List<TransactionalIdAndTransitMetadata> transactionsPendingForCompletion = new ArrayList<>();

                for (Map.Entry<String, TransactionMetadata> entry : loadedTransactions.entrySet()) {
                    TransactionMetadata txnMetadata = entry.getValue();
                    txnMetadata.inLock(() -> {
                        switch (txnMetadata.getState()) {
                            case PREPARE_ABORT:
                                transactionsPendingForCompletion.add(
                                        new TransactionalIdAndTransitMetadata(
                                                entry.getKey(),
                                                TransactionResult.ABORT,
                                                txnMetadata,
                                                txnMetadata.prepareComplete(SystemTime.SYSTEM.milliseconds())
                                        ));
                                break;
                            case PREPARE_COMMIT:
                                transactionsPendingForCompletion.add(
                                        new TransactionalIdAndTransitMetadata(
                                                entry.getKey(),
                                                TransactionResult.COMMIT,
                                                txnMetadata,
                                                txnMetadata.prepareComplete(SystemTime.SYSTEM.milliseconds())
                                        ));
                                break;
                            default:
                                // no op
                        }
                        return null;
                    });
                }

                // We first remove the partition from loading partition then send out the markers for those pending to
                // be completed transactions, so that when the markers get sent the attempt of appending the complete
                // transaction log would not be blocked by the coordinator loading error.
                loadingPartitions.remove(topicPartition.partition());

                transactionsPendingForCompletion.forEach(pendingTxn -> {
                    sendTxnMarkersCallback.send(pendingTxn.result, pendingTxn.txnMetadata, pendingTxn.transitMetadata);
                });
            }
            loadingPartitions.remove(topicPartition.partition());
            return null;
        });
        log.info("Completed loading transaction metadata from {}", topicPartition);
    }

    public void removeTransactionsForTxnTopicPartition(int partition) {
        TopicPartition topicPartition =
                new TopicPartition(transactionConfig.getTransactionMetadataTopicName(), partition);
        log.info("Scheduling unloading transaction metadata from {}", topicPartition);

        CoreUtils.inWriteLock(stateLock, () -> {
            loadingPartitions.remove(partition);
            leavingPartitions.add(partition);
            return null;
        });

        scheduler.submit(() -> {
            CoreUtils.inWriteLock(stateLock, () -> {
                if (leavingPartitions.contains(partition)) {
                    transactionMetadataCache.remove(partition).forEach((txnId, metadata) -> {
                        log.info("Unloaded transaction metadata {} for {} following local partition deletion",
                                metadata, topicPartition);
                    });

                    // remove related producers and readers
                    CompletableFuture<Producer<ByteBuffer>> producer = txnLogProducerMap.remove(partition);
                    CompletableFuture<Reader<ByteBuffer>> reader = txnLogReaderMap.remove(partition);
                    if (producer != null) {
                        producer.thenApplyAsync(Producer::closeAsync, scheduler).whenCompleteAsync((ignore, t) -> {
                            if (t != null) {
                                log.error("Failed to close producer when remove partition {}.",
                                        producer.join().getTopic());
                            }
                        }, scheduler);
                    }
                    if (reader != null) {
                        reader.thenApplyAsync(Reader::closeAsync, scheduler).whenCompleteAsync((ignore, t) -> {
                            if (t != null) {
                                log.error("Failed to close reader when remove partition {}.",
                                        reader.join().getTopic());
                            }
                        }, scheduler);
                    }
                    leavingPartitions.remove(partition);
                }
                return null;
            });
        });
    }

    interface SendTxnMarkersCallback {
        void send(TransactionResult transactionResult, TransactionMetadata transactionMetadata,
                  TransactionMetadata.TxnTransitMetadata txnTransitMetadata);
    }

    private CompletableFuture<Producer<ByteBuffer>> getProducer(Integer partition) {
        return txnLogProducerMap.computeIfAbsent(partition, key -> {
            String topic = transactionConfig.getTransactionMetadataTopicName()
                    + TopicName.PARTITIONED_TOPIC_SUFFIX + partition;
            return txnTopicClient.newProducerBuilder().clone().topic(topic).createAsync();
        });
    }

    protected CompletableFuture<MessageId> storeTxnLog(String transactionalId,
                                                     TransactionMetadata.TxnTransitMetadata txnTransitMetadata) {
        byte[] keyBytes = new TransactionLogKey(transactionalId).toBytes();
        ByteBuffer valueByteBuffer = new TransactionLogValue(txnTransitMetadata).toByteBuffer();
        return getProducer(partitionFor(transactionalId)).thenCompose(producer ->
                producer.newMessage().keyBytes(keyBytes).value(valueByteBuffer).sendAsync());
    }

    private CompletableFuture<Reader<ByteBuffer>> getReader(Integer partition) {
        return txnLogReaderMap.computeIfAbsent(partition, key -> {
            String topic = transactionConfig.getTransactionMetadataTopicName()
                    + TopicName.PARTITIONED_TOPIC_SUFFIX + partition;
            return txnTopicClient.newReaderBuilder().clone().topic(topic)
                    .startMessageId(MessageId.earliest).readCompacted(true).createAsync();
        });
    }


    public void shutdown() {
        shuttingDown.set(true);
        loadingPartitions.clear();
        transactionMetadataCache.clear();
        List<CompletableFuture<Void>> txnLogProducerCloses = txnLogProducerMap.values().stream()
                .map(producerCompletableFuture -> producerCompletableFuture
                        .thenComposeAsync(Producer::closeAsync, scheduler))
                .collect(Collectors.toList());
        txnLogProducerMap.clear();
        List<CompletableFuture<Void>> txnLogReaderCloses = txnLogReaderMap.values().stream()
                .map(readerCompletableFuture -> readerCompletableFuture
                        .thenComposeAsync(Reader::closeAsync, scheduler))
                .collect(Collectors.toList());
        txnLogProducerMap.clear();
        FutureUtil.waitForAll(txnLogProducerCloses).whenCompleteAsync((ignore, t) -> {
            if (t != null) {
                log.error("Error when close all the {} txnLogProducers in TransactionStateManager",
                        txnLogProducerCloses.size(), t);
            }
            if (log.isDebugEnabled()) {
                log.debug("Closed all the {} txnLogProducers in TransactionStateManager", txnLogProducerCloses.size());
            }
        }, scheduler);

        FutureUtil.waitForAll(txnLogReaderCloses).whenCompleteAsync((ignore, t) -> {
            if (t != null) {
                log.error("Error when close all the {} txnLogReaders in TransactionStateManager",
                        txnLogReaderCloses.size(), t);
            }
            if (log.isDebugEnabled()) {
                log.debug("Closed all the {} txnLogReaders in TransactionStateManager.", txnLogReaderCloses.size());
            }
        }, scheduler);
        scheduler.shutdown();
        log.info("Shutdown transaction state manager complete.");
    }

}
