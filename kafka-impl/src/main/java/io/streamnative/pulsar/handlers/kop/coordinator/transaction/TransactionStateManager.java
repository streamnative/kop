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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Utils;

/**
 * Transaction state manager.
 */
@Slf4j
public class TransactionStateManager {

    private final TransactionConfig transactionConfig;
    private final Map<String, TransactionMetadata> transactionStateMap;
    private ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    // Number of partitions for the transaction log topic.
    private int transactionTopicPartitionCount;

    // Partitions of transaction topic that are being loaded, state lock should be called BEFORE accessing this set.
    private Set<TransactionPartitionAndLeaderEpoch> loadingPartitions = new HashSet<>();

    // Transaction metadata cache indexed by assigned transaction topic partition ids
    private Map<Integer, TxnMetadataCacheEntry> transactionMetadataCache = new HashMap<>();

    public TransactionStateManager(TransactionConfig transactionConfig) {
        this.transactionConfig = transactionConfig;
        this.transactionStateMap = new ConcurrentHashMap<>();
        this.transactionTopicPartitionCount = transactionConfig.getTransactionLogNumPartitions();
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
     * TransactionPartition and leader epoch.
     */
    @AllArgsConstructor
    private static class TransactionPartitionAndLeaderEpoch {
        private Integer txnPartitionId;
        private final Integer coordinatorEpoch;
    }

    /**
     * TransactionalId, coordinatorEpoch and TransitMetadata.
     */
    @AllArgsConstructor
    private static class TransactionalIdCoordinatorEpochAndTransitMetadata {
        private final String transactionalId;
        private int coordinatorEpoch;
        TransactionResult result;
        TransactionMetadata txnMetadata;
        TransactionMetadata.TxnTransitMetadata transitMetadata;
    }

    public void appendTransactionToLog(String transactionalId,
                                       int coordinatorEpoch,
                                       TransactionMetadata.TxnTransitMetadata newMetadata,
                                       ResponseCallback responseCallback,
                                       RetryOnError retryOnError) {

        // generate the message for this transaction metadata
        TopicPartition topicPartition = new TopicPartition(
                Topic.TRANSACTION_STATE_TOPIC_NAME, partitionFor(transactionalId));

        CoreUtils.inReadLock(stateLock, () -> {
            // we need to hold the read lock on the transaction metadata cache until appending to local log returns;
            // this is to avoid the case where an emigration followed by an immigration could have completed after the
            // check returns and before appendRecords() is called, since otherwise entries with a high coordinator epoch
            // could have been appended to the log in between these two events, and therefore appendRecords() would
            // append entries with an old coordinator epoch that can still be successfully replicated on followers
            // and make the log in a bad state.
            ErrorsAndData<Optional<CoordinatorEpochAndTxnMetadata>> errorsAndData =
                    getTransactionState(transactionalId);

            if (errorsAndData.hasErrors()) {
                responseCallback.fail(errorsAndData.getErrors());
                return null;
            }

            if (!errorsAndData.getData().isPresent()) {
                responseCallback.fail(Errors.NOT_COORDINATOR);
                return null;
            }

            CoordinatorEpochAndTxnMetadata epochAndMetadata = errorsAndData.getData().get();
            TransactionMetadata metadata = epochAndMetadata.getTransactionMetadata();
            metadata.inLock(() -> {
                if (epochAndMetadata.getCoordinatorEpoch() != coordinatorEpoch) {
                    // the coordinator epoch has changed, reply to client immediately with NOT_COORDINATOR
                    responseCallback.fail(Errors.NOT_COORDINATOR);
                    return null;
                } else {
                    // do not need to check the metadata object itself since no concurrent thread should be able to
                    // modify it under the same coordinator epoch, so directly append to txn log now
                }
                // append log
                Map<TopicPartition, ProduceResponse.PartitionResponse> partitionResponseMap = new HashMap<>();
                partitionResponseMap.put(topicPartition, new ProduceResponse.PartitionResponse(Errors.NONE));
                updateCacheCallback(transactionalId, newMetadata, topicPartition, coordinatorEpoch,
                        partitionResponseMap, responseCallback, retryOnError);
                log.info("Appending new metadata {} for transaction id {} with coordinator epoch {} "
                        + "to the local transaction log", newMetadata, transactionalId, coordinatorEpoch);
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
        ErrorsAndData<Void> result = new ErrorsAndData<>();
        if (status.error == Errors.NONE) {
            result.setErrors(Errors.NONE);
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
                    result.setErrors(Errors.COORDINATOR_NOT_AVAILABLE);
                    break;
//                case Errors.NOT_LEADER_OR_FOLLOWER:
                case KAFKA_STORAGE_ERROR:
                    result.setErrors(Errors.NOT_COORDINATOR);
                    break;
                case MESSAGE_TOO_LARGE:
                case RECORD_LIST_TOO_LARGE:
                    result.setErrors(Errors.UNKNOWN_SERVER_ERROR);
                    break;
                default:
                    result.setErrors(Errors.UNKNOWN_SERVER_ERROR);
            }
        }

        if (!result.hasErrors()) {
            // now try to update the cache: we need to update the status in-place instead of
            // overwriting the whole object to ensure synchronization
            ErrorsAndData<Optional<CoordinatorEpochAndTxnMetadata>> errorsAndData =
                    getTransactionState(transactionalId);

            if (errorsAndData.hasErrors()) {
                log.info("Accessing the cached transaction metadata for {} returns {} error; "
                                + "aborting transition to the new metadata and setting the error in the callback",
                        transactionalId, errorsAndData.getErrors());
                result.setErrors(errorsAndData.getErrors());
            } else if (!errorsAndData.getData().isPresent()) {
                // this transactional id no longer exists, maybe the corresponding partition has already been migrated
                // out. return NOT_COORDINATOR to let the client re-discover the transaction coordinator
                log.info("The cached coordinator metadata does not exist in the cache anymore for {} after appended "
                                + "its new metadata {} to the transaction log (txn topic partition {}) while it was {}"
                                + " before appending; " + "aborting transition to the new metadata and returning {} "
                                + "in the callback",
                        transactionalId, newMetadata, partitionFor(transactionalId), coordinatorEpoch,
                        Errors.NOT_COORDINATOR);
                result.setErrors(Errors.NOT_COORDINATOR);
            } else {
                TransactionMetadata metadata = errorsAndData.getData().get().transactionMetadata;

                metadata.inLock(() -> {
                    if (errorsAndData.getData().get().coordinatorEpoch != coordinatorEpoch) {
                        // the cache may have been changed due to txn topic partition emigration and immigration,
                        // in this case directly return NOT_COORDINATOR to client and let it to re-discover the
                        // transaction coordinator
                        log.info("The cached coordinator epoch for {} has changed to {} after appended its new "
                                        + "metadata {} to the transaction log (txn topic partition {}) while it was "
                                        + "{} before appending; aborting transition to the new metadata and returning "
                                        + "{} in the callback",
                                transactionalId, coordinatorEpoch, newMetadata, partitionFor(transactionalId),
                                coordinatorEpoch, Errors.NOT_CONTROLLER);
                        result.setErrors(Errors.NOT_COORDINATOR);
                    } else {
                        metadata.completeTransitionTo(newMetadata);
                        if (log.isDebugEnabled()) {
                            log.debug("Updating {}'s transaction state to {} with coordinator epoch {} for {} "
                                    + "succeezded", transactionalId, newMetadata, coordinatorEpoch, transactionalId);

                        }
                    }
                    return null;
                });
            }
        } else {
            ErrorsAndData<Optional<CoordinatorEpochAndTxnMetadata>> errorsAndData =
                    getTransactionState(transactionalId);

            // Reset the pending state when returning an error, since there is no active transaction for the
            // transactional id at this point.
            if (errorsAndData.hasErrors()) {
                // Do nothing here, since we want to return the original append error to the user.
                log.info("TransactionalId {} append transaction log for {} transition failed due to {}, aborting state "
                        + "transition and returning the error in the callback since retrieving metadata "
                        + "returned {}", transactionalId, newMetadata, result.getErrors(), errorsAndData.getErrors());

            } else if (!errorsAndData.getData().isPresent()) {
                // Do nothing here, since we want to return the original append error to the user.
                log.info("TransactionalId {} append transaction log for {} transition failed due to {}, aborting state "
                        + "transition and returning the error in the callback since metadata is not available in the "
                        + "cache anymore", transactionalId, newMetadata, result.getErrors());
            } else {
                TransactionMetadata metadata = errorsAndData.getData().get().transactionMetadata;
                metadata.inLock(() -> {
                    if (errorsAndData.getData().get().coordinatorEpoch == coordinatorEpoch) {
                        if (retryOnError.retry(result.getErrors())) {
                            log.info("TransactionalId {} append transaction log for {} transition failed due to {}, "
                                    + "not resetting pending state {} but just returning the error in the callback "
                                    + "to let the caller retry",
                                    metadata.getTransactionalId(), newMetadata, result.getErrors(),
                                    metadata.getPendingState());
                        } else {
                            log.info("TransactionalId {} append transaction log for {} transition failed due to {}, "
                                    + "resetting pending state from {}, aborting state transition and returning {} in "
                                    + "the callback",
                                    metadata.getTransactionalId(), newMetadata, result.getErrors(),
                                    metadata.getPendingState(), result.getErrors());
                            metadata.setPendingState(Optional.empty());
                        }
                    } else {
                        log.info("TransactionalId {} append transaction log for {} transition failed due to {}, "
                                + "aborting state transition and returning the error in the callback since the "
                                + "coordinator epoch has changed from {} to {}", metadata.getTransactionalId(),
                                newMetadata, result.getErrors(), errorsAndData.getData().get().coordinatorEpoch,
                                coordinatorEpoch);
                    }
                    return null;
                });
            }
        }

        if (result.hasErrors()) {
            responseCallback.fail(result.getErrors());
        } else {
            responseCallback.complete();
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

    public ByteBuf getWriteMarker(String transactionalId) {
        TransactionMetadata metadata = transactionStateMap.get(transactionalId);
        WriteTxnMarkersRequest.TxnMarkerEntry txnMarkerEntry = new WriteTxnMarkersRequest.TxnMarkerEntry(
                metadata.getProducerId(),
                metadata.getProducerEpoch(),
                1,
                TransactionResult.COMMIT,
                new ArrayList<>(metadata.getTopicPartitions()));
        WriteTxnMarkersRequest txnMarkersRequest = new WriteTxnMarkersRequest.Builder(
                Lists.newArrayList(txnMarkerEntry)).build();
        RequestHeader requestHeader = new RequestHeader(ApiKeys.WRITE_TXN_MARKERS, txnMarkersRequest.version(), "", -1);
        return RequestUtils.serializeRequest(txnMarkersRequest.version(), requestHeader, txnMarkersRequest);
    }

    public ErrorsAndData<Optional<CoordinatorEpochAndTxnMetadata>> getTransactionState(String transactionalId) {
        return getAndMaybeAddTransactionState(transactionalId, Optional.empty());
    }

    public ErrorsAndData<Optional<CoordinatorEpochAndTxnMetadata>> putTransactionStateIfNotExists(
            TransactionMetadata metadata) {
        ErrorsAndData<Optional<CoordinatorEpochAndTxnMetadata>> errorsAndData =
                getAndMaybeAddTransactionState(metadata.getTransactionalId(), Optional.of(metadata));
        if (!errorsAndData.getData().isPresent()) {
            throw new IllegalStateException("Unexpected empty transaction metadata returned while putting " + metadata);
        }
        return errorsAndData;
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
    private ErrorsAndData<Optional<CoordinatorEpochAndTxnMetadata>> getAndMaybeAddTransactionState(
            String transactionalId,
            Optional<TransactionMetadata> createdTxnMetadataOpt) {
        return CoreUtils.inReadLock(stateLock, () -> {
            int partitionId = partitionFor(transactionalId);
            if (loadingPartitions.stream().anyMatch(
                    txnPartitionAndEpoch -> txnPartitionAndEpoch.txnPartitionId == partitionId)) {
                return new ErrorsAndData<>(Errors.CONCURRENT_TRANSACTIONS);
            } else {
                TxnMetadataCacheEntry cacheEntry = transactionMetadataCache.get(partitionId);
                if (cacheEntry == null) {
                    return new ErrorsAndData<>(Errors.NOT_COORDINATOR);
                }
                Optional<TransactionMetadata> txnMetadata;
                TransactionMetadata txnMetadataCache = cacheEntry.metadataPerTransactionalId.get(transactionalId);
                if (txnMetadataCache == null) {
                    if (createdTxnMetadataOpt.isPresent()) {
                        cacheEntry.metadataPerTransactionalId.put(transactionalId, createdTxnMetadataOpt.get());
                        txnMetadata = createdTxnMetadataOpt;
                    } else {
                        txnMetadata = Optional.empty();
                    }
                } else {
                    txnMetadata = Optional.of(txnMetadataCache);
                }

                return txnMetadata
                        .map(metadata ->
                                new ErrorsAndData<>(Optional.of(
                                        new CoordinatorEpochAndTxnMetadata(cacheEntry.coordinatorEpoch, metadata))))
                        .orElseGet(() -> new ErrorsAndData<>(Optional.empty()));
            }
        });
    }

    public int partitionFor(String transactionalId) {
        return Utils.abs(transactionalId.hashCode()) % transactionTopicPartitionCount;
    }

    /**
     * Add a transaction topic partition into the cache.
     */
    private void addLoadedTransactionsToCache(int txnTopicPartition,
                                              int coordinatorEpoch,
                                              Map<String, TransactionMetadata> loadedTransactions) {
        TxnMetadataCacheEntry txnMetadataCacheEntry = new TxnMetadataCacheEntry(coordinatorEpoch, loadedTransactions);
        TxnMetadataCacheEntry previousTxnMetadataCacheEntryOpt =
                transactionMetadataCache.put(txnTopicPartition, txnMetadataCacheEntry);

        log.warn("Unloaded transaction metadata {} from {} as part of loading metadata at epoch {}",
                previousTxnMetadataCacheEntryOpt, txnTopicPartition, coordinatorEpoch);
    }

    private Map<String, TransactionMetadata> loadTransactionMetadata(TopicPartition topicPartition,
                                                                     int coordinatorEpoch) {
        // TODO recover transaction metadata
        Map<String, TransactionMetadata> loadedTransactions = new HashMap<>();
        return loadedTransactions;
    }

    /**
     * When this broker becomes a leader for a transaction log partition, load this partition and populate the
     * transaction metadata cache with the transactional ids. This operation must be resilient to any partial state
     * left off from the previous loading / unloading operation.
     */
    public CompletableFuture<Void> loadTransactionsForTxnTopicPartition(
                                                                int partitionId,
                                                                int coordinatorEpoch,
                                                                SendTxnMarkersCallback sendTxnMarkers) {
        TopicPartition topicPartition = new TopicPartition(Topic.TRANSACTION_STATE_TOPIC_NAME, partitionId);
        TransactionPartitionAndLeaderEpoch partitionAndLeaderEpoch =
                new TransactionPartitionAndLeaderEpoch(partitionId, coordinatorEpoch);

        CoreUtils.inWriteLock(stateLock, () -> {
            loadingPartitions.add(partitionAndLeaderEpoch);
            return null;
        });

        long scheduleStartMs = SystemTime.SYSTEM.milliseconds();
        loadTransactions(scheduleStartMs, topicPartition, coordinatorEpoch, partitionAndLeaderEpoch, sendTxnMarkers);
        return CompletableFuture.completedFuture(null);
    }

    private void loadTransactions(long startTimeMs,
                                  TopicPartition topicPartition,
                                  int coordinatorEpoch,
                                  TransactionPartitionAndLeaderEpoch partitionAndLeaderEpoch,
                                  SendTxnMarkersCallback sendTxnMarkersCallback) {
        long schedulerTimeMs = SystemTime.SYSTEM.milliseconds() - startTimeMs;
        log.info("Loading transaction metadata from {} at epoch {}", topicPartition, coordinatorEpoch);
//        validateTransactionTopicPartitionCountIsStable();

        Map<String, TransactionMetadata> loadedTransactions = loadTransactionMetadata(topicPartition, coordinatorEpoch);
        long endTimeMs = SystemTime.SYSTEM.milliseconds();
        long totalLoadingTimeMs = endTimeMs - startTimeMs;
        log.info("Finished loading {} transaction metadata from {} in {} milliseconds, of which {} milliseconds "
                + "was spent in the scheduler.", loadedTransactions.size(), topicPartition, totalLoadingTimeMs,
                schedulerTimeMs);

        CoreUtils.inWriteLock(stateLock, () -> {
            if (loadingPartitions.contains(partitionAndLeaderEpoch)) {
                addLoadedTransactionsToCache(topicPartition.partition(), coordinatorEpoch, loadedTransactions);

                List<TransactionalIdCoordinatorEpochAndTransitMetadata> transactionsPendingForCompletion =
                        new ArrayList<>();

                for (Map.Entry<String, TransactionMetadata> entry : loadedTransactions.entrySet()) {
                    TransactionMetadata txnMetadata = entry.getValue();
                    txnMetadata.inLock(() -> {
                        switch (txnMetadata.getState()) {
                            case PREPARE_ABORT:
                                transactionsPendingForCompletion.add(
                                        new TransactionalIdCoordinatorEpochAndTransitMetadata(
                                                entry.getKey(),
                                                coordinatorEpoch,
                                                TransactionResult.ABORT,
                                                txnMetadata,
                                                txnMetadata.prepareComplete(SystemTime.SYSTEM.milliseconds())
                                        ));
                                break;
                            case PREPARE_COMMIT:
                                transactionsPendingForCompletion.add(
                                        new TransactionalIdCoordinatorEpochAndTransitMetadata(
                                                entry.getKey(),
                                                coordinatorEpoch,
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
                loadingPartitions.remove(partitionAndLeaderEpoch);

                transactionsPendingForCompletion.forEach(pendingTxn -> {
                    sendTxnMarkersCallback.send(pendingTxn.coordinatorEpoch, pendingTxn.result,
                            pendingTxn.txnMetadata, pendingTxn.transitMetadata);
                });
            }
            return null;
        });

        log.info("Completed loading transaction metadata from {} for coordinator epoch {}",
                topicPartition, coordinatorEpoch);
    }

    interface SendTxnMarkersCallback {
        void send(int coordinatorEpoch, TransactionResult transactionResult, TransactionMetadata transactionMetadata,
             TransactionMetadata.TxnTransitMetadata txnTransitMetadata);
    }

}
