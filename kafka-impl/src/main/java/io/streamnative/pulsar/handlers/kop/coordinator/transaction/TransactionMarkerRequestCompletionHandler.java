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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;

/**
 * Transaction marker request completion handler.
 */
@AllArgsConstructor
@Slf4j
public class TransactionMarkerRequestCompletionHandler {

    private Integer brokerId;
    private TransactionStateManager txnStateManager;
    private TransactionMarkerChannelManager txnMarkerChannelManager;
    private List<TransactionMarkerChannelManager.TxnIdAndMarkerEntry> txnIdAndMarkerEntries;

    private static class AbortSendingRetryPartitions {
        private AtomicBoolean abortSending = new AtomicBoolean(false);
        private Set<TopicPartition> retryPartitions = new HashSet<>();
    }

    public void onComplete(WriteTxnMarkersResponse writeTxnMarkerResponse) {
        log.info("Received WriteTxnMarker response from node with correlation id $correlationId");

        for (TransactionMarkerChannelManager.TxnIdAndMarkerEntry txnIdAndMarker : txnIdAndMarkerEntries) {
            String transactionalId = txnIdAndMarker.getTransactionalId();
            WriteTxnMarkersRequest.TxnMarkerEntry txnMarker = txnIdAndMarker.getEntry();
            Map<TopicPartition, Errors> errors = writeTxnMarkerResponse.errors(txnMarker.producerId());

            if (errors == null) {
                throw new IllegalStateException("WriteTxnMarkerResponse does not contain expected error map for "
                        + "producer id " + txnMarker.producerId());
            }

            ErrorsAndData<Optional<TransactionStateManager.CoordinatorEpochAndTxnMetadata>> errorsAndData =
                    txnStateManager.getTransactionState(transactionalId);

            if (errorsAndData.hasErrors()) {
                switch (errorsAndData.getErrors()) {
                    case NOT_COORDINATOR:
                        log.info("I am no longer the coordinator for {}; cancel sending transaction markers {} to the "
                                + "brokers", transactionalId, txnMarker);
                        txnMarkerChannelManager.removeMarkersForTxnId(transactionalId);
                        break;
                    case COORDINATOR_LOAD_IN_PROGRESS:
                        log.info("I am loading the transaction partition that contains {} which means the current "
                                + "markers have to be obsoleted; cancel sending transaction markers {} to the brokers",
                                transactionalId, txnMarker);
                        txnMarkerChannelManager.removeMarkersForTxnId(transactionalId);
                        break;
                    default:
                        throw new IllegalStateException("Unhandled error " + errorsAndData.getErrors()
                                + " when fetching current transaction state");
                }
                return;
            }

            if (!errorsAndData.getData().isPresent()) {
                throw new IllegalStateException("The coordinator still owns the transaction partition for "
                        + transactionalId + ", but there is no metadata in the cache; this is not expected");
            }

            AbortSendingRetryPartitions abortSendOrRetryPartitions =
                    hasAbortSendOrRetryPartitions(transactionalId, txnMarker, errorsAndData, errors);

            if (abortSendOrRetryPartitions.abortSending.get()) {
                return;
            }

            if (abortSendOrRetryPartitions.retryPartitions.isEmpty()) {
                txnMarkerChannelManager.maybeWriteTxnCompletion(transactionalId);
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Re-enqueuing {} transaction markers for transactional id {} under coordinator epoch {}",
                        txnMarker.transactionResult(), transactionalId, txnMarker.coordinatorEpoch());
            }

            // re-enqueue with possible new leaders of the partitions
            txnMarkerChannelManager.addTxnMarkersToBrokerQueue(
                    transactionalId,
                    txnMarker.producerId(),
                    txnMarker.producerEpoch(),
                    txnMarker.transactionResult(),
                    txnMarker.coordinatorEpoch(),
                    abortSendOrRetryPartitions.retryPartitions);
        }
    }

    private AbortSendingRetryPartitions hasAbortSendOrRetryPartitions(
            String transactionalId,
            WriteTxnMarkersRequest.TxnMarkerEntry txnMarker,
            ErrorsAndData<Optional<TransactionStateManager.CoordinatorEpochAndTxnMetadata>> errorsAndData,
            Map<TopicPartition, Errors> errors) {

        TransactionStateManager.CoordinatorEpochAndTxnMetadata epochAndMetadata = errorsAndData.getData().get();
        TransactionMetadata txnMetadata = epochAndMetadata.getTransactionMetadata();

        AbortSendingRetryPartitions abortSendingAndRetryPartitions = new AbortSendingRetryPartitions();

        if (epochAndMetadata.getCoordinatorEpoch() != txnMarker.coordinatorEpoch()) {
            // coordinator epoch has changed, just cancel it from the purgatory
            log.info("Transaction coordinator epoch for {} has changed from {} to {}; cancel sending transaction "
                            + "markers {} to the brokers", transactionalId, txnMarker.coordinatorEpoch(),
                    epochAndMetadata.getCoordinatorEpoch(), txnMarker);
            txnMarkerChannelManager.removeMarkersForTxnId(transactionalId);
            abortSendingAndRetryPartitions.abortSending.set(true);
        } else {
            txnMetadata.inLock(() -> {
                for (Map.Entry<TopicPartition, Errors> errorsEntry : errors.entrySet()) {
                    TopicPartition topicPartition = errorsEntry.getKey();
                    Errors error = errorsEntry.getValue();
                    switch (error) {
                        case NONE:
                            txnMetadata.removePartition(topicPartition);
                            break;
                        case CORRUPT_MESSAGE:
                        case MESSAGE_TOO_LARGE:
                        case RECORD_LIST_TOO_LARGE:
                        case INVALID_REQUIRED_ACKS: // these are all unexpected and fatal errors
                            throw new IllegalStateException("Received fatal error " + error.exceptionName()
                                    + " while sending txn marker for " + transactionalId);
                        case UNKNOWN_TOPIC_OR_PARTITION:
                        // this error was introduced in newer kafka client version,
                        // recover this condition after bump the kafka client version
                        // case NOT_LEADER_OR_FOLLOWER:
                        case NOT_ENOUGH_REPLICAS:
                        case NOT_ENOUGH_REPLICAS_AFTER_APPEND:
                        case REQUEST_TIMED_OUT:
                        case KAFKA_STORAGE_ERROR: // these are retriable errors
                            log.info("Sending {}'s transaction marker for partition {} has failed with error {}, "
                                    + "retrying with current coordinator epoch {}", transactionalId, topicPartition,
                                    error.exceptionName(), epochAndMetadata.getCoordinatorEpoch());
                            abortSendingAndRetryPartitions.retryPartitions.add(topicPartition);
                            break;
                        case INVALID_PRODUCER_EPOCH:
                            // producer or coordinator epoch has changed, this txn can now be ignored
                        case TRANSACTION_COORDINATOR_FENCED:
                            log.info("Sending {}'s transaction marker for partition {} has permanently failed "
                                    + "with error {} with the current coordinator epoch {}; cancel sending any "
                                    + "more transaction markers {} to the brokers", transactionalId, topicPartition,
                                    error.exceptionName(), epochAndMetadata.getCoordinatorEpoch(), txnMarker);
                            txnMarkerChannelManager.removeMarkersForTxnId(transactionalId);
                            abortSendingAndRetryPartitions.abortSending.set(true);
                            break;
                        case UNSUPPORTED_FOR_MESSAGE_FORMAT:
                        case UNSUPPORTED_VERSION:
                            // The producer would have failed to send data to the failed topic so we can safely
                            // remove the partition from the set waiting for markers
                            log.info("Sending {}'s transaction marker from partition {} has failed with  {}. "
                                    + "This partition will be removed from the set of partitions waiting for "
                                    + "completion", transactionalId, topicPartition, error.name());
                            txnMetadata.removePartition(topicPartition);
                            break;
                        default:
                            throw new IllegalStateException("Unexpected error " + error.exceptionName()
                                    + " while sending txn marker for $transactionalId");
                    }
                }
                return null;
            });
        }
        return abortSendingAndRetryPartitions;
    }


}
