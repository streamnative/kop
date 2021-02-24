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

import static io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionState.ONGOING;
import static io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionState.PREPARE_ABORT;
import static io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionState.PREPARE_COMMIT;
import static io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionState.PREPARE_EPOCH_FENCE;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.kop.KafkaRequestHandler;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionStateManager.CoordinatorEpochAndTxnMetadata;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionMetadata.TxnTransitMetadata;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.ProducerIdAndEpoch;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.EndTxnResponse;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.zookeeper.ZooKeeper;

/**
 * Transaction coordinator.
 */
@Slf4j
public class TransactionCoordinator {

    private final TransactionConfig transactionConfig;
    private final ProducerIdManager producerIdManager;
    private final TransactionStateManager txnManager;
    private final TransactionMarkerChannelManager transactionMarkerChannelManager;

    // map from topic to the map from initial offset to producerId
    private final Map<TopicName, NavigableMap<Long, Long>> activeOffsetPidMap = new HashMap<>();
    private final Map<TopicName, ConcurrentHashMap<Long, Long>> activePidOffsetMap = new HashMap<>();
    private final List<AbortedIndexEntry> abortedIndexList = new ArrayList<>();

    private TransactionCoordinator(TransactionConfig transactionConfig, Integer brokerId, ZooKeeper zkClient) {
        this.transactionConfig = transactionConfig;
        this.txnManager = new TransactionStateManager(transactionConfig);
        this.producerIdManager = new ProducerIdManager(brokerId, zkClient);
        this.transactionMarkerChannelManager = new TransactionMarkerChannelManager(null, null, false);
    }

    public static TransactionCoordinator of(TransactionConfig transactionConfig, Integer brokerId, ZooKeeper zkClient) {
        return new TransactionCoordinator(transactionConfig, brokerId, zkClient);
    }

    interface EndTxnCallback {
        void complete(Errors errors);
    }

    public CompletableFuture<Void> loadTransactionMetadata(int partition) {
        return txnManager.loadTransactionsForTxnTopicPartition(partition, -1,
                (coordinatorEpoch, transactionResult, transactionMetadata, txnTransitMetadata) -> {
            // TODO finish pending completion txn
        });
    }

    public CompletableFuture<Void> startup() {
        return this.producerIdManager.initialize();
    }

    public int partitionFor(String transactionalId) {
        return MathUtils.signSafeMod(
                transactionalId.hashCode(),
                transactionConfig.getTransactionLogNumPartitions()
        );
    }

    public String getTopicPartitionName() {
        return transactionConfig.getTransactionMetadataTopicName();
    }

    public String getTopicPartitionName(int partitionId) {
        return getTopicPartitionName() + PARTITIONED_TOPIC_SUFFIX + partitionId;
    }

    public void handleInitProducerId(String transactionalId, int transactionTimeoutMs,
                                     Optional<ProducerIdAndEpoch> expectedProducerIdAndEpoch,
                                     KafkaRequestHandler requestHandler,
                                     CompletableFuture<AbstractResponse> response) {
        if (transactionalId == null) {
            // if the transactional id is null, then always blindly accept the request
            // and return a new producerId from the producerId manager
            producerIdManager.generateProducerId().whenComplete((pid, throwable) -> {
                short producerEpoch = 0;
                if (throwable != null) {
                    response.complete(new InitProducerIdResponse(0, Errors.UNKNOWN_SERVER_ERROR, pid, producerEpoch));
                    return;
                }
                response.complete(new InitProducerIdResponse(0, Errors.NONE, pid, producerEpoch));
            });
        } else if (StringUtils.isEmpty(transactionalId)) {
            // if transactional id is empty then return error as invalid request. This is
            // to make TransactionCoordinator's behavior consistent with producer client
            response.complete(new InitProducerIdResponse(0, Errors.INVALID_REQUEST));
        } else if (!txnManager.validateTransactionTimeoutMs(transactionTimeoutMs)){
            // check transactionTimeoutMs is not larger than the broker configured maximum allowed value
            response.complete(new InitProducerIdResponse(0, Errors.INVALID_TRANSACTION_TIMEOUT));
        } else {
            ErrorsAndData<Optional<CoordinatorEpochAndTxnMetadata>> existMeta =
                    txnManager.getTransactionState(transactionalId);

            CompletableFuture<ErrorsAndData<Optional<CoordinatorEpochAndTxnMetadata>>>
                    epochAndTxnMetaFuture = new CompletableFuture<>();
            if (!existMeta.getData().isPresent()) {
                producerIdManager.generateProducerId().whenComplete((pid, throwable) -> {
                    short producerEpoch = 0;
                    if (throwable != null) {
                        response.complete(new InitProducerIdResponse(
                                0, Errors.UNKNOWN_SERVER_ERROR, -1, producerEpoch));
                        return;
                    }
                    TransactionMetadata newMetadata = TransactionMetadata.builder()
                            .transactionalId(transactionalId)
                            .producerId(pid)
                            .producerEpoch(producerEpoch)
                            .state(TransactionState.EMPTY)
                            .topicPartitions(new HashSet<>())
                            .build();
                    epochAndTxnMetaFuture.complete(txnManager.putTransactionStateIfNotExists(newMetadata));
//                    response.complete(new InitProducerIdResponse(
//                            0, Errors.NONE, newMetadata.getProducerId(), newMetadata.getProducerEpoch()));
                });
            } else {
                // TODO generate monotonically increasing epoch
                // TODO conflict resolve
                epochAndTxnMetaFuture.complete(existMeta);
//                response.complete(new InitProducerIdResponse(
//                        0, Errors.NONE, metadata.getProducerId(), metadata.getProducerEpoch()));
            }

            epochAndTxnMetaFuture.whenComplete((epochAndTxnMeta, throwable) -> {
                int coordinatorEpoch = epochAndTxnMeta.getData().get().getCoordinatorEpoch();
                TransactionMetadata txnMetadata = epochAndTxnMeta.getData().get().getTransactionMetadata();

                txnMetadata.inLock(() -> {
                    CompletableFuture<ErrorsAndData<EpochAndTxnTransitMetadata>> prepareInitProducerIdResult =
                            prepareInitProducerIdTransit(
                                transactionalId,
                                transactionTimeoutMs,
                                coordinatorEpoch,
                                txnMetadata,
                                expectedProducerIdAndEpoch);

                    prepareInitProducerIdResult.whenComplete((errorsAndData, prepareThrowable) -> {
                        if (errorsAndData.hasErrors()) {
                            initTransactionError(response, errorsAndData.getErrors());
                            return;
                        }
                        if (prepareThrowable != null) {
                            log.error("Failed to init producerId.", prepareThrowable);
                            initTransactionError(response, Errors.forException(prepareThrowable));
                            return;
                        }
                        TxnTransitMetadata newMetadata = errorsAndData.getData().txnTransitMetadata;
                        if (errorsAndData.getData().txnTransitMetadata.getTxnState() == PREPARE_EPOCH_FENCE) {
                            endTransaction(transactionalId,
                                    newMetadata.getProducerId(),
                                    newMetadata.getProducerEpoch(),
                                    TransactionResult.ABORT,
                                    false,
                                    requestHandler,
                                    errors -> {
                                        if (errors != Errors.NONE) {
                                            initTransactionError(response, errors);
                                        } else {
                                            initTransactionError(response, Errors.CONCURRENT_TRANSACTIONS);
                                        }
                                    });
                        } else {
                            txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, newMetadata,
                                    new TransactionStateManager.ResponseCallback() {
                                        @Override
                                        public void complete() {
                                            log.info("Initialized transactionalId {} with producerId {} and producer "
                                                    + "epoch {} on partition {}-{}", transactionalId,
                                                    newMetadata.getProducerId(), newMetadata.getProducerEpoch(),
                                                    Topic.TRANSACTION_STATE_TOPIC_NAME,
                                                    txnManager.partitionFor(transactionalId));
                                            response.complete(new InitProducerIdResponse(
                                                    0, Errors.NONE, newMetadata.getProducerId(),
                                                    newMetadata.getProducerEpoch()));
                                        }

                                        @Override
                                        public void fail(Errors errors) {
                                            log.info("Returning {} error code to client for {}'s InitProducerId "
                                                    + "request", errors, transactionalId);
                                            initTransactionError(response, errors);
                                        }
                                    }, errors -> true);
                                }
                    });
                    return null;
                });
            });

        }
    }

    private void initTransactionError(CompletableFuture<AbstractResponse> response, Errors errors) {
        response.complete(
                new InitProducerIdResponse(0, errors, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH));
    }

    @AllArgsConstructor
    private static class EpochAndTxnTransitMetadata {
        int coordinatorEpoch;
        TxnTransitMetadata txnTransitMetadata;
    }

    private boolean isValidProducerId(TransactionMetadata txnMetadata, ProducerIdAndEpoch producerIdAndEpoch) {
        // If a producer ID and epoch are provided by the request, fence the producer unless one of the following is true:
        //   1. The producer epoch is equal to -1, which implies that the metadata was just created. This is the case of a
        //      producer recovering from an UNKNOWN_PRODUCER_ID error, and it is safe to return the newly-generated
        //      producer ID.
        //   2. The expected producer ID matches the ID in current metadata (the epoch will be checked when we try to
        //      increment it)
        //   3. The expected producer ID matches the previous one and the expected epoch is exhausted, in which case this
        //      could be a retry after a valid epoch bump that the producer never received the response for
        return txnMetadata.getProducerEpoch() == RecordBatch.NO_PRODUCER_EPOCH
                || producerIdAndEpoch.producerId == txnMetadata.getProducerId()
                || (producerIdAndEpoch.producerId == txnMetadata.getLastProducerEpoch()
                && txnMetadata.isEpochExhausted(producerIdAndEpoch.epoch));
    }

    private CompletableFuture<ErrorsAndData<EpochAndTxnTransitMetadata>> prepareInitProducerIdTransit(
                                            String transactionalId,
                                            Integer transactionTimeoutMs,
                                            Integer coordinatorEpoch,
                                            TransactionMetadata txnMetadata,
                                            Optional<ProducerIdAndEpoch> expectedProducerIdAndEpoch) {

        CompletableFuture<ErrorsAndData<EpochAndTxnTransitMetadata>> resultFuture = new CompletableFuture<>();
        if (txnMetadata.getPendingState().isPresent()) {
            // return a retriable exception to let the client backoff and retry
            resultFuture.complete(new ErrorsAndData<>(Errors.CONCURRENT_TRANSACTIONS));
            return resultFuture;
        }

        if (expectedProducerIdAndEpoch.isPresent()
                && !isValidProducerId(txnMetadata, expectedProducerIdAndEpoch.get())) {
            // TODO the error should be Errors.PRODUCER_FENCED, needs upgrade kafka client version
            resultFuture.complete(new ErrorsAndData<>(producerEpochFenceErrors()));
            return resultFuture;
        } else {
            // caller should have synchronized on txnMetadata already
            switch (txnMetadata.getState()) {
                case PREPARE_ABORT:
                case PREPARE_COMMIT:
                    // reply to client and let it backoff and retry
                    resultFuture.complete(new ErrorsAndData<>(Errors.CONCURRENT_TRANSACTIONS));
                    break;
                case COMPLETE_ABORT:
                case COMPLETE_COMMIT:
                case EMPTY:
                    final CompletableFuture<TxnTransitMetadata> transitMetadata =
                            new CompletableFuture<>();
                    // If the epoch is exhausted and the expected epoch (if provided) matches it, generate a new producer ID
                    if (txnMetadata.isProducerEpochExhausted()) {
                        CompletableFuture<Long> newProducerId = producerIdManager.generateProducerId();
                        newProducerId.thenAccept(newPid -> {
                            transitMetadata.complete(
                                    txnMetadata.prepareProducerIdRotation(
                                            newPid,
                                            transactionTimeoutMs,
                                            SystemTime.SYSTEM.milliseconds(),
                                            expectedProducerIdAndEpoch.isPresent()));
                        });
                    } else {
                        transitMetadata.complete(
                                txnMetadata.prepareIncrementProducerEpoch(
                                        transactionTimeoutMs,
                                        expectedProducerIdAndEpoch.map(ProducerIdAndEpoch::getEpoch),
                                        SystemTime.SYSTEM.milliseconds()).getData());
                    }
                    transitMetadata.whenComplete((txnTransitMetadata, throwable) -> {
                        resultFuture.complete(new ErrorsAndData<>(
                                new EpochAndTxnTransitMetadata(coordinatorEpoch, txnTransitMetadata)));
                    });
                    break;
                case ONGOING:
                    // indicate to abort the current ongoing txn first. Note that this epoch is never returned to the
                    // user. We will abort the ongoing transaction and return CONCURRENT_TRANSACTIONS to the client.
                    // This forces the client to retry, which will ensure that the epoch is bumped a second time. In
                    // particular, if fencing the current producer exhausts the available epochs for the current producerId,
                    // then when the client retries, we will generate a new producerId.
                    resultFuture.complete(new ErrorsAndData<>(
                            new EpochAndTxnTransitMetadata(coordinatorEpoch, txnMetadata.prepareFenceProducerEpoch())));
                    break;
                case DEAD:
                case PREPARE_EPOCH_FENCE:
                    String errorMsg = String.format("Found transactionalId %s with state %s. "
                                    + "This is illegal as we should never have transitioned to this state.",
                            transactionalId, txnMetadata.getState());
                    resultFuture.completeExceptionally(new IllegalStateException(errorMsg));
            }
        }
        return resultFuture;
    }

    public void handleAddPartitionsToTransaction(String transactionalId,
                                                 long producerId,
                                                 short producerEpoch,
                                                 List<TopicPartition> partitionList,
                                                 CompletableFuture<AbstractResponse> response) {
        if (transactionalId == null || transactionalId.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Returning {} error code to client for {}'s AddPartitions request",
                        Errors.INVALID_REQUEST, transactionalId);
            }
            response.complete(
                    new AddPartitionsToTxnResponse(0, addPartitionError(partitionList, Errors.INVALID_REQUEST)));
            return;
        }

        // try to update the transaction metadata and append the updated metadata to txn log;
        // if there is no such metadata treat it as invalid producerId mapping error.
        ErrorsAndData<Optional<CoordinatorEpochAndTxnMetadata>> metadata =
                txnManager.getTransactionState(transactionalId);
        ErrorsAndData<EpochAndTxnTransitMetadata> result = new ErrorsAndData<>();
        if (!metadata.getData().isPresent()) {
            response.complete(
                    new AddPartitionsToTxnResponse(0,
                            addPartitionError(partitionList, Errors.INVALID_PRODUCER_ID_MAPPING)));
        } else {
            CoordinatorEpochAndTxnMetadata epochAndTxnMetadata = metadata.getData().get();
            int coordinatorEpoch = epochAndTxnMetadata.getCoordinatorEpoch();
            TransactionMetadata txnMetadata = epochAndTxnMetadata.getTransactionMetadata();

            txnMetadata.inLock(() -> {
                if (txnMetadata.getProducerId() != producerId) {
                    result.setErrors(Errors.INVALID_PRODUCER_ID_MAPPING);
                } else if (txnMetadata.getProducerEpoch() != producerEpoch) {
                    // TODO the error should be Errors.PRODUCER_FENCED, needs upgrade kafka client version
                    result.setErrors(producerEpochFenceErrors());
                } else if (txnMetadata.getPendingState().isPresent()) {
                    // return a retriable exception to let the client backoff and retry
                    result.setErrors(Errors.CONCURRENT_TRANSACTIONS);
                } else if (txnMetadata.getState() == PREPARE_COMMIT || txnMetadata.getState() == PREPARE_ABORT) {
                    result.setErrors(Errors.CONCURRENT_TRANSACTIONS);
                } else if (txnMetadata.getState() == ONGOING
                        && txnMetadata.getTopicPartitions().containsAll(partitionList)) {
                    // this is an optimization: if the partitions are already in the metadata reply OK immediately
                    result.setErrors(Errors.NONE);
                } else {
                    result.setData(new EpochAndTxnTransitMetadata(
                            coordinatorEpoch, txnMetadata.prepareAddPartitions(
                                    new HashSet<>(partitionList), SystemTime.SYSTEM.milliseconds())));
                }
                return null;
            });
        }

        if (result.getErrors() != null) {
            response.complete(new AddPartitionsToTxnResponse(0, addPartitionError(partitionList, result.getErrors())));
        } else {
            txnManager.appendTransactionToLog(
                    transactionalId, result.getData().coordinatorEpoch, result.getData().txnTransitMetadata,
                    new TransactionStateManager.ResponseCallback() {
                        @Override
                        public void complete() {
                            response.complete(
                                    new AddPartitionsToTxnResponse(0, addPartitionError(partitionList, Errors.NONE)));
                        }

                        @Override
                        public void fail(Errors e) {
                            response.complete(new AddPartitionsToTxnResponse(0, addPartitionError(partitionList, e)));
                        }
                    }, errors -> true);

        }
    }

    private Map<TopicPartition, Errors> addPartitionError(List<TopicPartition> partitionList, Errors errors) {
        Map<TopicPartition, Errors> data = new HashMap<>();
        for (TopicPartition topicPartition : partitionList) {
            data.put(topicPartition, errors);
        }
        return data;
    }

    private Errors producerEpochFenceErrors() {
        return Errors.forException(new Throwable("There is a newer producer with the same transactionalId "
                + "which fences the current one."));
    }

    public void handleEndTransaction(String transactionalId,
                                     long producerId,
                                     short producerEpoch,
                                     TransactionResult transactionResult,
                                     KafkaRequestHandler requestHandler,
                                     CompletableFuture<AbstractResponse> response) {
        endTransaction(transactionalId, producerId, producerEpoch, transactionResult, true,
                requestHandler, errors -> response.complete(new EndTxnResponse(0, errors)));
//        ErrorsAndData<Optional<CoordinatorEpochAndTxnMetadata>> errorsAndData =
//                txnManager.getTransactionState(transactionalId);
//        TransactionMetadata metadata = errorsAndData.getData().get().getTransactionMetadata();
//        switch (metadata.getState()) {
//            case ONGOING:
//                TransactionState nextState;
//                if (transactionResult == TransactionResult.COMMIT) {
//                    nextState = PREPARE_COMMIT;
//                } else {
//                    nextState = TransactionState.PREPARE_ABORT;
//                }
//
//                if (nextState == TransactionState.PREPARE_ABORT && metadata.getPendingState().isPresent()
//                        && metadata.getPendingState().get().equals(TransactionState.PREPARE_EPOCH_FENCE)) {
//                    // We should clear the pending state to make way for the transition to PrepareAbort and also bump
//                    // the epoch in the transaction metadata we are about to append.
//                    metadata.setPendingState(Optional.empty());
//                    metadata.setProducerEpoch(producerEpoch);
//                }
//
//                TxnTransitMetadata newMetadata =
//                        metadata.prepareAbortOrCommit(nextState, SystemTime.SYSTEM.milliseconds());
//                txnManager.appendTransactionToLog(transactionalId, 0, newMetadata,
//                        new TransactionStateManager.ResponseCallback() {
//                            @Override
//                            public void complete() {
//
//                            }
//
//                            @Override
//                            public void fail(Errors errors) {
//
//                            }
//                        }, errors -> true);
//                break;
//            case COMPLETE_COMMIT:
//                break;
//            case COMPLETE_ABORT:
//                break;
//            case PREPARE_COMMIT:
//                break;
//            case PREPARE_ABORT:
//                break;
//            case EMPTY:
//                break;
//            case DEAD:
//            case PREPARE_EPOCH_FENCE:
//                break;
//        }
//
//        final Map<InetSocketAddress, MarkerHandler> markerHandlerMap = new HashMap<>();
//        final List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
//        for (TopicPartition topicPartition : metadata.getTopicPartitions()) {
//            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
//            completableFutureList.add(completableFuture);
//            String pulsarTopic = new KopTopic(topicPartition.topic()).getPartitionName(topicPartition.partition());
//            requestHandler.findBroker(TopicName.get(pulsarTopic))
//                    .thenAccept(partitionMetadata -> {
//                        InetSocketAddress socketAddress = new InetSocketAddress(
//                                partitionMetadata.leader().host(), partitionMetadata.leader().port());
//                        CompletableFuture<TransactionMarkerChannelHandler> handlerFuture =
//                                transactionMarkerChannelManager.getChannel(socketAddress);
//                        markerHandlerMap.compute(socketAddress, (key, value) -> {
//                            if (value == null) {
//                                List<TopicPartition> topicPartitionList = new ArrayList<>();
//                                topicPartitionList.add(topicPartition);
//                                return MarkerHandler.builder()
//                                        .topicPartitionList(topicPartitionList)
//                                        .handlerFuture(handlerFuture)
//                                        .build();
//                            } else {
//                                value.topicPartitionList.add(topicPartition);
//                                return value;
//                            }
//                        });
//                        completableFuture.complete(null);
//                    }).exceptionally(e -> {
//                        log.error("EndTxn findBroker fail", e);
//                        completableFuture.completeExceptionally(e);
//                        return null;
//            });
//        }
//
//        FutureUtil.waitForAll(completableFutureList).thenRun(() -> {
//            List<CompletableFuture<WriteTxnMarkersResponse>> writeTxnMarkersFutureList = new ArrayList<>();
//            for (MarkerHandler markerHandler : markerHandlerMap.values()) {
//                writeTxnMarkersFutureList.add(
//                        markerHandler.writeTxnMarker(producerId, producerEpoch, transactionResult));
//            }
//
//            FutureUtil.waitForAll(writeTxnMarkersFutureList).whenComplete((ignored, throwable) -> {
//                if (throwable != null) {
//                    response.complete(new EndTxnResponse(0, Errors.COORDINATOR_NOT_AVAILABLE));
//                    return;
//                }
//                TxnTransitMetadata newMetadata =
//                        metadata.prepareComplete(SystemTime.SYSTEM.milliseconds());
//                txnManager.appendTransactionToLog(transactionalId, 0, newMetadata,
//                        new TransactionStateManager.ResponseCallback() {
//                            @Override
//                            public void complete() {
//                                response.complete(new EndTxnResponse(0, Errors.NONE));
//                            }
//
//                            @Override
//                            public void fail(Errors errors) {
//
//                            }
//                        }, errors -> true);
//            });
//        }).exceptionally(e -> {
//            response.complete(new EndTxnResponse(0, Errors.COORDINATOR_NOT_AVAILABLE));
//            return null;
//        });
    }

    @AllArgsConstructor
    @Data
    private static class PreSendResult {
        TransactionMetadata transactionMetadata;
        TxnTransitMetadata txnTransitMetadata;
    }

    private void endTransaction(String transactionalId,
                                Long producerId,
                                Short producerEpoch,
                                TransactionResult txnMarkerResult,
                                Boolean isFromClient,
                                KafkaRequestHandler requestHandler,
                                EndTxnCallback callback) {
        AtomicBoolean isEpochFence = new AtomicBoolean(false);
        if (transactionalId == null || transactionalId.isEmpty()) {
            callback.complete(Errors.INVALID_REQUEST);
            return;
        }

        Optional<CoordinatorEpochAndTxnMetadata> epochAndMetadata =
                txnManager.getTransactionState(transactionalId).getData();

        if (!epochAndMetadata.isPresent()) {
            callback.complete(Errors.INVALID_PRODUCER_ID_MAPPING);
            return;
        }

        TransactionMetadata txnMetadata = epochAndMetadata.get().getTransactionMetadata();
        int coordinatorEpoch = epochAndMetadata.get().getCoordinatorEpoch();

        ErrorsAndData<TxnTransitMetadata> preAppendResult = new ErrorsAndData<>();
        txnMetadata.inLock(() -> {
            if (txnMetadata.getProducerId() != producerId) {
                preAppendResult.setErrors(Errors.INVALID_PRODUCER_ID_MAPPING);
                return null;
            }
            if (isFromClient && producerEpoch != txnMetadata.getProducerEpoch()
                    || producerEpoch < txnMetadata.getProducerEpoch()) {
                // TODO the error should be Errors.PRODUCER, needs upgrade kafka client version
                preAppendResult.setErrors(producerEpochFenceErrors());
                return null;
            }
            if (txnMetadata.getPendingState().isPresent()
                    && txnMetadata.getPendingState().get() != PREPARE_EPOCH_FENCE) {
                preAppendResult.setErrors(Errors.CONCURRENT_TRANSACTIONS);
                return null;
            }

            switch(txnMetadata.getState()) {
                case ONGOING:
                    TransactionState nextState;
                    if (txnMarkerResult == TransactionResult.COMMIT) {
                        nextState = PREPARE_COMMIT;
                    } else {
                        nextState = TransactionState.PREPARE_ABORT;
                    }

                    if (nextState == TransactionState.PREPARE_ABORT && txnMetadata.getPendingState().isPresent()
                            && txnMetadata.getPendingState().get().equals(TransactionState.PREPARE_EPOCH_FENCE)) {
                        // We should clear the pending state to make way for the transition to PrepareAbort and also bump
                        // the epoch in the transaction metadata we are about to append.
                        isEpochFence.set(true);
                        txnMetadata.setPendingState(Optional.empty());
                        txnMetadata.setProducerEpoch(producerEpoch);
                        txnMetadata.setLastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH);
                    }

                    preAppendResult.setData(
                            txnMetadata.prepareAbortOrCommit(nextState, SystemTime.SYSTEM.milliseconds()));
                    break;
                case COMPLETE_COMMIT:
                    if (txnMarkerResult == TransactionResult.COMMIT) {
                        preAppendResult.setErrors(Errors.NONE);
                    } else {
                        preAppendResult.setErrors(logInvalidStateTransitionAndReturnError(
                                transactionalId, txnMetadata.getState(), txnMarkerResult));
                    }
                    break;
                case COMPLETE_ABORT:
                    if (txnMarkerResult == TransactionResult.ABORT) {
                        preAppendResult.setErrors(Errors.NONE);
                    } else {
                        preAppendResult.setErrors(logInvalidStateTransitionAndReturnError(
                                transactionalId, txnMetadata.getState(), txnMarkerResult));
                    }
                    break;
                case PREPARE_COMMIT:
                    if (txnMarkerResult == TransactionResult.COMMIT) {
                        preAppendResult.setErrors(Errors.CONCURRENT_TRANSACTIONS);
                    } else {
                        preAppendResult.setErrors(logInvalidStateTransitionAndReturnError(
                                transactionalId, txnMetadata.getState(), txnMarkerResult));
                    }
                    break;
                case PREPARE_ABORT:
                    if (txnMarkerResult == TransactionResult.ABORT) {
                        preAppendResult.setErrors(Errors.CONCURRENT_TRANSACTIONS);
                    } else {
                        preAppendResult.setErrors(logInvalidStateTransitionAndReturnError(
                                transactionalId, txnMetadata.getState(), txnMarkerResult));
                    }
                    break;
                case EMPTY:
                    preAppendResult.setErrors(logInvalidStateTransitionAndReturnError(
                            transactionalId, txnMetadata.getState(), txnMarkerResult));
                    break;
                case DEAD:
                case PREPARE_EPOCH_FENCE:
                    String errorMsg = String.format("Found transactionalId %s with state %s. "
                            + "This is illegal as we should never have transitioned to this state.",
                            transactionalId, txnMetadata.getState());
                    log.error(errorMsg);
                    throw new IllegalStateException(errorMsg);
            }
            return null;
        });

        if (preAppendResult.hasErrors()) {
            log.error("Aborting append of {} to transaction log with coordinator and returning {} error to client "
                    + "for {}'s EndTransaction request", txnMarkerResult, preAppendResult.getErrors(), transactionalId);
            callback.complete(preAppendResult.getErrors());
            return;
        }

        txnManager.appendTransactionToLog(transactionalId, coordinatorEpoch, preAppendResult.getData(), 
                new TransactionStateManager.ResponseCallback() {
                    @Override
                    public void complete() {
                        ErrorsAndData<Optional<CoordinatorEpochAndTxnMetadata>> errorsAndData =
                                txnManager.getTransactionState(transactionalId);

                        if(!errorsAndData.getData().isPresent()) {
                            String errorMsg = String.format("The coordinator still owns the transaction partition for "
                                    + "%s, but there is no metadata in the cache; this is not expected",
                                    transactionalId);
                            log.error(errorMsg);
                            throw new IllegalStateException(errorMsg);
                        }

                        ErrorsAndData<PreSendResult> preSendResult = new ErrorsAndData<>();
                        CoordinatorEpochAndTxnMetadata epochAndTxnMetadata = errorsAndData.getData().get();
                        if (epochAndTxnMetadata.getCoordinatorEpoch() == coordinatorEpoch) {
                            TransactionMetadata txnMetadata = epochAndTxnMetadata.getTransactionMetadata();
                            txnMetadata.inLock(() -> {
                                if (txnMetadata.getProducerId() != producerId) {
                                    preSendResult.setErrors(Errors.INVALID_PRODUCER_ID_MAPPING);
                                } else if (txnMetadata.getProducerEpoch() != producerEpoch) {
                                    preSendResult.setErrors(producerEpochFenceErrors());
                                } else if (txnMetadata.getPendingState().isPresent()) {
                                    preSendResult.setErrors(Errors.CONCURRENT_TRANSACTIONS);
                                } else {
                                    switch (txnMetadata.getState()) {
                                        case EMPTY:
                                        case ONGOING:
                                        case COMPLETE_ABORT:
                                        case COMPLETE_COMMIT:
                                            preSendResult.setErrors(logInvalidStateTransitionAndReturnError(
                                                    transactionalId, txnMetadata.getState(), txnMarkerResult));
                                            break;
                                        case PREPARE_COMMIT:
                                            if (txnMarkerResult != TransactionResult.COMMIT) {
                                                preSendResult.setErrors(logInvalidStateTransitionAndReturnError(
                                                        transactionalId, txnMetadata.getState(), txnMarkerResult));
                                            } else {
                                                TxnTransitMetadata txnTransitMetadata =
                                                        txnMetadata.prepareComplete(SystemTime.SYSTEM.milliseconds());
                                                preSendResult.setData(
                                                        new PreSendResult(txnMetadata, txnTransitMetadata));
                                            }
                                            break;
                                        case PREPARE_ABORT:
                                            if (txnMarkerResult != TransactionResult.ABORT) {
                                                preSendResult.setErrors(logInvalidStateTransitionAndReturnError(
                                                        transactionalId, txnMetadata.getState(), txnMarkerResult));

                                            } else {
                                                TxnTransitMetadata txnTransitMetadata =
                                                        txnMetadata.prepareComplete(SystemTime.SYSTEM.milliseconds());
                                                preSendResult.setData(
                                                        new PreSendResult(txnMetadata, txnTransitMetadata));
                                            }
                                            break;
                                        case DEAD:
                                        case PREPARE_EPOCH_FENCE:
                                            String errorMsg = String.format("Found transactionalId %s with state %s. "
                                                    + "This is illegal as we should never have transitioned to "
                                                    + "this state.", transactionalId, txnMetadata.getState());
                                            log.error(errorMsg);
                                            throw new IllegalStateException(errorMsg);
                                    }
                                }
                                return null;
                            });
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("The transaction coordinator epoch has changed to {} after {} was "
                                        + "successfully appended to the log for {} with old epoch {}",
                                        epochAndTxnMetadata.getCoordinatorEpoch(), txnMarkerResult, transactionalId,
                                        coordinatorEpoch);
                            }
                            preSendResult.setErrors(Errors.NOT_COORDINATOR);
                        }

                        if (preSendResult.hasErrors()) {
                            log.info("Aborting sending of transaction markers after appended {} to transaction log "
                                    + "and returning {} error to client for $transactionalId's EndTransaction request",
                                    txnMarkerResult, preSendResult.getErrors());
                            callback.complete(preSendResult.getErrors());
                            return;
                        }

                        sendTxnResultMarker(epochAndTxnMetadata.getTransactionMetadata(),
                                preSendResult.getData().getTxnTransitMetadata(),
                                txnMarkerResult, requestHandler, callback);
                    }
        
                    @Override
                    public void fail(Errors errors) {
                        log.info("Aborting sending of transaction markers and returning {} error to client for {}'s " 
                                + "EndTransaction request of {}, since appending {} to transaction log with " 
                                + "coordinator epoch {} failed", errors, transactionalId, txnMarkerResult, 
                                preAppendResult.getData(), coordinatorEpoch);
        
                        if (isEpochFence.get()) {
                            ErrorsAndData<Optional<CoordinatorEpochAndTxnMetadata>> 
                                    errorsAndData = txnManager.getTransactionState(transactionalId);
                            if (!errorsAndData.getData().isPresent()) {
                                log.warn("The coordinator still owns the transaction partition for {}, but there " 
                                        + "is no metadata in the cache; this is not expected", transactionalId);
                            } else if (errorsAndData.getData().isPresent()) {
                                if (epochAndMetadata.get().getCoordinatorEpoch() == coordinatorEpoch) {
                                    // This was attempted epoch fence that failed, so mark this state on the metadata
                                    epochAndMetadata.get().getTransactionMetadata().setHasFailedEpochFence(true);
                                    log.warn("The coordinator failed to write an epoch fence transition for producer " 
                                            + "{} to the transaction log with error {}. The epoch was increased to ${} " 
                                            + "but not returned to the client", transactionalId, errors,
                                            preAppendResult.getData().getProducerEpoch());
                                }
        
                            }
                        }
        
                        callback.complete(errors);
                    }
                }, errors1 -> true);
    }

    private void sendTxnResultMarker(TransactionMetadata metadata,
                                     TxnTransitMetadata newMetadata,
                                     TransactionResult transactionResult,
                                     KafkaRequestHandler requestHandler,
                                     EndTxnCallback callback) {
        final Map<InetSocketAddress, MarkerHandler> markerHandlerMap = new HashMap<>();
        final List<CompletableFuture<Void>> completableFutureList = new ArrayList<>();
        for (TopicPartition topicPartition : metadata.getTopicPartitions()) {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            completableFutureList.add(completableFuture);
            String pulsarTopic = new KopTopic(topicPartition.topic()).getPartitionName(topicPartition.partition());
            requestHandler.findBroker(TopicName.get(pulsarTopic))
                    .thenAccept(partitionMetadata -> {
                        InetSocketAddress socketAddress = new InetSocketAddress(
                                partitionMetadata.leader().host(), partitionMetadata.leader().port());
                        CompletableFuture<TransactionMarkerChannelHandler> handlerFuture =
                                transactionMarkerChannelManager.getChannel(socketAddress);
                        markerHandlerMap.compute(socketAddress, (key, value) -> {
                            if (value == null) {
                                List<TopicPartition> topicPartitionList = new ArrayList<>();
                                topicPartitionList.add(topicPartition);
                                return MarkerHandler.builder()
                                        .topicPartitionList(topicPartitionList)
                                        .handlerFuture(handlerFuture)
                                        .build();
                            } else {
                                value.topicPartitionList.add(topicPartition);
                                return value;
                            }
                        });
                        completableFuture.complete(null);
                    }).exceptionally(e -> {
                log.error("EndTxn findBroker fail", e);
                completableFuture.completeExceptionally(e);
                return null;
            });
        }

        FutureUtil.waitForAll(completableFutureList).thenRun(() -> {
            List<CompletableFuture<WriteTxnMarkersResponse>> writeTxnMarkersFutureList = new ArrayList<>();
            for (MarkerHandler markerHandler : markerHandlerMap.values()) {
                writeTxnMarkersFutureList.add(
                        markerHandler.writeTxnMarker(newMetadata.getProducerId(), newMetadata.getProducerEpoch(), transactionResult));
            }

            FutureUtil.waitForAll(writeTxnMarkersFutureList).whenComplete((ignored, throwable) -> {
                if (throwable != null) {
                    callback.complete(Errors.COORDINATOR_NOT_AVAILABLE);
                    return;
                }
                metadata.completeTransitionTo(newMetadata);
                callback.complete(Errors.NONE);
            });
        }).exceptionally(e -> {
            callback.complete(Errors.COORDINATOR_NOT_AVAILABLE);
            return null;
        });
    }
    
    private Errors logInvalidStateTransitionAndReturnError(String transactionalId,
                                                         TransactionState transactionState,
                                                         TransactionResult transactionResult) {
        log.debug("TransactionalId: {}'s state is {}, but received transaction marker result to send: {}",
                transactionalId, transactionState, transactionResult);
        return Errors.INVALID_TXN_STATE;
    }

    public void addActivePidOffset(TopicName topicName, long pid, long offset) {
        ConcurrentHashMap<Long, Long> pidOffsetMap =
                activePidOffsetMap.computeIfAbsent(topicName, topicKey -> new ConcurrentHashMap<>());

        NavigableMap<Long, Long> offsetPidMap =
                activeOffsetPidMap.computeIfAbsent(topicName, topicKey -> new ConcurrentSkipListMap<>());

        pidOffsetMap.computeIfAbsent(pid, pidKey -> {
            offsetPidMap.computeIfAbsent(offset, offsetKey -> {
                return pid;
            });
            return offset;
        });
    }

    public long removeActivePidOffset(TopicName topicName, long pid) {
        ConcurrentHashMap<Long, Long> pidOffsetMap = activePidOffsetMap.getOrDefault(topicName, null);
        if (pidOffsetMap == null) {
            return -1;
        }

        NavigableMap<Long, Long> offsetPidMap = activeOffsetPidMap.getOrDefault(topicName, null);
        if (offsetPidMap == null) {
            log.warn("[removeActivePidOffset] offsetPidMap is null");
            return -1;
        }

        Long offset = pidOffsetMap.remove(pid);
        if (offset == null) {
            log.warn("[removeActivePidOffset] pidOffsetMap is not contains pid {}.", pid);
            return -1;
        }

        if (offsetPidMap.containsKey(offset)) {
            offsetPidMap.remove(offset);
        }
        return offset;
    }

    public long getLastStableOffset(TopicName topicName, long highWaterMark) {
        NavigableMap<Long, Long> map = activeOffsetPidMap.getOrDefault(topicName, null);
        if (map == null || map.isEmpty()) {
            return highWaterMark;
        }
        return map.firstKey();
    }

    public void addAbortedIndex(AbortedIndexEntry abortedIndexEntry) {
        abortedIndexList.add(abortedIndexEntry);
    }

    public List<FetchResponse.AbortedTransaction> getAbortedIndexList(long fetchOffset) {
        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>(abortedIndexList.size());
        for (AbortedIndexEntry abortedIndexEntry : abortedIndexList) {
            if (abortedIndexEntry.getLastOffset() >= fetchOffset) {
                abortedTransactions.add(
                        new FetchResponse.AbortedTransaction(
                                abortedIndexEntry.getPid(),
                                abortedIndexEntry.getFirstOffset()));
            }
        }
        return abortedTransactions;
    }

    @Builder
    @Data
    private static class MarkerHandler {

        private CompletableFuture<TransactionMarkerChannelHandler> handlerFuture;
        private List<TopicPartition> topicPartitionList;

        public CompletableFuture<WriteTxnMarkersResponse> writeTxnMarker(long producerId,
                                                                         short producerEpoch,
                                                                         TransactionResult transactionResult) {
            WriteTxnMarkersRequest.TxnMarkerEntry txnMarkerEntry = new WriteTxnMarkersRequest.TxnMarkerEntry(
                    producerId,
                    producerEpoch,
                    1,
                    transactionResult,
                    topicPartitionList);
            WriteTxnMarkersRequest txnMarkersRequest =
                    new WriteTxnMarkersRequest.Builder(Lists.newArrayList(txnMarkerEntry)).build();
            return handlerFuture.thenCompose(handler -> handler.enqueueRequest(txnMarkersRequest));
        }
    }

}
