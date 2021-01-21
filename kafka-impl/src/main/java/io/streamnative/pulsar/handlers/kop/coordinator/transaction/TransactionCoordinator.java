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

import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.kop.KafkaRequestHandler;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
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

/**
 * Transaction coordinator.
 */
@Slf4j
public class TransactionCoordinator {

    private final TransactionConfig transactionConfig;
    private final ProducerIdManager producerIdManager;
    private final TransactionStateManager txnStateManager;
    private final TransactionMarkerChannelManager transactionMarkerChannelManager;

    // map from topic to the map from initial offset to producerId
    private final Map<TopicName, NavigableMap<Long, Long>> activeOffsetPidMap = new HashMap<>();
    private final Map<TopicName, ConcurrentHashMap<Long, Long>> activePidOffsetMap = new HashMap<>();
    private final List<AbortedIndexEntry> abortedIndexList = new ArrayList<>();

    private TransactionCoordinator(TransactionConfig transactionConfig) {
        this.transactionConfig = transactionConfig;
        this.txnStateManager = new TransactionStateManager(transactionConfig);
        this.producerIdManager = new ProducerIdManager();
        this.transactionMarkerChannelManager = new TransactionMarkerChannelManager(null, null, false);
    }

    public static TransactionCoordinator of(TransactionConfig transactionConfig) {
        return new TransactionCoordinator(transactionConfig);
    }

    public int partitionFor(String transactionalId) {
        return MathUtils.signSafeMod(
                transactionalId.hashCode(),
                transactionConfig.getTransactionMetadataTopicPartition()
        );
    }

    public String getTopicPartitionName() {
        return transactionConfig.getTransactionMetadataTopicName();
    }

    public String getTopicPartitionName(int partitionId) {
        return getTopicPartitionName() + PARTITIONED_TOPIC_SUFFIX + partitionId;
    }

    public void handleInitProducerId(String transactionalId, int transactionTimeoutMs,
                                     CompletableFuture<AbstractResponse> response) {
        if (transactionalId == null) {
            // if the transactional id is null, then always blindly accept the request
            // and return a new producerId from the producerId manager
            long producerId = producerIdManager.generateProducerId();
            short producerEpoch = 0;
            response.complete(new InitProducerIdResponse(0, Errors.NONE, producerId, producerEpoch));
        } else if (StringUtils.isEmpty(transactionalId)) {
            // if transactional id is empty then return error as invalid request. This is
            // to make TransactionCoordinator's behavior consistent with producer client
            response.complete(new InitProducerIdResponse(0, Errors.INVALID_REQUEST));
        } else if (!txnStateManager.validateTransactionTimeoutMs(transactionTimeoutMs)){
            // check transactionTimeoutMs is not larger than the broker configured maximum allowed value
            response.complete(new InitProducerIdResponse(0, Errors.INVALID_TRANSACTION_TIMEOUT));
        } else {
            TransactionMetadata metadata = txnStateManager.getTransactionState(transactionalId);
            if (metadata == null) {
                long producerId = producerIdManager.generateProducerId();
                short producerEpoch = 0;
                metadata = TransactionMetadata.builder()
                        .transactionalId(transactionalId)
                        .producerId(producerId)
                        .producerEpoch(producerEpoch)
                        .state(TransactionState.EMPTY)
                        .topicPartitions(new HashSet<>())
                        .build();
                txnStateManager.putTransactionStateIfNotExists(transactionalId, metadata);
                response.complete(new InitProducerIdResponse(
                        0, Errors.NONE, metadata.getProducerId(), metadata.getProducerEpoch()));
            } else {
                // TODO generate monotonically increasing epoch
                // TODO conflict resolve
                response.complete(new InitProducerIdResponse(
                        0, Errors.NONE, metadata.getProducerId(), metadata.getProducerEpoch()));
            }
        }
    }

    public void handleAddPartitionsToTransaction(String transactionalId,
                                                 long producerId,
                                                 short producerEpoch,
                                                 List<TopicPartition> partitionList,
                                                 CompletableFuture<AbstractResponse> response) {
        TransactionMetadata metadata = txnStateManager.getTransactionState(transactionalId);
        TransactionMetadata.TxnTransitMetadata newMetadata =
                metadata.prepareAddPartitions(new HashSet<>(partitionList), SystemTime.SYSTEM.milliseconds());
        txnStateManager.appendTransactionToLog(
                transactionalId, 0, newMetadata,
                new TransactionStateManager.ResponseCallback() {
                    @Override
                    public void complete() {
                        Map<TopicPartition, Errors> data = new HashMap<>();
                        for (TopicPartition topicPartition : newMetadata.getTopicPartitions()) {
                            data.put(topicPartition, Errors.NONE);
                        }
                        response.complete(new AddPartitionsToTxnResponse(0, data));
                    }

                    @Override
                    public void fail(Exception e) {

                    }
                });
    }

    public void handleEndTransaction(String transactionalId,
                                     long producerId,
                                     short producerEpoch,
                                     TransactionResult transactionResult,
                                     KafkaRequestHandler requestHandler,
                                     CompletableFuture<AbstractResponse> response) {

        TransactionMetadata metadata = txnStateManager.getTransactionState(transactionalId);
        switch (metadata.getState()) {
            case ONGOING:
                TransactionState nextState;
                if (transactionResult == TransactionResult.COMMIT) {
                    nextState = TransactionState.PREPARE_COMMIT;
                } else {
                    nextState = TransactionState.PREPARE_ABORT;
                }

                if (nextState == TransactionState.PREPARE_ABORT && metadata.getPendingState() != null
                        && metadata.getPendingState().equals(TransactionState.PREPARE_EPOCH_FENCE)) {
                    // We should clear the pending state to make way for the transition to PrepareAbort and also bump
                    // the epoch in the transaction metadata we are about to append.
                    metadata.setPendingState(null);
                    metadata.setProducerEpoch(producerEpoch);
                }

                TransactionMetadata.TxnTransitMetadata newMetadata =
                        metadata.prepareAbortOrCommit(nextState, SystemTime.SYSTEM.milliseconds());
                txnStateManager.appendTransactionToLog(transactionalId, 0, newMetadata,
                        new TransactionStateManager.ResponseCallback() {
                            @Override
                            public void complete() {

                            }

                            @Override
                            public void fail(Exception e) {

                            }
                        });
                break;
            case COMPLETE_COMMIT:
                break;
            case COMPLETE_ABORT:
                break;
            case PREPARE_COMMIT:
                break;
            case PREPARE_ABORT:
                break;
            case EMPTY:
                break;
            case DEAD:
            case PREPARE_EPOCH_FENCE:
                break;
        }

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
                        markerHandler.writeTxnMarker(producerId, producerEpoch, transactionResult));
            }

            FutureUtil.waitForAll(writeTxnMarkersFutureList).whenComplete((ignored, throwable) -> {
                if (throwable != null) {
                    response.complete(new EndTxnResponse(0, Errors.COORDINATOR_NOT_AVAILABLE));
                    return;
                }
                TransactionMetadata.TxnTransitMetadata newMetadata =
                        metadata.prepareComplete(SystemTime.SYSTEM.milliseconds());
                txnStateManager.appendTransactionToLog(transactionalId, 0, newMetadata,
                        new TransactionStateManager.ResponseCallback() {
                            @Override
                            public void complete() {
                                response.complete(new EndTxnResponse(0, Errors.NONE));
                            }

                            @Override
                            public void fail(Exception e) {

                            }
                        });
            });
        }).exceptionally(e -> {
            response.complete(new EndTxnResponse(0, Errors.COORDINATOR_NOT_AVAILABLE));
            return null;
        });
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

    public long getLastStableOffset(TopicName topicName) {
        NavigableMap<Long, Long> map = activeOffsetPidMap.getOrDefault(topicName, null);
        if (map == null) {
            log.warn("[activeOffsetPidMap] size: {} the topic {} map is null last",
                    activeOffsetPidMap.size(), topicName);
            return Long.MAX_VALUE;
        }
        if (map.size() == 0) {
            return Long.MAX_VALUE;
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
