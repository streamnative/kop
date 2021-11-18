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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.streamnative.pulsar.handlers.kop.EndPoint;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.KopBrokerLookupManager;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.ssl.SSLUtils;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.ChannelFutures;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.ssl.SslContextFactory;


/**
 * Transaction marker channel manager.
 */
@Slf4j
public class TransactionMarkerChannelManager {

    private final KafkaServiceConfiguration kafkaConfig;
    private final EventLoopGroup eventLoopGroup;
    private final boolean enableTls;
    private final SslContextFactory sslContextFactory;
    private final EndPoint sslEndPoint;
    private final KopBrokerLookupManager kopBrokerLookupManager;

    private final Bootstrap bootstrap;

    private Map<InetSocketAddress, CompletableFuture<TransactionMarkerChannelHandler>> handlerMap = new HashMap<>();

    private TransactionStateManager txnStateManager;
    private ConcurrentHashMap<String, PendingCompleteTxn> transactionsWithPendingMarkers = new ConcurrentHashMap<>();
    private Map<InetSocketAddress, TxnMarkerQueue> markersQueuePerBroker = new ConcurrentHashMap<>();
    private TxnMarkerQueue markersQueueForUnknownBroker = new TxnMarkerQueue(null);
    private BlockingQueue<PendingCompleteTxn> txnLogAppendRetryQueue = new LinkedBlockingQueue<>();
    private volatile boolean closed;

    @AllArgsConstructor
    private static class PendingCompleteTxn {
        private final String transactionalId;
        private final Integer coordinatorEpoch;
        private final TransactionMetadata txnMetadata;
        private final TransactionMetadata.TxnTransitMetadata newMetadata;
    }

    /**
     * Txn id and TxnMarkerEntry.
     */
    @Data
    @AllArgsConstructor
    protected static class TxnIdAndMarkerEntry {
        private final String transactionalId;
        private final TxnMarkerEntry entry;
    }

    private static class TxnMarkerQueue {

        private final InetSocketAddress address;

        // keep track of the requests per txn topic partition so we can easily clear the queue
        // during partition emigration
        private final Map<Integer, BlockingQueue<TxnIdAndMarkerEntry>> markersPerPartition = new ConcurrentHashMap<>();

        public TxnMarkerQueue(InetSocketAddress address) {
            this.address = address;
        }

        public BlockingQueue<TxnIdAndMarkerEntry> removeMarkersForTxnTopicPartition(Integer partition) {
            return markersPerPartition.remove(partition);
        }

        public void addMarkers(Integer txnTopicPartition, TxnIdAndMarkerEntry txnIdAndMarker) {
            BlockingQueue<TxnIdAndMarkerEntry> markersQueue =
                    markersPerPartition.computeIfAbsent(txnTopicPartition, k -> new BlockingArrayQueue<>());
            markersQueue.add(txnIdAndMarker);
        }

        public void forEachTxnTopicPartition(BiConsumer<Integer, BlockingQueue<TxnIdAndMarkerEntry>> f) {
            for (Map.Entry<Integer, BlockingQueue<TxnIdAndMarkerEntry>> entry: markersPerPartition.entrySet()) {
                Integer partition = entry.getKey();
                BlockingQueue<TxnIdAndMarkerEntry> queue = entry.getValue();
                if (!queue.isEmpty()) {
                    f.accept(partition, queue);
                }
            }
        }
    }

    public TransactionMarkerChannelManager(KafkaServiceConfiguration kafkaConfig,
                                           TransactionStateManager txnStateManager,
                                           KopBrokerLookupManager kopBrokerLookupManager,
                                           boolean enableTls) {
        this.kafkaConfig = kafkaConfig;
        this.txnStateManager = txnStateManager;
        this.kopBrokerLookupManager = kopBrokerLookupManager;
        this.enableTls = enableTls;
        if (this.enableTls) {
            sslContextFactory = SSLUtils.createSslContextFactory(kafkaConfig);
            sslEndPoint = EndPoint.getSslEndPoint(kafkaConfig.getKafkaListeners());
        } else {
            sslContextFactory = null;
            sslEndPoint = null;
        }
        eventLoopGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new TransactionMarkerChannelInitializer(kafkaConfig, enableTls));

        Thread thread = new Thread(() -> {
            while (!closed) {
                drainQueuedTransactionMarkers();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, "kop-transaction-channel-manager");
        thread.setDaemon(true);
        thread.start();
    }

    public CompletableFuture<TransactionMarkerChannelHandler> getChannel(InetSocketAddress socketAddress) {
        return handlerMap.computeIfAbsent(socketAddress, address -> {
            CompletableFuture<TransactionMarkerChannelHandler> handlerFuture = new CompletableFuture<>();
            ChannelFutures.toCompletableFuture(bootstrap.connect(socketAddress))
                    .thenAccept(channel -> {
                        handlerFuture.complete(
                                (TransactionMarkerChannelHandler) channel.pipeline().get("txnHandler"));
                    }).exceptionally(e -> {
                        handlerFuture.completeExceptionally(e);
                        return null;
                    });
            return handlerFuture;
        });
    }

    public void addTxnMarkersToSend(Integer coordinatorEpoch,
                                    TransactionResult txnResult,
                                    TransactionMetadata txnMetadata,
                                    TransactionMetadata.TxnTransitMetadata newMetadata) {
        String transactionalId = txnMetadata.getTransactionalId();
        PendingCompleteTxn pendingCompleteTxn = new PendingCompleteTxn(
                transactionalId,
                coordinatorEpoch,
                txnMetadata,
                newMetadata);

        transactionsWithPendingMarkers.put(transactionalId, pendingCompleteTxn);
        addTxnMarkersToBrokerQueue(transactionalId, txnMetadata.getProducerId(),
                txnMetadata.getProducerEpoch(), txnResult, coordinatorEpoch, txnMetadata.getTopicPartitions());
        maybeWriteTxnCompletion(transactionalId);
    }

    private boolean hasPendingMarkersToWrite(TransactionMetadata txnMetadata) {
        AtomicBoolean isHas = new AtomicBoolean(true);
        txnMetadata.inLock(() -> {
            isHas.set(!txnMetadata.getTopicPartitions().isEmpty());
            return null;
        });
        return isHas.get();
    }

    public void maybeWriteTxnCompletion(String transactionalId) {
        PendingCompleteTxn pendingCompleteTxn = transactionsWithPendingMarkers.get(transactionalId);
        if (!hasPendingMarkersToWrite(pendingCompleteTxn.txnMetadata)
                && transactionsWithPendingMarkers.remove(transactionalId, pendingCompleteTxn)) {
            writeTxnCompletion(pendingCompleteTxn);
        }
    }

    public void addTxnMarkersToBrokerQueue(String transactionalId,
                                           Long producerId,
                                           Short producerEpoch,
                                           TransactionResult result,
                                           Integer coordinatorEpoch,
                                           Set<TopicPartition> topicPartitions) {
        Integer txnTopicPartition = txnStateManager.partitionFor(transactionalId);

        Map<InetSocketAddress, List<TopicPartition>> addressAndPartitionMap = new ConcurrentHashMap<>();
        List<TopicPartition> unknownBrokerTopicList = new ArrayList<>();

        List<CompletableFuture<Void>> addressFutureList = new ArrayList<>();
        for (TopicPartition topicPartition : topicPartitions) {
            String pulsarTopic = new KopTopic(topicPartition.topic(), null)
                    .getPartitionName(topicPartition.partition());
            CompletableFuture<Optional<InetSocketAddress>> addressFuture =
                    kopBrokerLookupManager.findBroker(pulsarTopic, sslEndPoint);
            CompletableFuture<Void> addFuture = new CompletableFuture<>();
            addressFutureList.add(addFuture);
            addressFuture.whenComplete((address, throwable) -> {
                if (throwable != null) {
                    log.warn("Failed to find broker for topic partition {}", topicPartition, throwable);
                    unknownBrokerTopicList.add(topicPartition);
                    addFuture.completeExceptionally(throwable);
                    return;
                }
                addressAndPartitionMap.compute(address.orElse(null), (__, set) -> {
                    if (set == null) {
                        set = new ArrayList<>();
                    }
                    set.add(topicPartition);
                    return set;
                });
                addFuture.complete(null);
            });
        }
        FutureUtil.waitForAll(addressFutureList).whenComplete((ignored, throwable) -> {
            addressAndPartitionMap.forEach((address, partitions) -> {
                TxnMarkerEntry entry = new TxnMarkerEntry(
                        producerId, producerEpoch, coordinatorEpoch, result, partitions);
                TxnMarkerQueue markerQueue =
                        markersQueuePerBroker.computeIfAbsent(address, key -> new TxnMarkerQueue(address));
                markerQueue.addMarkers(txnTopicPartition, new TxnIdAndMarkerEntry(transactionalId, entry));
            });
            if (unknownBrokerTopicList.size() > 0) {
                TxnMarkerEntry entry = new TxnMarkerEntry(
                        producerId, producerEpoch, coordinatorEpoch, result, unknownBrokerTopicList);
                markersQueueForUnknownBroker.addMarkers(
                        txnTopicPartition, new TxnIdAndMarkerEntry(transactionalId, entry));
            }
        });
    }

    private void writeTxnCompletion(PendingCompleteTxn pendingCompleteTxn) {
        String transactionalId = pendingCompleteTxn.transactionalId;
        TransactionMetadata txnMetadata = pendingCompleteTxn.txnMetadata;
        TransactionMetadata.TxnTransitMetadata newMetadata = pendingCompleteTxn.newMetadata;
        int coordinatorEpoch = pendingCompleteTxn.coordinatorEpoch;

        log.info("Completed sending transaction markers for {}; begin transition to {}",
                transactionalId, newMetadata.getTxnState());

        ErrorsAndData<Optional<TransactionStateManager.CoordinatorEpochAndTxnMetadata>> errorsAndData =
                txnStateManager.getTransactionState(transactionalId);

        if (errorsAndData.hasErrors()) {
            switch (errorsAndData.getErrors()) {
                case NOT_COORDINATOR:
                    log.info("No longer the coordinator for {} with coordinator epoch {}; cancel appending {} to "
                            + "transaction log", transactionalId, coordinatorEpoch, newMetadata);
                    break;
                case COORDINATOR_LOAD_IN_PROGRESS:
                    log.info("Loading the transaction partition that contains {} while my current coordinator epoch "
                            + "is {}; so cancel appending {} to transaction log since the loading process will "
                            + "continue the remaining work", transactionalId, coordinatorEpoch, newMetadata);
                    break;
                default:
                    throw new IllegalStateException("Unhandled error {} when fetching current transaction state",
                            errorsAndData.getErrors().exception());
            }
        } else {
            if (!errorsAndData.getData().isPresent()) {
                String errorMsg = String.format("The coordinator still owns the transaction partition for %s, but "
                        + "there is no metadata in the cache; this is not expected", transactionalId);
                throw new IllegalStateException(errorMsg);
            }
            TransactionStateManager.CoordinatorEpochAndTxnMetadata epochAndMetadata = errorsAndData.getData().get();
            if (epochAndMetadata.getCoordinatorEpoch() == coordinatorEpoch) {
                log.debug("Sending {}'s transaction markers for {} with coordinator epoch {} succeeded, trying to "
                        + "append complete transaction log now", transactionalId, txnMetadata, coordinatorEpoch);
                tryAppendToLog(new PendingCompleteTxn(transactionalId, coordinatorEpoch, txnMetadata, newMetadata));
            } else {
                log.info("The cached metadata {} has changed to {} after completed sending the markers with "
                        + "coordinator epoch {}; abort transiting the metadata to {} as it may have been updated "
                        + "by another process", txnMetadata, epochAndMetadata, coordinatorEpoch, newMetadata);
            }
        }
    }

    private void tryAppendToLog(PendingCompleteTxn txnLogAppend) {
        // try to append to the transaction log
        txnStateManager.appendTransactionToLog(txnLogAppend.transactionalId, txnLogAppend.coordinatorEpoch,
                txnLogAppend.newMetadata, new TransactionStateManager.ResponseCallback() {
            @Override
            public void complete() {
                log.info("Completed transaction for {} with coordinator epoch {}, final state after commit: {}",
                    txnLogAppend.transactionalId, txnLogAppend.coordinatorEpoch, txnLogAppend.txnMetadata.getState());
            }

            @Override
            public void fail(Errors errors) {
                switch (errors) {
                    case NOT_COORDINATOR:
                        log.info("No longer the coordinator for transactionalId: {} while trying to append to "
                                + "transaction log, skip writing to transaction log", txnLogAppend.transactionalId);
                        break;
                    case COORDINATOR_NOT_AVAILABLE:
                        log.info("Not available to append {}: possible causes include {}, {}, {} and {}; "
                                + "retry appending", txnLogAppend, Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                Errors.NOT_ENOUGH_REPLICAS, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND,
                                Errors.REQUEST_TIMED_OUT);

                        // enqueue for retry
                        txnLogAppendRetryQueue.add(txnLogAppend);
                        break;
                    case COORDINATOR_LOAD_IN_PROGRESS:
                        log.info("Coordinator is loading the partition {} and hence cannot complete append of {}; "
                                + "skip writing to transaction log as the loading process should complete it",
                                txnStateManager.partitionFor(txnLogAppend.transactionalId), txnLogAppend);
                        break;
                    default:
                        String errorMsg = String.format("Unexpected error %s while appending to transaction log for %s",
                                errors.exceptionName(), txnLogAppend.transactionalId);
                        throw new IllegalStateException(errorMsg);
                }
            }
        }, null);
    }

    public void removeMarkersForTxnTopicPartition(Integer txnTopicPartitionId) {
        BlockingQueue<TxnIdAndMarkerEntry> unknownBrokerMarkerEntries =
                markersQueueForUnknownBroker.removeMarkersForTxnTopicPartition(txnTopicPartitionId);
        if (unknownBrokerMarkerEntries != null) {
            unknownBrokerMarkerEntries.forEach(markerEntry -> {
                removeMarkersForTxnId(markerEntry.getTransactionalId());
            });
        }

        markersQueuePerBroker.forEach((__, txnMarkerQueue) -> {
            BlockingQueue<TxnIdAndMarkerEntry> markerEntries =
                    txnMarkerQueue.removeMarkersForTxnTopicPartition(txnTopicPartitionId);
            if (markerEntries != null) {
                markerEntries.forEach(markerEntry -> {
                    removeMarkersForTxnId(markerEntry.getTransactionalId());
                });
            }
        });
    }

    public void removeMarkersForTxnId(String transactionalId) {
        transactionsWithPendingMarkers.remove(transactionalId);
    }

    private void retryLogAppends() {
        List<PendingCompleteTxn> txnLogAppendRetries = new ArrayList<>();
        txnLogAppendRetryQueue.drainTo(txnLogAppendRetries);
        for (PendingCompleteTxn pendingCompleteTxn : txnLogAppendRetries) {
            if (log.isDebugEnabled()) {
                log.debug("Retry appending {} transaction log", pendingCompleteTxn);
            }
            tryAppendToLog(pendingCompleteTxn);
        }
    }

    private void drainQueuedTransactionMarkers() {
        retryLogAppends();
        List<TxnIdAndMarkerEntry> txnIdAndMarkerEntries = new ArrayList<>();
        markersQueueForUnknownBroker.forEachTxnTopicPartition((__, queue) -> queue.drainTo(txnIdAndMarkerEntries));

        for (TxnIdAndMarkerEntry txnIdAndMarker : txnIdAndMarkerEntries) {
            String transactionalId = txnIdAndMarker.getTransactionalId();
            long producerId = txnIdAndMarker.getEntry().producerId();
            short producerEpoch = txnIdAndMarker.getEntry().producerEpoch();
            TransactionResult txnResult = txnIdAndMarker.getEntry().transactionResult();
            int coordinatorEpoch = txnIdAndMarker.getEntry().coordinatorEpoch();
            List<TopicPartition> topicPartitions = txnIdAndMarker.getEntry().partitions();

            addTxnMarkersToBrokerQueue(transactionalId, producerId, producerEpoch,
                    txnResult, coordinatorEpoch, new HashSet<>(topicPartitions));
        }

        for (TxnMarkerQueue txnMarkerQueue : markersQueuePerBroker.values()) {
            txnIdAndMarkerEntries.clear();
            txnMarkerQueue.forEachTxnTopicPartition((__, queue) -> queue.drainTo(txnIdAndMarkerEntries));
            if (!txnIdAndMarkerEntries.isEmpty()) {
                getChannel(txnMarkerQueue.address).whenComplete((channelHandler, throwable) -> {

                    List<TxnMarkerEntry> sendEntries = new ArrayList<>();
                    for (TxnIdAndMarkerEntry txnIdAndMarkerEntry : txnIdAndMarkerEntries) {
                        sendEntries.add(txnIdAndMarkerEntry.entry);
                    }
                    channelHandler.enqueueRequest(
                            new WriteTxnMarkersRequest.Builder(sendEntries).build(),
                            new TransactionMarkerRequestCompletionHandler(
                                    0, txnStateManager, this, txnIdAndMarkerEntries));
                });
            }
        }
    }

    public void close() {
        this.closed = true;
    }

}
