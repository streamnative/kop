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
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.kop.EndPoint;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.KopBrokerLookupManager;
import io.streamnative.pulsar.handlers.kop.scala.Either;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.ssl.SSLUtils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.AuthenticationUtil;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.ChannelFutures;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.ssl.SslContextFactory;


/**
 * Transaction marker channel manager.
 */
@Slf4j
public class TransactionMarkerChannelManager {

    private final String tenant;
    @Getter
    private final KafkaServiceConfiguration kafkaConfig;
    private final EventLoopGroup eventLoopGroup;
    private final boolean enableTls;
    private final SslContextFactory sslContextFactory;
    private final EndPoint sslEndPoint;
    private final KopBrokerLookupManager kopBrokerLookupManager;

    private final Bootstrap bootstrap;

    private final Map<InetSocketAddress, CompletableFuture<TransactionMarkerChannelHandler>> handlerMap =
            new ConcurrentHashMap<>();

    private TransactionStateManager txnStateManager;

    @Getter
    @VisibleForTesting
    private ConcurrentHashMap<String, PendingCompleteTxn> transactionsWithPendingMarkers = new ConcurrentHashMap<>();
    private Map<InetSocketAddress, TxnMarkerQueue> markersQueuePerBroker = new ConcurrentHashMap<>();
    private TxnMarkerQueue markersQueueForUnknownBroker = new TxnMarkerQueue(null);
    private BlockingQueue<PendingCompleteTxn> txnLogAppendRetryQueue = new LinkedBlockingQueue<>();
    private volatile boolean closed;
    private final String namespacePrefixForUserTopics;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> drainQueuedTransactionMarkersHandle;

    @Getter
    private Authentication authentication;

    @AllArgsConstructor
    @Getter
    @ToString
    protected static class PendingCompleteTxn {
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
    @ToString
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

    public TransactionMarkerChannelManager(String tenant,
                                           KafkaServiceConfiguration kafkaConfig,
                                           TransactionStateManager txnStateManager,
                                           KopBrokerLookupManager kopBrokerLookupManager,
                                           boolean enableTls,
                                           String namespacePrefixForUserTopics,
                                           ScheduledExecutorService scheduler) throws Exception {
        this.tenant = tenant;
        this.kafkaConfig = kafkaConfig;
        this.namespacePrefixForUserTopics = namespacePrefixForUserTopics;
        this.txnStateManager = txnStateManager;
        this.kopBrokerLookupManager = kopBrokerLookupManager;
        this.enableTls = enableTls;
        this.scheduler = scheduler;
        if (this.enableTls) {
            sslContextFactory = SSLUtils.createSslContextFactory(kafkaConfig);
            sslEndPoint = EndPoint.getSslEndPoint(kafkaConfig.getKafkaListeners());
        } else {
            sslContextFactory = null;
            sslEndPoint = null;
        }
        if (kafkaConfig.isAuthenticationEnabled()) {
            String auth = kafkaConfig.getBrokerClientAuthenticationPlugin();
            String authParams = kafkaConfig.getBrokerClientAuthenticationParameters();
            authentication = AuthenticationUtil.create(auth, authParams);
            authentication.start();
        }
        eventLoopGroup = EventLoopUtil
                .newEventLoopGroup(0, false, new DefaultThreadFactory("kop-txn"));
        bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(EventLoopUtil.getClientSocketChannelClass(eventLoopGroup));
        bootstrap.handler(new TransactionMarkerChannelInitializer(kafkaConfig, enableTls, this));
    }

    public CompletableFuture<TransactionMarkerChannelHandler> getChannel(InetSocketAddress socketAddress) {
        if (closed) {
            return FutureUtil.failedFuture(new Exception("This TransactionMarkerChannelManager is closed"));
        }
        ensureDrainQueuedTransactionMarkersActivity();
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

    public void channelFailed(InetSocketAddress socketAddress, TransactionMarkerChannelHandler handler) {
        log.error("channelFailed {} {}", socketAddress, handler);
        handlerMap.computeIfPresent(socketAddress, (kek, value) -> {
           if (value.isCompletedExceptionally() || value.isCancelled()) {
               return null;
           }
           final TransactionMarkerChannelHandler currentValue = value.getNow(null);
           if (currentValue == handler) {
               log.error("channelFailed removing {} {}", socketAddress, handler);
               // remove the entry only if it is the expected value
               return null;
           }
           return value;
        });
    }

    public void addTxnMarkersToSend(Integer coordinatorEpoch,
                                    TransactionResult txnResult,
                                    TransactionMetadata txnMetadata,
                                    TransactionMetadata.TxnTransitMetadata newMetadata,
                                    String namespacePrefix) {
        ensureDrainQueuedTransactionMarkersActivity();
        String transactionalId = txnMetadata.getTransactionalId();
        PendingCompleteTxn pendingCompleteTxn = new PendingCompleteTxn(
                transactionalId,
                coordinatorEpoch,
                txnMetadata,
                newMetadata);

        transactionsWithPendingMarkers.put(transactionalId, pendingCompleteTxn);
        addTxnMarkersToBrokerQueue(transactionalId, txnMetadata.getProducerId(),
                txnMetadata.getProducerEpoch(), txnResult, coordinatorEpoch, txnMetadata.getTopicPartitions(),
                namespacePrefix);
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
        ensureDrainQueuedTransactionMarkersActivity();
        Optional.ofNullable(transactionsWithPendingMarkers.get(transactionalId)).ifPresent(pendingCompleteTxn -> {
            if (!hasPendingMarkersToWrite(pendingCompleteTxn.txnMetadata)
                    && transactionsWithPendingMarkers.remove(transactionalId, pendingCompleteTxn)) {
                writeTxnCompletion(pendingCompleteTxn);
            }
        });
    }

    public void addTxnMarkersToBrokerQueue(String transactionalId,
                                           Long producerId,
                                           Short producerEpoch,
                                           TransactionResult result,
                                           Integer coordinatorEpoch,
                                           Set<TopicPartition> topicPartitions,
                                           String namespacePrefixForUserTopics) {
        ensureDrainQueuedTransactionMarkersActivity();
        Integer txnTopicPartition = txnStateManager.partitionFor(transactionalId);

        Map<InetSocketAddress, List<TopicPartition>> addressAndPartitionMap = new ConcurrentHashMap<>();
        List<TopicPartition> unknownBrokerTopicList = new CopyOnWriteArrayList<>();

        List<CompletableFuture<Void>> addressFutureList = new ArrayList<>();
        for (TopicPartition topicPartition : topicPartitions) {
            String pulsarTopic = new KopTopic(topicPartition.topic(), namespacePrefixForUserTopics)
                    .getPartitionName(topicPartition.partition());
            CompletableFuture<Void> addFuture = new CompletableFuture<>();
            addressFutureList.add(addFuture);
            kopBrokerLookupManager.isTopicExists(pulsarTopic)
                    .thenAccept(isTopicExists -> {
                        if (!isTopicExists) {
                            Either<Errors, Optional<TransactionStateManager.CoordinatorEpochAndTxnMetadata>>
                                    transactionState = txnStateManager.getTransactionState(transactionalId);
                            if (transactionState.isLeft()) {
                                log.info("Encountered {} trying to fetch transaction metadata for {} with coordinator "
                                                + "epoch {}; cancel sending markers to its partition leaders"
                                , transactionState.getLeft(), transactionalId, coordinatorEpoch);
                                transactionsWithPendingMarkers.remove(transactionalId);
                                addFuture.complete(null);
                                return;
                            }
                            Optional<TransactionStateManager.CoordinatorEpochAndTxnMetadata> epochAndTxnMetadata =
                                    transactionState.getRight();
                            if (epochAndTxnMetadata.isPresent()) {
                                if (!coordinatorEpoch.equals(epochAndTxnMetadata.get().getCoordinatorEpoch())) {
                                    log.info("The cached metadata has changed to {} (old coordinator epoch is {}) "
                                                    + "since preparing to send markers; "
                                                    + "cancel sending markers to its partition leaders",
                                            epochAndTxnMetadata, coordinatorEpoch);
                                    transactionsWithPendingMarkers.remove(transactionalId);
                                } else {
                                    log.info("Couldn't find leader endpoint for partitions {} while trying "
                                                    + "to send transaction markers for {}, these partitions are "
                                                    + "likely deleted already and hence can be skipped",
                                            topicPartition, transactionalId);
                                    TransactionMetadata txnMetadata =
                                            epochAndTxnMetadata.get().getTransactionMetadata();
                                    txnMetadata.inLock(() -> {
                                        topicPartitions.forEach(txnMetadata::removePartition);
                                        return null;
                                    });
                                    maybeWriteTxnCompletion(transactionalId);
                                }
                            } else {
                                log.error("The coordinator still owns "
                                        + "the transaction partition for {}, but there is no metadata in the cache; "
                                        + "this is not expected", transactionalId);
                                addFuture.complete(null);
                                return;
                            }
                            addFuture.complete(null);
                            return;
                        }

                        CompletableFuture<Optional<InetSocketAddress>> addressFuture =
                                kopBrokerLookupManager.findBroker(pulsarTopic, sslEndPoint);

                        addressFuture.whenComplete((address, throwable) -> {
                            if (throwable != null) {
                                log.warn("Failed to find broker for topic partition {}", topicPartition, throwable);
                                unknownBrokerTopicList.add(topicPartition);
                                addFuture.complete(null);
                                return;
                            }
                            if (!address.isPresent()) {
                                log.warn("No address for broker for topic partition {}", topicPartition);
                                unknownBrokerTopicList.add(topicPartition);
                                addFuture.complete(null);
                                return;
                            }
                            addressAndPartitionMap.compute(address.get(), (__, set) -> {
                                if (set == null) {
                                    set = new ArrayList<>();
                                }
                                set.add(topicPartition);
                                return set;
                            });
                            addFuture.complete(null);
                        });
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

        if (log.isDebugEnabled()) {
            log.debug("Completed sending transaction markers for {}; begin transition to {}",
                    transactionalId, newMetadata.getTxnState());
        }

        Either<Errors, Optional<TransactionStateManager.CoordinatorEpochAndTxnMetadata>> errorsAndData =
                txnStateManager.getTransactionState(transactionalId);

        if (errorsAndData.isLeft()) {
            switch (errorsAndData.getLeft()) {
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
                            errorsAndData.getLeft().exception());
            }
        } else {
            if (!errorsAndData.getRight().isPresent()) {
                String errorMsg = String.format("The coordinator still owns the transaction partition for %s, but "
                        + "there is no metadata in the cache; this is not expected", transactionalId);
                throw new IllegalStateException(errorMsg);
            }
            TransactionStateManager.CoordinatorEpochAndTxnMetadata epochAndMetadata = errorsAndData.getRight().get();
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
                if (log.isDebugEnabled()) {
                    log.debug("Completed transaction for {} with coordinator epoch {}, "
                                    + "final state after commit: {}",
                            txnLogAppend.transactionalId, txnLogAppend.coordinatorEpoch,
                            txnLogAppend.txnMetadata.getState());
                }
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
                        log.error(errorMsg);
                        throw new IllegalStateException(errorMsg);
                }
            }
        }, null);
    }

    public void removeMarkersForTxnTopicPartition(Integer txnTopicPartitionId) {
        ensureDrainQueuedTransactionMarkersActivity();
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
                    txnResult, coordinatorEpoch, new HashSet<>(topicPartitions), namespacePrefixForUserTopics);
        }

        for (TxnMarkerQueue txnMarkerQueue : markersQueuePerBroker.values()) {
            List<TxnIdAndMarkerEntry> txnIdAndMarkerEntriesForMarker = new ArrayList<>();
            txnMarkerQueue.forEachTxnTopicPartition((__, queue) -> queue.drainTo(txnIdAndMarkerEntriesForMarker));
            if (!txnIdAndMarkerEntriesForMarker.isEmpty()) {
                getChannel(txnMarkerQueue.address).whenComplete((channelHandler, throwable) -> {
                    if (throwable != null) {
                        log.error("Get channel for {} failed, re-enqueing {} txnIdAndMarkerEntriesForMarker",
                                txnMarkerQueue.address, txnIdAndMarkerEntriesForMarker.size());
                        // put back
                        txnIdAndMarkerEntriesForMarker.forEach(txnIdAndMarkerEntry -> {
                            log.error("Re-enqueueing {}", txnIdAndMarkerEntry);
                            addTxnMarkersToBrokerQueue(txnIdAndMarkerEntry.getTransactionalId(),
                                    txnIdAndMarkerEntry.getEntry().producerId(),
                                    txnIdAndMarkerEntry.getEntry().producerEpoch(),
                                    txnIdAndMarkerEntry.getEntry().transactionResult(),
                                    txnIdAndMarkerEntry.getEntry().coordinatorEpoch(),
                                    new HashSet<>(txnIdAndMarkerEntry.getEntry().partitions()),
                                    namespacePrefixForUserTopics);
                        });
                    } else {
                        List<TxnMarkerEntry> sendEntries = new ArrayList<>();
                        for (TxnIdAndMarkerEntry txnIdAndMarkerEntry : txnIdAndMarkerEntriesForMarker) {
                            sendEntries.add(txnIdAndMarkerEntry.entry);
                        }
                        channelHandler.enqueueWriteTxnMarkers(sendEntries,
                                new TransactionMarkerRequestCompletionHandler(txnStateManager, this,
                                        kopBrokerLookupManager,
                                        txnIdAndMarkerEntriesForMarker, namespacePrefixForUserTopics));
                    }
                });
            }
        }
    }

    private synchronized void ensureDrainQueuedTransactionMarkersActivity() {
        if (drainQueuedTransactionMarkersHandle != null || closed) {
            return;
        }
        drainQueuedTransactionMarkersHandle = scheduler.scheduleWithFixedDelay(() -> {
            drainQueuedTransactionMarkers();
        }, 100, 100, TimeUnit.MILLISECONDS);
    }

    private synchronized void stopDrainQueuedTransactionMarkersHandleActivity() {
        if (drainQueuedTransactionMarkersHandle != null) {
            drainQueuedTransactionMarkersHandle.cancel(false);
        }
    }

    public void close() {
        this.closed = true;
        stopDrainQueuedTransactionMarkersHandleActivity();
        handlerMap.forEach((address, handler) -> {
            try {
                final TransactionMarkerChannelHandler transactionMarkerChannelHandler = handler.get();
                transactionMarkerChannelHandler.close();
            } catch (ExecutionException | InterruptedException err) {
                log.info("Cannot close TransactionMarkerChannelHandler for {}", address, err);
            }
        });
        if (authentication != null) {
            try {
                authentication.close();
            } catch (IOException e) {
                log.error("Transaction marker authentication close failed.", e);
            }
        }
    }

    public String getAuthenticationUsername() {
        return tenant;
    }
}
