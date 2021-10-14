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
package io.streamnative.pulsar.handlers.kop;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;

/**
 * KafkaTopicManager manages a Map of topic to KafkaTopicConsumerManager.
 * For each topic, there is a KafkaTopicConsumerManager, which manages a topic and its related offset cursor.
 * This is mainly used to cache the produce/consume topic, not include offsetTopic.
 */
@Slf4j
public class KafkaTopicManager {

    private static final KafkaTopicConsumerManagerCache TCM_CACHE = KafkaTopicConsumerManagerCache.getInstance();

    private final KafkaRequestHandler requestHandler;
    private final BrokerService brokerService;
    private final LookupClient lookupClient;
    private volatile SocketAddress remoteAddress;

    // cache for topics: <topicName, persistentTopic>, for removing producer
    @Getter
    private static final ConcurrentHashMap<String, CompletableFuture<Optional<PersistentTopic>>>
        topics = new ConcurrentHashMap<>();
    // cache for references in PersistentTopic: <topicName, producer>
    @Getter
    private static final ConcurrentHashMap<String, Producer>
        references = new ConcurrentHashMap<>();

    private final InternalServerCnx internalServerCnx;

    // every 1 min, check if the KafkaTopicConsumerManagers have expired cursors.
    // remove expired cursors, so backlog can be cleared.
    private static final long checkPeriodMillis = 1 * 60 * 1000;
    private static final long expirePeriodMillis = 2 * 60 * 1000;
    private static volatile ScheduledFuture<?> cursorExpireTask = null;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static final ConcurrentHashMap<String, ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>>>
        LOOKUP_CACHE = new ConcurrentHashMap<>();

    public static final ConcurrentHashMap<String, CompletableFuture<Optional<String>>>
            KOP_ADDRESS_CACHE = new ConcurrentHashMap<>();

    KafkaTopicManager(KafkaRequestHandler kafkaRequestHandler) {
        this.requestHandler = kafkaRequestHandler;
        PulsarService pulsarService = kafkaRequestHandler.getPulsarService();
        this.brokerService = pulsarService.getBrokerService();
        this.internalServerCnx = new InternalServerCnx(requestHandler);
        this.lookupClient = KafkaProtocolHandler.getLookupClient(pulsarService);

        initializeCursorExpireTask(brokerService.executor());
    }

    private static void initializeCursorExpireTask(final ScheduledExecutorService executor) {
        if (cursorExpireTask == null) {
            synchronized (KafkaTopicManager.class) {
                if (cursorExpireTask == null) {
                    // check expired cursor every 1 min.
                    cursorExpireTask = executor.scheduleWithFixedDelay(() -> {
                        long current = System.currentTimeMillis();
                        TCM_CACHE.forEach(future -> {
                            if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
                                future.join().deleteExpiredCursor(current, expirePeriodMillis);
                            }
                        });
                    }, checkPeriodMillis, checkPeriodMillis, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    public static void cancelCursorExpireTask() {
        synchronized (KafkaTopicManager.class) {
            if (cursorExpireTask != null) {
                cursorExpireTask.cancel(true);
                cursorExpireTask = null;
            }
        }
    }

    // update Ctx information, since at internalServerCnx create time there is no ctx passed into kafkaRequestHandler.
    public void setRemoteAddress(SocketAddress remoteAddress) {
        internalServerCnx.updateCtx(remoteAddress);
        this.remoteAddress = remoteAddress;
    }

    // topicName is in pulsar format. e.g. persistent://public/default/topic-partition-0
    // future will complete with null when topic not owned by this broker, or meet error.
    public CompletableFuture<KafkaTopicConsumerManager> getTopicConsumerManager(String topicName) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Return null for getTopicConsumerManager({}) since channel closing",
                        requestHandler.ctx.channel(), topicName);
            }
            return CompletableFuture.completedFuture(null);
        }
        if (remoteAddress == null) {
            log.error("[{}] Try to getTopicConsumerManager({}) while remoteAddress is not set",
                    requestHandler.ctx.channel(), topicName);
            return CompletableFuture.completedFuture(null);
        }
        return TCM_CACHE.computeIfAbsent(
            topicName,
            remoteAddress,
            () -> {
                final CompletableFuture<KafkaTopicConsumerManager> tcmFuture = new CompletableFuture<>();
                getTopic(topicName).whenComplete((persistentTopic, throwable) -> {
                    if (persistentTopic.isPresent() && throwable == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Call getTopicConsumerManager for {}, and create TCM for {}.",
                                    requestHandler.ctx.channel(), topicName, persistentTopic);
                        }
                        tcmFuture.complete(new KafkaTopicConsumerManager(requestHandler, persistentTopic.get()));
                    } else {
                        if (throwable != null) {
                            log.error("[{}] Failed to getTopicConsumerManager caused by getTopic '{}' throws {}",
                                    requestHandler.ctx.channel(), topicName, throwable.getMessage());
                        } else { // persistentTopic == null
                            log.error("[{}] Failed to getTopicConsumerManager caused by getTopic '{}' returns empty",
                                    requestHandler.ctx.channel(), topicName);
                        }
                        tcmFuture.complete(null);
                    }
                });
                return tcmFuture;
            }
        );
    }

    public static void removeTopicManagerCache(String topicName) {
        LOOKUP_CACHE.remove(topicName);
        KOP_ADDRESS_CACHE.remove(topicName);
    }

    public static void clearTopicManagerCache() {
        LOOKUP_CACHE.clear();
        KOP_ADDRESS_CACHE.clear();
    }

    private Producer registerInPersistentTopic(PersistentTopic persistentTopic) {
        Producer producer = new InternalProducer(persistentTopic, internalServerCnx,
            lookupClient.getPulsarClient().newRequestId(),
            brokerService.generateUniqueProducerName());

        if (log.isDebugEnabled()) {
            log.debug("[{}] Register Mock Producer {} into PersistentTopic {}",
                requestHandler.ctx.channel(), producer, persistentTopic.getName());
        }

        // this will register and add USAGE_COUNT_UPDATER.
        persistentTopic.addProducer(producer, new CompletableFuture<>());
        return producer;
    }

    // call pulsarclient.lookup.getbroker to get and
    // own a topic.
    //    // when error happens, the returned future will complete with null.
    public CompletableFuture<InetSocketAddress> getTopicBroker(String topicName, String listenerName) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Return null for getTopicBroker({}) since channel closing",
                        requestHandler.ctx.channel(), topicName);
            }
            return CompletableFuture.completedFuture(null);
        }

        ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>> topicLookupCache =
                LOOKUP_CACHE.computeIfAbsent(topicName, t-> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] topic {} not in Lookup_cache, call lookupBroker",
                                requestHandler.ctx.channel(), topicName);
                    }
                    ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>> cache = new ConcurrentHashMap<>();
                    cache.put(listenerName == null ? "" : listenerName, lookupBroker(topicName, listenerName));
                    return cache;
                });

        return topicLookupCache.computeIfAbsent(listenerName == null ? "" : listenerName, t-> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] topic {} not in Lookup_cache, call lookupBroker",
                        requestHandler.ctx.channel(), topicName);
            }
            return lookupBroker(topicName, listenerName);
        });
    }

    private CompletableFuture<InetSocketAddress> lookupBroker(final String topic, String listenerName) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Return null for getTopic({}) since channel closing",
                        requestHandler.ctx.channel(), topic);
            }
            return CompletableFuture.completedFuture(null);
        }
        return lookupClient.getBrokerAddress(TopicName.get(topic), listenerName);
    }

    // A wrapper of `BrokerService#getTopic` that is to find the topic's associated `PersistentTopic` instance
    public CompletableFuture<Optional<PersistentTopic>> getTopic(String topicName) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Return null for getTopic({}) since channel is closing",
                        requestHandler.ctx.channel(), topicName);
            }
            return CompletableFuture.completedFuture(Optional.empty());
        }
        CompletableFuture<Optional<PersistentTopic>> topicCompletableFuture = new CompletableFuture<>();
        brokerService.getTopicIfExists(topicName).whenComplete((t2, throwable) -> {
            TopicName topicNameObject = TopicName.get(topicName);
            if (throwable != null) {
                // Failed to getTopic from current broker, remove cache, which added in getTopicBroker.
                removeTopicManagerCache(topicName);
                if (topicNameObject.getPartitionIndex() == 0) {
                    log.warn("Get partition-0 error [{}].", throwable.getMessage());
                } else {
                    handleGetTopicException(topicName, topicCompletableFuture, throwable);
                    return;
                }
            }
            if (t2 != null && t2.isPresent()) {
                topicCompletableFuture.complete(Optional.of((PersistentTopic) t2.get()));
                return;
            }
            // Fallback try use non-partitioned topic
            if (topicNameObject.getPartitionIndex() == 0) {
                String nonPartitionedTopicName = topicNameObject.getPartitionedTopicName();
                if (log.isDebugEnabled()) {
                    log.debug("[{}]Try to get non-partitioned topic for name {}",
                            requestHandler.ctx.channel(), nonPartitionedTopicName);
                }
                brokerService.getTopicIfExists(nonPartitionedTopicName).whenComplete((nonPartitionedTopic, ex) -> {
                    if (ex != null) {
                        handleGetTopicException(nonPartitionedTopicName, topicCompletableFuture, ex);
                        // Failed to getTopic from current broker, remove non-partitioned topic cache,
                        // which added in getTopicBroker.
                        removeTopicManagerCache(nonPartitionedTopicName);
                        return;
                    }
                    if (nonPartitionedTopic.isPresent()) {
                        PersistentTopic persistentTopic = (PersistentTopic) nonPartitionedTopic.get();
                        topicCompletableFuture.complete(Optional.of(persistentTopic));
                    } else {
                        log.error("[{}]Get empty non-partitioned topic for name {}",
                                requestHandler.ctx.channel(), nonPartitionedTopicName);
                        removeTopicManagerCache(nonPartitionedTopicName);
                        topicCompletableFuture.complete(Optional.empty());
                    }
                });
                return;
            }
            log.error("[{}]Get empty topic for name {}", requestHandler.ctx.channel(), topicName);
            removeTopicManagerCache(topicName);
            topicCompletableFuture.complete(Optional.empty());
        });
        // cache for removing producer
        topics.put(topicName, topicCompletableFuture);
        return topicCompletableFuture;
    }

    private void handleGetTopicException(@NonNull final String topicName,
                                  @NonNull final CompletableFuture<Optional<PersistentTopic>> topicCompletableFuture,
                                  @NonNull final Throwable ex) {
        // The ServiceUnitNotReadyException is retryable, so we should print a warning log instead of error log
        if (ex instanceof BrokerServiceException.ServiceUnitNotReadyException) {
            log.warn("[{}] Failed to getTopic {}: {}",
                    requestHandler.ctx.channel(), topicName, ex.getMessage());
            topicCompletableFuture.complete(Optional.empty());
        } else {
            log.error("[{}] Failed to getTopic {}. exception:",
                    requestHandler.ctx.channel(), topicName, ex);
            topicCompletableFuture.completeExceptionally(ex);
        }

    }

    public void registerProducerInPersistentTopic(String topicName, PersistentTopic persistentTopic) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to registerProducerInPersistentTopic for topic '{}'",
                        requestHandler.ctx.channel(), topicName);
            }
            return;
        }
        if (references.containsKey(topicName)) {
            return;
        }
        synchronized (this) {
            if (references.containsKey(topicName)) {
                return;
            }
            references.put(topicName, registerInPersistentTopic(persistentTopic));
        }
    }

    // when channel close, release all the topics reference in persistentTopic
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Closing TopicManager",
                        requestHandler.ctx.channel());
            }
            return;
        }

        try {
            TCM_CACHE.removeAndCloseByAddress(remoteAddress);

            topics.keySet().forEach(topicName -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] remove producer {} for topic {} at close()",
                            requestHandler.ctx.channel(), references.get(topicName), topicName);
                }
                removePersistentTopicAndReferenceProducer(topicName);
            });
        } catch (Exception e) {
            log.error("[{}] Failed to close KafkaTopicManager. exception:",
                requestHandler.ctx.channel(), e);
        }
    }

    public static Producer getReferenceProducer(String topicName) {
        return references.get(topicName);
    }

    private static void removePersistentTopicAndReferenceProducer(final String topicName) {
        // 1. Remove PersistentTopic and Producer from caches, these calls are thread safe
        final CompletableFuture<Optional<PersistentTopic>> topicFuture = topics.remove(topicName);
        final Producer producer = references.remove(topicName);

        if (topicFuture == null) {
            removeTopicManagerCache(topicName);
            return;
        }

        // 2. Remove Producer from PersistentTopic's internal cache
        try {
            // It's safe to wait until the future is completed because it's completed when
            // `BrokerService#getTopicIfExists` completed and it won't block too long.
            final Optional<PersistentTopic> persistentTopic = topicFuture.get();
            if (producer != null && persistentTopic.isPresent()) {
                try {
                    persistentTopic.get().removeProducer(producer);
                } catch (IllegalArgumentException ignored) {
                    log.error("[{}] The producer's topic ({}) doesn't match the current PersistentTopic",
                            topicName, (producer.getTopic() == null) ? "null" : producer.getTopic().getName());
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to get topic '{}' in removeTopicAndReferenceProducer", topicName, e);
        }
    }

    public static void deReference(String topicName) {
        try {
            removeTopicManagerCache(topicName);

            TCM_CACHE.removeAndCloseByTopic(topicName);
            removePersistentTopicAndReferenceProducer(topicName);
        } catch (Exception e) {
            log.error("Failed to close reference for individual topic {}. exception:", topicName, e);
        }
    }
}
