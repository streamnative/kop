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

import com.google.common.annotations.VisibleForTesting;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
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

    private final KafkaRequestHandler requestHandler;
    private final PulsarService pulsarService;
    private final BrokerService brokerService;

    // consumerTopicManagers for consumers cache.
    private static final ConcurrentHashMap<String, CompletableFuture<KafkaTopicConsumerManager>>
        consumerTopicManagers = new ConcurrentHashMap<>();

    // cache for topics: <topicName, persistentTopic>, for removing producer
    @Getter
    private static final ConcurrentHashMap<String, CompletableFuture<PersistentTopic>>
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

    public static final ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>>
        LOOKUP_CACHE = new ConcurrentHashMap<>();

    public static final ConcurrentHashMap<String, CompletableFuture<Optional<String>>>
            KOP_ADDRESS_CACHE = new ConcurrentHashMap<>();

    KafkaTopicManager(KafkaRequestHandler kafkaRequestHandler) {
        this.requestHandler = kafkaRequestHandler;
        this.pulsarService = kafkaRequestHandler.getPulsarService();
        this.brokerService = pulsarService.getBrokerService();
        this.internalServerCnx = new InternalServerCnx(requestHandler);

        initializeCursorExpireTask(brokerService.executor());
    }

    private static void initializeCursorExpireTask(final ScheduledExecutorService executor) {
        if (cursorExpireTask == null) {
            synchronized (KafkaTopicManager.class) {
                if (cursorExpireTask == null) {
                    // check expired cursor every 1 min.
                    cursorExpireTask = executor.scheduleWithFixedDelay(() -> {
                        long current = System.currentTimeMillis();
                        consumerTopicManagers.values().forEach(future -> {
                            if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
                                future.join().deleteExpiredCursor(current, expirePeriodMillis);
                            }
                        });
                    }, checkPeriodMillis, checkPeriodMillis, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    // update Ctx information, since at internalServerCnx create time there is no ctx passed into kafkaRequestHandler.
    public void updateCtx() {
        internalServerCnx.updateCtx();
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
        return consumerTopicManagers.computeIfAbsent(
            topicName,
            t -> {
                final CompletableFuture<KafkaTopicConsumerManager> tcmFuture = new CompletableFuture<>();
                getTopic(t).whenComplete((persistentTopic, throwable) -> {
                    if (persistentTopic != null && throwable == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Call getTopicConsumerManager for {}, and create TCM for {}.",
                                    requestHandler.ctx.channel(), topicName, persistentTopic);
                        }
                        tcmFuture.complete(new KafkaTopicConsumerManager(requestHandler, persistentTopic));
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
            KafkaProtocolHandler.getLookupClient(pulsarService).getPulsarClient().newRequestId(),
            brokerService.generateUniqueProducerName());

        if (log.isDebugEnabled()) {
            log.debug("[{}] Register Mock Producer {} into PersistentTopic {}",
                requestHandler.ctx.channel(), producer, persistentTopic.getName());
        }

        // this will register and add USAGE_COUNT_UPDATER.
        persistentTopic.addProducer(producer, new CompletableFuture<>());
        return producer;
    }

    // call pulsarclient.lookup.getbroker to get and own a topic.
    // when error happens, the returned future will complete with null.
    public CompletableFuture<InetSocketAddress> getTopicBroker(String topicName) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Return null for getTopicBroker({}) since channel closing",
                        requestHandler.ctx.channel(), topicName);
            }
            return CompletableFuture.completedFuture(null);
        }
        return LOOKUP_CACHE.computeIfAbsent(topicName, t -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] topic {} not in Lookup_cache, call lookupBroker",
                    requestHandler.ctx.channel(), topicName);
            }
            return KafkaProtocolHandler.getLookupClient(pulsarService).getBrokerAddress(TopicName.get(topicName));
        });
    }

    // A wrapper of `BrokerService#getTopic` that is to find the topic's associated `PersistentTopic` instance
    public CompletableFuture<PersistentTopic> getTopic(String topicName) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Return null for getTopic({}) since channel is closing",
                        requestHandler.ctx.channel(), topicName);
            }
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<PersistentTopic> topicCompletableFuture = new CompletableFuture<>();
        brokerService.getTopicIfExists(topicName).whenComplete((t2, throwable) -> {
            if (throwable != null) {
                // The ServiceUnitNotReadyException is retriable so we should print a warning log instead of error log
                if (throwable instanceof BrokerServiceException.ServiceUnitNotReadyException) {
                    log.warn("[{}] Failed to getTopic {}: {}",
                            requestHandler.ctx.channel(), topicName, throwable.getMessage());
                    topicCompletableFuture.complete(null);
                } else {
                    log.error("[{}] Failed to getTopic {}. exception:",
                            requestHandler.ctx.channel(), topicName, throwable);
                    topicCompletableFuture.completeExceptionally(throwable);
                }
                // failed to getTopic from current broker, remove cache, which added in getTopicBroker.
                removeTopicManagerCache(topicName);
                return;
            }
            if (t2.isPresent()) {
                PersistentTopic persistentTopic = (PersistentTopic) t2.get();
                topicCompletableFuture.complete(persistentTopic);
            } else {
                log.error("[{}]Get empty topic for name {}", requestHandler.ctx.channel(), topicName);
                removeTopicManagerCache(topicName);
                topicCompletableFuture.complete(null);
            }
        });
        // cache for removing producer
        topics.put(topicName, topicCompletableFuture);
        return topicCompletableFuture;
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
            closeKafkaTopicConsumerManagers();

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
        final CompletableFuture<PersistentTopic> topicFuture = topics.remove(topicName);
        final Producer producer = references.remove(topicName);

        if (topicFuture == null) {
            removeTopicManagerCache(topicName);
            return;
        }

        // 2. Remove Producer from PersistentTopic's internal cache
        try {
            // It's safe to wait until the future is completed because it's completed when
            // `BrokerService#getTopicIfExists` completed and it won't block too long.
            final PersistentTopic persistentTopic = topicFuture.get();
            if (producer != null && persistentTopic != null) {
                try {
                    persistentTopic.removeProducer(producer);
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

            Optional.ofNullable(consumerTopicManagers.remove(topicName)).ifPresent(
                    // Use thenAccept to avoid blocking
                    tcmFuture -> tcmFuture.thenAccept(tcm -> {
                        if (tcm != null) {
                            tcm.close();
                        }
                    })
            );

            removePersistentTopicAndReferenceProducer(topicName);
        } catch (Exception e) {
            log.error("Failed to close reference for individual topic {}. exception:", topicName, e);
        }
    }

    public static void removeKafkaTopicConsumerManager(String topicName) {
        consumerTopicManagers.remove(topicName);
    }

    public static void closeKafkaTopicConsumerManagers() {
        synchronized (KafkaTopicManager.class) {
            if (cursorExpireTask != null) {
                cursorExpireTask.cancel(true);
                cursorExpireTask = null;
            }
        }
        consumerTopicManagers.forEach((topic, tcmFuture) -> {
            try {
                Optional.ofNullable(tcmFuture.get(300, TimeUnit.SECONDS))
                        .ifPresent(KafkaTopicConsumerManager::close);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.warn("Failed to get TCM future of {} when trying to close it", topic);
            }
        });
        consumerTopicManagers.clear();
    }

    @VisibleForTesting
    public static int getNumberOfKafkaTopicConsumerManagers() {
        return consumerTopicManagers.size();
    }
}
