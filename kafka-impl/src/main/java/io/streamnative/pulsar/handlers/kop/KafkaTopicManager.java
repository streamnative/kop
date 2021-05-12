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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.pulsar.handlers.kop.coordinator.group.OffsetAcker;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
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
    private final ScheduledFuture<?> cursorExpireTask;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static final ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>>
        LOOKUP_CACHE = new ConcurrentHashMap<>();

    public static final ConcurrentHashMap<String, CompletableFuture<Optional<String>>>
            KOP_ADDRESS_CACHE = new ConcurrentHashMap<>();

    // cache for consumers for collect metrics: <groupId, Consumer>
    public static final ConcurrentHashMap<String, CompletableFuture<Consumer>>
            CONSUMERS_CACHE = new ConcurrentHashMap<>();

    KafkaTopicManager(KafkaRequestHandler kafkaRequestHandler) {
        this.requestHandler = kafkaRequestHandler;
        this.pulsarService = kafkaRequestHandler.getPulsarService();
        this.brokerService = pulsarService.getBrokerService();
        this.internalServerCnx = new InternalServerCnx(requestHandler);

        // check expired cursor every 1 min.
        this.cursorExpireTask = brokerService.executor().scheduleWithFixedDelay(() -> {
            long current = System.currentTimeMillis();
            if (log.isDebugEnabled()) {
                log.debug("[{}] Schedule a check of expired cursor",
                    requestHandler.ctx.channel());
            }
            consumerTopicManagers.values().forEach(future -> {
                if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
                    future.join().deleteExpiredCursor(current, expirePeriodMillis);
                }
            });
        }, checkPeriodMillis, checkPeriodMillis, TimeUnit.MILLISECONDS);
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
        CONSUMERS_CACHE.clear();
    }

    public static void clearTopicManagerCache() {
        LOOKUP_CACHE.clear();
        KOP_ADDRESS_CACHE.clear();
    }

    // exception throw for pulsar.getClient();
    private Producer registerInPersistentTopic(PersistentTopic persistentTopic) throws Exception {
        Producer producer = new InternalProducer(persistentTopic, internalServerCnx,
            ((PulsarClientImpl) (pulsarService.getClient())).newRequestId(),
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
            return lookupBroker(topicName);
        });
    }

    public InternalServerCnx getInternalServerCnx() {
        return internalServerCnx;
    }

    // this method do the real lookup into Pulsar broker.
    // retFuture will be completed with null when meet error.
    private CompletableFuture<InetSocketAddress> lookupBroker(String topicName) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Return null for getTopic({}) since channel closing",
                        requestHandler.ctx.channel(), topicName);
            }
            return CompletableFuture.completedFuture(null);
        }
        try {
            final CompletableFuture<InetSocketAddress> retFuture = new CompletableFuture<>();
            ((PulsarClientImpl) pulsarService.getClient()).getLookup()
                .getBroker(TopicName.get(topicName))
                .thenAccept(pair -> {
                    checkState(pair.getLeft().equals(pair.getRight()));
                    retFuture.complete(pair.getLeft());
                })
                .exceptionally(th -> {
                    log.warn("[{}] getBroker for topic failed. throwable: ",
                            topicName, th);
                    retFuture.complete(null);
                    return null;
                });
            return retFuture;
        } catch (PulsarServerException e) {
            log.error("[{}] getTopicBroker for topic {} failed get pulsar client, return null. throwable: ",
                requestHandler.ctx.channel(), topicName, e);
            return CompletableFuture.completedFuture(null);
        }
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
        try {
            if (references.containsKey(topicName)) {
                return;
            }
            synchronized (this) {
                if (references.containsKey(topicName)) {
                    return;
                }
                references.put(topicName, registerInPersistentTopic(persistentTopic));
            }
        } catch (Exception e){
            log.error("[{}] Failed to register producer in PersistentTopic {}. exception:",
                    requestHandler.ctx.channel(), topicName, e);
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
            this.cursorExpireTask.cancel(true);

            closeKafkaTopicConsumerManagers();

            for (Map.Entry<String, CompletableFuture<PersistentTopic>> entry : topics.entrySet()) {
                String topicName = entry.getKey();
                CompletableFuture<PersistentTopic> topicFuture = entry.getValue();
                if (log.isDebugEnabled()) {
                    log.debug("[{}] remove producer {} for topic {} at close()",
                        requestHandler.ctx.channel(), references.get(topicName), topicName);
                }
                if (references.get(topicName) != null) {
                    PersistentTopic persistentTopic = topicFuture.get();
                    if (persistentTopic != null) {
                        persistentTopic.removeProducer(references.get(topicName));
                    }
                    references.remove(topicName);
                }
            }
            // clear topics after close
            topics.clear();
        } catch (Exception e) {
            log.error("[{}] Failed to close KafkaTopicManager. exception:",
                requestHandler.ctx.channel(), e);
        }
    }

    public static Producer getReferenceProducer(String topicName) {
        return references.get(topicName);
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

            if (!topics.containsKey(topicName)) {
                return;
            }
            PersistentTopic persistentTopic = topics.get(topicName).get();
            Producer producer = references.get(topicName);
            if (persistentTopic != null && producer != null) {
                persistentTopic.removeProducer(producer);
            }
            topics.remove(topicName);

            OffsetAcker.removeOffsetAcker(topicName);
        } catch (Exception e) {
            log.error("Failed to close reference for individual topic {}. exception:", topicName, e);
        }
    }

    public static void removeKafkaTopicConsumerManager(String topicName) {
        consumerTopicManagers.remove(topicName);
    }

    public static void closeKafkaTopicConsumerManagers() {
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

    public CompletableFuture<Consumer> getGroupConsumers(String groupId, TopicPartition kafkaPartition) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Return null for getGroupConsumers({}, {}) since channel closing",
                        requestHandler.ctx.channel(), groupId, kafkaPartition);
            }
            return CompletableFuture.completedFuture(null);
        }
        if (StringUtils.isEmpty(groupId)) {
            if (log.isDebugEnabled()) {
                log.debug("Try to get group consumers with an empty group id");
            }
            return CompletableFuture.completedFuture(null);
        }

        // The future of the offset consumer should be created before in `GroupCoordinator#handleSyncGroup`
        final OffsetAcker offsetAcker = requestHandler.getGroupCoordinator().getOffsetAcker();
        final CompletableFuture<org.apache.pulsar.client.api.Consumer<byte[]>> offsetConsumerFuture =
                offsetAcker.getConsumer(groupId, kafkaPartition);
        if (offsetConsumerFuture == null) {
            if (log.isDebugEnabled()) {
                log.debug("No offset consumer for [group={}] [topic={}]", groupId, kafkaPartition);
            }
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Consumer> consumerFuture = new CompletableFuture<>();
        return CONSUMERS_CACHE.computeIfAbsent(groupId, group -> {
            try {
                TopicName topicName = TopicName.get(KopTopic.toString(kafkaPartition));
                NamespaceBundle namespaceBundle = pulsarService.getBrokerService()
                        .pulsar().getNamespaceService().getBundle(topicName);
                PersistentTopic persistentTopic = (PersistentTopic) pulsarService
                        .getBrokerService().getMultiLayerTopicsMap()
                        .get(topicName.getNamespace()).get(namespaceBundle.toString())
                        .get(topicName.toString());
                // The `Consumer` in broker side won't be created until the `Consumer` in client side subscribes
                // successfully, so we should wait until offset consumer's future is completed.
                offsetConsumerFuture.whenComplete((ignored, e) -> {
                    if (e != null) {
                        log.warn("Failed to create offset consumer for [group={}] [topic={}]: {}",
                                groupId, kafkaPartition, e.getMessage());
                        offsetAcker.removeConsumer(groupId, kafkaPartition);
                        // Here we don't return because the `Consumer` in broker side may be created already
                    }
                    // Double check for if the `Consumer` in broker side has been created
                    final List<Consumer> consumers =
                            persistentTopic.getSubscriptions().get(groupId).getDispatcher().getConsumers();
                    if (consumers.isEmpty()) {
                        log.error("There's no internal consumer for [group={}]", groupId);
                        consumerFuture.complete(null);
                        return;
                    }
                    // only one consumer existed for internal subscription
                    final Consumer consumer = persistentTopic.getSubscriptions()
                            .get(groupId).getDispatcher().getConsumers().get(0);
                    consumerFuture.complete(consumer);
                });
            } catch (Exception e) {
                log.error("get topic error", e);
                consumerFuture.complete(null);
            }
            return consumerFuture;
        });
    }
}
