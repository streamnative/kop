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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.PulsarClientImpl;
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
    @Getter
    private final ConcurrentHashMap<String, CompletableFuture<KafkaTopicConsumerManager>> consumerTopicManagers;

    // cache for topics: <topicName, persistentTopic>
    private final ConcurrentHashMap<String, CompletableFuture<PersistentTopic>> topics;
    // cache for references in PersistentTopic: <topicName, producer>
    private final ConcurrentHashMap<String, Producer> references;

    private InternalServerCnx internalServerCnx;

    // every 1 min, check if the KafkaTopicConsumerManagers have expired cursors.
    // remove expired cursors, so backlog can be cleared.
    private long checkPeriodMillis = 1 * 60 * 1000;
    private long expirePeriodMillis = 2 * 60 * 1000;
    private final ScheduledFuture<?> cursorExpireTask;

    // the lock for closed status change.
    private final ReentrantReadWriteLock rwLock;
    private boolean closed;

    public static final ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>>
        LOOKUP_CACHE = new ConcurrentHashMap<>();

    public static final ConcurrentHashMap<String, CompletableFuture<Optional<String>>>
            KOP_ADDRESS_CACHE = new ConcurrentHashMap<>();

    KafkaTopicManager(KafkaRequestHandler kafkaRequestHandler) {
        this.requestHandler = kafkaRequestHandler;
        this.pulsarService = kafkaRequestHandler.getPulsarService();
        this.brokerService = pulsarService.getBrokerService();
        this.internalServerCnx = new InternalServerCnx(requestHandler);

        consumerTopicManagers = new ConcurrentHashMap<>();
        topics = new ConcurrentHashMap<>();
        references = new ConcurrentHashMap<>();

        this.rwLock = new ReentrantReadWriteLock();
        this.closed = false;

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
        return consumerTopicManagers.computeIfAbsent(
            topicName,
            t -> {
                CompletableFuture<PersistentTopic> topic = getTopic(t);
                checkState(topic != null);

                return topic.thenApply(t2 -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Call getTopicConsumerManager for {}, and create TCM for {}.",
                            requestHandler.ctx.channel(), topicName, t2);
                    }

                    if (t2 == null) {
                        return null;
                    }
                    // return consumer manager
                    return new KafkaTopicConsumerManager(requestHandler, t2);
                });
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

    // whether topic exists in cache.
    public boolean topicExists(String topicName) {
        return topics.containsKey(topicName);
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
        persistentTopic.addProducer(producer);
        return producer;
    }

    // call pulsarclient.lookup.getbroker to get and own a topic.
    // when error happens, the returned future will complete with null.
    public CompletableFuture<InetSocketAddress> getTopicBroker(String topicName) {

        rwLock.readLock().lock();
        try {
            if (closed) {
                CompletableFuture<InetSocketAddress> retFuture = new CompletableFuture<>();
                retFuture.complete(null);
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Return null for getTopicBroker({}) since channel closing",
                        requestHandler.ctx.channel(), topicName);
                }
                return retFuture;
            }
        } finally {
            rwLock.readLock().unlock();
        }

        return LOOKUP_CACHE.computeIfAbsent(topicName, t -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] topic {} not in Lookup_cache, call lookupBroker",
                    requestHandler.ctx.channel(), topicName);
            }
            CompletableFuture<InetSocketAddress> returnFuture = new CompletableFuture<>();
            Backoff backoff = new Backoff(
                100, TimeUnit.MILLISECONDS,
                30, TimeUnit.SECONDS,
                30, TimeUnit.SECONDS
                );
            lookupBroker(topicName, backoff, returnFuture);
            return returnFuture;
        });
    }

    public InternalServerCnx getInternalServerCnx() {
        return internalServerCnx;
    }

    // this method do the real lookup into Pulsar broker.
    // retFuture will be completed with null when meet error.
    private void lookupBroker(String topicName,
                              Backoff backoff,
                              CompletableFuture<InetSocketAddress> retFuture) {
        try {
            // If channel is closing, complete a null to avoid brings in further handling.
            rwLock.readLock().lock();
            try {
                if (closed) {
                    retFuture.complete(null);
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Return null for getTopic({}) since channel closing",
                            requestHandler.ctx.channel(), topicName);
                    }
                    return;
                }
            } finally {
                rwLock.readLock().unlock();
            }

            ((PulsarClientImpl) pulsarService.getClient()).getLookup()
                .getBroker(TopicName.get(topicName))
                .thenAccept(pair -> {
                    checkState(pair.getLeft().equals(pair.getRight()));
                    retFuture.complete(pair.getLeft());
                })
                .exceptionally(th -> {
                    long waitTimeMs = backoff.next();

                    if (backoff.isMandatoryStopMade()) {
                        log.warn("[{}] getBroker for topic {} failed, retried too many times {}, return null."
                                        + " throwable: ", requestHandler.ctx.channel(), topicName, waitTimeMs, th);
                        retFuture.complete(null);
                    } else {
                        log.warn("[{}] getBroker for topic failed, will retry in {} ms. throwable: ",
                            topicName, waitTimeMs, th);
                        requestHandler.getPulsarService().getExecutor()
                            .schedule(() -> lookupBroker(topicName, backoff, retFuture),
                                waitTimeMs,
                                TimeUnit.MILLISECONDS);
                    }
                    return null;
                });
        } catch (PulsarServerException e) {
            log.error("[{}] getTopicBroker for topic {} failed get pulsar client, return null. throwable: ",
                requestHandler.ctx.channel(), topicName, e);
            retFuture.complete(null);
        }
    }

    // For Produce/Consume we need to lookup, to make sure topic served by brokerService,
    // or will meet error: "Service unit is not ready when loading the topic".
    // If getTopic is called after lookup, then no needLookup.
    // Returned Future wil complete with null when meet error.
    public CompletableFuture<PersistentTopic> getTopic(String topicName) {
        CompletableFuture<PersistentTopic> topicCompletableFuture = new CompletableFuture<>();

        // If channel is closing, complete a null to avoid brings in further handling.
        rwLock.readLock().lock();
        try {
            if (closed) {
                topicCompletableFuture.complete(null);
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Return null for getTopic({}) since channel is closing",
                        requestHandler.ctx.channel(), topicName);
                }
                return topicCompletableFuture;
            }
        } finally {
            rwLock.readLock().unlock();
        }

        return topics.computeIfAbsent(topicName, t -> {
            getTopicBroker(t).whenCompleteAsync((ignore, th) -> {
                if (th != null || ignore == null) {
                    log.warn("[{}] Failed getTopicBroker {}, return null PersistentTopic. throwable: ",
                            requestHandler.ctx.channel(), t, th);

                    // get topic broker returns null. topic should be removed from LookupCache.
                    if (ignore == null) {
                        removeTopicManagerCache(topicName);
                    }

                    topicCompletableFuture.complete(null);
                    return;
                }

                if (log.isDebugEnabled()) {
                    log.debug("[{}] getTopicBroker for {} in KafkaTopicManager. brokerAddress: {}",
                            requestHandler.ctx.channel(), t, ignore);
                }

                brokerService.getTopic(t, brokerService.isAllowAutoTopicCreation(t)).whenComplete((t2, throwable) -> {
                    if (throwable != null) {
                        log.error("[{}] Failed to getTopic {}. exception:",
                                requestHandler.ctx.channel(), t, throwable);
                        // failed to getTopic from current broker, remove cache, which added in getTopicBroker.
                        removeTopicManagerCache(t);
                        topicCompletableFuture.complete(null);
                        return;
                    }
                    if (t2.isPresent()) {
                        PersistentTopic persistentTopic = (PersistentTopic) t2.get();
                        topicCompletableFuture.complete(persistentTopic);
                    } else {
                        log.error("[{}]Get empty topic for name {}",
                                requestHandler.ctx.channel(), t);
                        topicCompletableFuture.complete(null);
                    }
                });
            });
            return topicCompletableFuture;
        });
    }

    public void registerProducerInPersistentTopic (String topicName, PersistentTopic persistentTopic) {
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
    public synchronized void close() {
        rwLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (log.isDebugEnabled()) {
                log.debug("[{}] Closing TopicManager",
                    requestHandler.ctx.channel());
            }
        } finally {
            rwLock.writeLock().unlock();
        }

        try {
            this.cursorExpireTask.cancel(true);

            for (CompletableFuture<KafkaTopicConsumerManager> manager : consumerTopicManagers.values()) {
                manager.get().close();
            }
            consumerTopicManagers.clear();

            for (Map.Entry<String, CompletableFuture<PersistentTopic>> entry : topics.entrySet()) {
                String topicName = entry.getKey();
                removeTopicManagerCache(topicName);
                CompletableFuture<PersistentTopic> topicFuture = entry.getValue();
                if (log.isDebugEnabled()) {
                    log.debug("[{}] remove producer {} for topic {} at close()",
                        requestHandler.ctx.channel(), references.get(topicName), topicName);
                }
                if (references.get(topicName) != null) {
                    topicFuture.get().removeProducer(references.get(topicName));
                    references.remove(topicName);
                }
                topics.remove(topicName);
            }
            topics.clear();
        } catch (Exception e) {
            log.error("[{}] Failed to close KafkaTopicManager. exception:",
                requestHandler.ctx.channel(), e);
        }
    }

    public Producer getReferenceProducer(String topicName) {
        return references.get(topicName);
    }

    public void deReference(String topicName) {
        try {
            removeTopicManagerCache(topicName);

            if (consumerTopicManagers.containsKey(topicName)) {
                CompletableFuture<KafkaTopicConsumerManager> manager = consumerTopicManagers.get(topicName);
                manager.get().close();
                consumerTopicManagers.remove(topicName);
            }

            if (!topics.containsKey(topicName)) {
                return;
            }
            topics.get(topicName).get().removeProducer(references.get(topicName));
            topics.remove(topicName);
        } catch (Exception e) {
            log.error("[{}] Failed to close reference for individual topic {}. exception:",
                requestHandler.ctx.channel(), topicName, e);
        }
    }

}
