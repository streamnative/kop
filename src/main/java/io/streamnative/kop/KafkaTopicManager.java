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
package io.streamnative.kop;

import static com.google.common.base.Preconditions.checkState;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.BackoffBuilder;
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

    public static final ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>>
        lookupCache = new ConcurrentHashMap<>();

    KafkaTopicManager(KafkaRequestHandler kafkaRequestHandler) {
        this.requestHandler = kafkaRequestHandler;
        this.pulsarService = kafkaRequestHandler.getPulsarService();
        this.brokerService = pulsarService.getBrokerService();
        this.internalServerCnx = new InternalServerCnx(requestHandler);

        consumerTopicManagers = new ConcurrentHashMap<>();
        topics = new ConcurrentHashMap<>();
        references = new ConcurrentHashMap<>();
    }

    // update Ctx information, since at create time there is no ctx passed into kafkaRequestHandler.
    public void updateCtx() {
        internalServerCnx.updateCtx();
        if (log.isDebugEnabled()) {
            log.debug("internalServerCnx.remoteAddress: {}", internalServerCnx.getRemoteAddress());
        }
    }

    // topicName is in pulsar format. e.g. persistent://public/default/topic-partition-0
    // return null if topic not owned by this broker
    public CompletableFuture<KafkaTopicConsumerManager> getTopicConsumerManager(String topicName) {
        return consumerTopicManagers.computeIfAbsent(
            topicName,
            t -> {
                CompletableFuture<PersistentTopic> topic = getTopic(t);
                if (topic == null) {
                    log.warn("Failed to getTopicConsumerManager for topic {}. return null", t);
                    return null;
                }

                return topic.thenApply(t2 -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Call getTopicConsumerManager for {}, and create KafkaTopicConsumerManager.", t);
                    }
                    // return consumer manager
                    return new KafkaTopicConsumerManager(t2);
                }).exceptionally(ex -> {
                    log.error("Failed to getTopicConsumerManager {}. exception:",
                        t, ex);
                    return null;
                });
            }
        );
    }

    public void removeLookupCache(String topicName) {
        lookupCache.remove(topicName);
    }

    // whether topic exists in cache.
    public boolean topicExists(String topicName) {
        return topics.containsKey(topicName);
    }

    private Producer registerInPersistentTopic(PersistentTopic persistentTopic) throws Exception {
        Producer producer = new InternalProducer(persistentTopic, internalServerCnx,
            ((PulsarClientImpl) (pulsarService.getClient())).newRequestId(),
            brokerService.generateUniqueProducerName());

        if (log.isDebugEnabled()) {
            log.debug("Register Mock Producer {} into PersistentTopic {}",
                producer, persistentTopic.getName());
        }

        // this will register and add USAGE_COUNT_UPDATER.
        persistentTopic.addProducer(producer);
        return producer;
    }

    // call pulsarclient.lookup.getbroker to get and own a topic
    public CompletableFuture<InetSocketAddress> getTopicBroker(String topicName) {
        return lookupCache.computeIfAbsent(topicName, t -> {
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

    private void lookupBroker(String topicName,
                              Backoff backoff,
                              CompletableFuture<InetSocketAddress> retFuture) {
        try {
            ((PulsarClientImpl) pulsarService.getClient()).getLookup()
                .getBroker(TopicName.get(topicName))
                .thenAccept(pair -> {
                    checkState(pair.getLeft().equals(pair.getRight()));
                    retFuture.complete(pair.getLeft());
                })
                .exceptionally(th -> {
                    long waitTimeMs = backoff.next();

                    if (backoff.isMandatoryStopMade()) {
                        log.warn("[{}] getBroker for topic failed, retried too many times, return null. throwable: ",
                            topicName, waitTimeMs, th);
                        retFuture.complete(null);
                    } else {
                        log.warn("[{}] getBroker for topic failed, will retry in {} ms. throwable: ",
                            topicName, waitTimeMs, th);
                        requestHandler.getPulsarService().getExecutor()
                            .schedule(() -> lookupBroker(topicName, backoff, retFuture), waitTimeMs, TimeUnit.MILLISECONDS);
                    }
                    return null;
                });
        } catch (PulsarServerException e) {
            log.error("[{}] getTopicBroker for topic failed get pulsar client, return null. throwable: ",
                topicName, e);
            retFuture.complete(null);
        }
    }

    // For Produce/Consume we need to lookup, to make sure topic served by brokerService,
    // or will meet error: "Service unit is not ready when loading the topic".
    // If getTopic is called after lookup, then no needLookup.
    public CompletableFuture<PersistentTopic> getTopic(String topicName) {
        return topics.computeIfAbsent(topicName,
            t -> {
                final CompletableFuture<PersistentTopic> topicCompletableFuture = new CompletableFuture<>();

                getTopicBroker(t).whenCompleteAsync((ignore, th) -> {
                    if (th != null || ignore == null) {
                        log.warn("[{}] failed getTopicBroker, return null PersistentTopic. throwable: ",
                            topicName, th);
                        topicCompletableFuture.complete(null);
                        return;
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("getTopicBroker for {} in KafkaTopicManager. brokerAddress: {}",
                            t, ignore);
                    }

                    brokerService
                        .getTopic(t, true)
                        .thenApply(t2 -> {
                            if (log.isDebugEnabled()) {
                                log.debug("GetTopic for {} in KafkaTopicManager", t);
                            }

                            try {
                                if (t2.isPresent()) {
                                    PersistentTopic persistentTopic = (PersistentTopic) t2.get();
                                    references.putIfAbsent(t, registerInPersistentTopic(persistentTopic));
                                    topicCompletableFuture.complete(persistentTopic);
                                } else {
                                    log.error("Get empty topic for name {}", t);
                                    topicCompletableFuture.complete(null);
                                }
                            } catch (Exception e) {
                                log.error("Failed to registerInPersistentTopic {}. exception:",
                                    t, e);
                                topicCompletableFuture.complete(null);
                            }

                            return null;
                        })
                        .exceptionally(ex -> {
                            log.error("Failed to getTopic {}. exception:",
                                t, ex);
                            topicCompletableFuture.complete(null);
                            return null;
                        });

                });
                return topicCompletableFuture;
            });
    }

    // when channel close, release all the topics reference in persistentTopic
    public synchronized void close() {
        try {
            for (CompletableFuture<KafkaTopicConsumerManager> manager : consumerTopicManagers.values()) {
                manager.get().getConsumers().values()
                    .forEach(pair -> {
                        try {
                            pair.get().getLeft().close();
                        } catch (Exception e) {
                            log.error("Failed to close cursor for topic {}. exception:",
                                pair.join().getLeft().getName(), e);
                        }
                    });
            }
            consumerTopicManagers.clear();

            for (Map.Entry<String, CompletableFuture<PersistentTopic>> entry : topics.entrySet()) {
                String topicName = entry.getKey();
                lookupCache.remove(topicName);
                CompletableFuture<PersistentTopic> topicFuture = entry.getValue();
                if (log.isDebugEnabled()) {
                    log.debug("remove producer {} for topic {} at close()",
                        references.get(topicName), topicName);
                }
                if (references.get(topicName) != null) {
                    topicFuture.get().removeProducer(references.get(topicName));
                    references.remove(topicName);
                }
                topics.remove(topicName);
            }
            topics.clear();
        } catch (Exception e) {
            log.error("Failed to close KafkaTopicManager. exception:", e);
        }
    }

    public void deReference(String topicName) {
        try {
            lookupCache.remove(topicName);

            if (!consumerTopicManagers.containsKey(topicName)) {
                return;
            }

            consumerTopicManagers.get(topicName).get().getConsumers().values().forEach(pair -> {
                try {
                    pair.join().getLeft().close();
                    consumerTopicManagers.remove(topicName);
                } catch (Exception e) {
                    log.error("Failed to close cursor for individual topic {}. exception:",
                        topicName, e);
                }
            });

            if (!topics.containsKey(topicName)) {
                return;
            }

            topics.get(topicName).get().removeProducer(references.get(topicName));
            topics.remove(topicName);
        } catch (Exception e) {
            log.error("Failed to close reference for individual topic {}. exception:",
                topicName, e);
        }
    }

}
