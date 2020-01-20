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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
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

    private MockServerCnx mockServerCnx;

    KafkaTopicManager(KafkaRequestHandler kafkaRequestHandler) {
        this.requestHandler = kafkaRequestHandler;
        this.pulsarService = kafkaRequestHandler.getPulsarService();
        this.brokerService = pulsarService.getBrokerService();
        this.mockServerCnx = new MockServerCnx(requestHandler);

        consumerTopicManagers = new ConcurrentHashMap<>();
        topics = new ConcurrentHashMap<>();
        references = new ConcurrentHashMap<>();
    }

    // update Ctx information, since at create time there is no ctx passed into kafkaRequestHandler.
    public void updateCtx() {
        mockServerCnx.updateCtx();
        if (log.isDebugEnabled()) {
            log.debug("mockServerCnx.remoteAddress: {}", mockServerCnx.getRemoteAddress());
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

    // whether topic exists in cache.
    public boolean topicExists(String topicName) {
        return topics.containsKey(topicName);
    }

    private Producer registerInPersistentTopic(PersistentTopic persistentTopic) throws Exception {
        Producer producer = new MockProducer(persistentTopic, mockServerCnx,
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

    // this should be the only entrance for getTopic, since we need register topic into PersistentTopic.
    // return null if not owned by this broker.
    public CompletableFuture<PersistentTopic> getTopic(String topicName) {
        return getTopic(topicName, false);
    }

    // TODO: some times we need to lookup, to make sure topic served by brokerService,
    // or will meet error: "Service unit is not ready when loading the topic".
    // If getTopic is called after lookup, then no needLookup.
    public synchronized CompletableFuture<PersistentTopic> getTopic(String topicName, boolean needLookup) {
        return topics.computeIfAbsent(topicName,
            t -> {
                try {
                    CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> broker =
                        ((PulsarClientImpl) pulsarService.getClient()).getLookup().getBroker(TopicName.get(t));

                    final CompletableFuture<PersistentTopic> topicCompletableFuture = new CompletableFuture<>();

                    // TODO: make it more smooth.
                    broker.whenCompleteAsync((tt, th) -> {
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
                        }
                    );
                    return topicCompletableFuture;
                } catch (Exception e) {
                    log.error("Caught error while getclient for topic:{} ", t, e);
                    return null;
                }
            });
    }

    // when channel close, release all the topics reference in persistentTopic
    // TODO: set channel status?
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
                CompletableFuture<PersistentTopic> topicFuture = entry.getValue();
                if (log.isDebugEnabled()) {
                    log.debug("remove producer {} for topic {} at close()",
                        references.get(topicName), topicName);
                }
                topicFuture.get().removeProducer(references.get(topicName));
                references.remove(topicName);
                topics.remove(topicName);
            }
            topics.clear();
        } catch (Exception e) {
            log.error("Failed to close KafkaTopicManager. exception:", e);
        }
    }

    public void deReference(String topicName) {
        try {
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
