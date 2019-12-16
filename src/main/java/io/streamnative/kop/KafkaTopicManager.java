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

import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * KafkaTopicManager manages a Map of topic to KafkaTopicConsumerManager.
 * For each topic, there is a KafkaTopicConsumerManager, which manages a topic and its related offset cursor.
 */
@Slf4j
public class KafkaTopicManager {

    private final BrokerService service;

    // consumerTopics for consumers cache.
    @Getter
    private final ConcurrentOpenHashMap<String, CompletableFuture<KafkaTopicConsumerManager>> consumerTopics;

    // cache for topics
    private final ConcurrentOpenHashMap<String, PersistentTopic> topics;

    KafkaTopicManager(BrokerService service) {
        this.service = service;
        consumerTopics = new ConcurrentOpenHashMap<>();
        topics = new ConcurrentOpenHashMap<>();
    }

    // topicName is in pulsar format. e.g. persistent://public/default/topic-partition-0
    public CompletableFuture<KafkaTopicConsumerManager> getTopicConsumerManager(String topicName) {
        return consumerTopics.computeIfAbsent(
            topicName,
            t -> service
                .getTopic(topicName, true)
                .thenApply(t2 -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Call getTopicConsumerManager for {}, and create KafkaTopicConsumerManager.",
                            topicName);
                    }
                    topics.putIfAbsent(topicName, (PersistentTopic) t2.get());
                    return new KafkaTopicConsumerManager((PersistentTopic) t2.get());
                })
                .exceptionally(ex -> {
                    log.error("Failed to getTopicConsumerManager {}. exception:",
                        topicName, ex);
                    return null;
                })
        );
    }

    // whether topic exists or not
    public boolean topicExists(String topicName) {
        return topics.containsKey(topicName);
    }

    public PersistentTopic addTopic(String topicName, PersistentTopic persistentTopic) {
        return topics.putIfAbsent(topicName, persistentTopic);
    }

    public PersistentTopic getTopic(String topicName) {
        return topics.get(topicName);
    }

    public void close() {
        consumerTopics.values()
            .forEach(manager -> manager.join().getConsumers().values()
                .forEach(pair -> {
                    try {
                        pair.join().getLeft().close();
                    } catch (Exception e) {
                        log.error("Failed to close cursor for topic {}. exception:",
                            pair.join().getLeft().getName(), e);
                    }
                }));
    }

}
