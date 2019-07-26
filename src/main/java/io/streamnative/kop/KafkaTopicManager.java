package io.streamnative.kop;

import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * KafkaTopicManager manages a Map of topic to KafkaTopicConsumerManager .
 *
 * For each topic, there is a KafkaTopicConsumerManager, which
 */
@Slf4j
public class KafkaTopicManager {

    private final BrokerService service;
    private final ConcurrentOpenHashMap<String, CompletableFuture<KafkaTopicConsumerManager>> topics;

    KafkaTopicManager(BrokerService service) {
        this.service = service;
        topics = new ConcurrentOpenHashMap<>();
    }

    public CompletableFuture<KafkaTopicConsumerManager> getTopic(String topicName) {
        return topics.computeIfAbsent(
            topicName,
            t -> service
                .getTopic(topicName, true)
                .thenApply(t2 -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Call getTopic for {}, and create KafkaTopicConsumerManager.", topicName);
                    }
                    return new KafkaTopicConsumerManager((PersistentTopic) t2.get());
                })
                .exceptionally(ex -> {
                    log.error("Failed to getTopic {}. exception:",
                        topicName, ex);
                    return null;
                })
        );
    }
}
