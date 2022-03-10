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

import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

@Slf4j
public final class KafkaTopicManagerSharedState {

    @Getter
    private final KafkaTopicConsumerManagerCache kafkaTopicConsumerManagerCache =
            new KafkaTopicConsumerManagerCache();

    // every 1 min, check if the KafkaTopicConsumerManagers have expired cursors.
    // remove expired cursors, so backlog can be cleared.
    private static final long checkPeriodMillis = 1 * 60 * 1000;
    private static final long expirePeriodMillis = 2 * 60 * 1000;
    private static volatile ScheduledFuture<?> cursorExpireTask = null;

    // cache for topics: <topicName, persistentTopic>, for removing producer
    @Getter
    private final ConcurrentHashMap<String, CompletableFuture<Optional<PersistentTopic>>>
            topics = new ConcurrentHashMap<>();
    // cache for references in PersistentTopic: <topicName, producer>
    @Getter
    private final ConcurrentHashMap<String, Producer>
            references = new ConcurrentHashMap<>();


    public KafkaTopicManagerSharedState(BrokerService brokerService) {
        initializeCursorExpireTask(brokerService.executor());
    }


    private void initializeCursorExpireTask(final ScheduledExecutorService executor) {
        if (executor == null) {
            // this happens in tests with mock BrokerService.
            return;
        }
        // check expired cursor every 1 min.
        cursorExpireTask = executor.scheduleWithFixedDelay(() -> {
            long current = System.currentTimeMillis();
            kafkaTopicConsumerManagerCache.forEach(future -> {
                if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
                    future.join().deleteExpiredCursor(current, expirePeriodMillis);
                }
            });
        }, checkPeriodMillis, checkPeriodMillis, TimeUnit.MILLISECONDS);
    }

    public void close() {
        cancelCursorExpireTask();
        kafkaTopicConsumerManagerCache.close();
        references.clear();
        topics.clear();
    }

    private void cancelCursorExpireTask() {
        if (cursorExpireTask != null) {
            cursorExpireTask.cancel(true);
            cursorExpireTask = null;
        }
    }


    public Producer getReferenceProducer(String topicName) {
        return references.get(topicName);
    }

    private void removePersistentTopicAndReferenceProducer(final String topicName) {
        // 1. Remove PersistentTopic and Producer from caches, these calls are thread safe
        final CompletableFuture<Optional<PersistentTopic>> topicFuture = topics.remove(topicName);
        final Producer producer = references.remove(topicName);

        if (topicFuture == null) {
            KopBrokerLookupManager.removeTopicManagerCache(topicName);
            return;
        }

        // 2. Remove Producer from PersistentTopic's internal cache
        topicFuture.thenAccept(persistentTopic -> {
            if (producer != null && persistentTopic.isPresent()) {
                try {
                    persistentTopic.get().removeProducer(producer);
                } catch (IllegalArgumentException ignored) {
                    log.error(
                            "[{}] The producer's topic ({}) doesn't match the current PersistentTopic",
                            topicName, (producer.getTopic() == null) ? "null" : producer.getTopic().getName());
                }
            }
        }).exceptionally(e -> {
            log.error("Failed to get topic '{}' in removeTopicAndReferenceProducer", topicName, e);
            return null;
        });
    }

    public void handlerKafkaRequestHandlerClosed(SocketAddress remoteAddress, KafkaRequestHandler requestHandler) {
        try {
            kafkaTopicConsumerManagerCache.removeAndCloseByAddress(remoteAddress);

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

    public void deReference(String topicName) {
        try {
            KopBrokerLookupManager.removeTopicManagerCache(topicName);
            kafkaTopicConsumerManagerCache.removeAndCloseByTopic(topicName);
            removePersistentTopicAndReferenceProducer(topicName);
        } catch (Exception e) {
            log.error("Failed to close reference for individual topic {}. exception:", topicName, e);
        }
    }

}
