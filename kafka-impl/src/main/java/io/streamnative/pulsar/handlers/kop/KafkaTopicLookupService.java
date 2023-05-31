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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;

/**
 * KafkaTopicLookupManager manages a Map of topic to KafkaTopicConsumerManager.
 * For each topic, there is a KafkaTopicConsumerManager, which manages a topic and its related offset cursor.
 * This is mainly used to cache the produce/consume topic, not include offsetTopic.
 */
@Slf4j
public class KafkaTopicLookupService {
    private final BrokerService brokerService;

    public KafkaTopicLookupService(BrokerService brokerService) {
        this.brokerService = brokerService;
     }

    // A wrapper of `BrokerService#getTopic` that is to find the topic's associated `PersistentTopic` instance
    public CompletableFuture<Optional<PersistentTopic>> getTopic(String topicName, Object requestor) {
        CompletableFuture<Optional<PersistentTopic>> topicCompletableFuture = new CompletableFuture<>();
        brokerService.getTopicIfExists(topicName).whenComplete((t2, throwable) -> {
            TopicName topicNameObject = TopicName.get(topicName);
            if (throwable != null) {
                // Failed to getTopic from current broker, remove cache, which added in getTopicBroker.
                KopBrokerLookupManager.removeTopicManagerCache(topicName);
                if (topicNameObject.getPartitionIndex() == 0) {
                    log.warn("Get partition-0 error [{}].", throwable.getMessage());
                } else {
                    handleGetTopicException(topicName, topicCompletableFuture, throwable, requestor);
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
                            requestor, nonPartitionedTopicName);
                }
                brokerService.getTopicIfExists(nonPartitionedTopicName).whenComplete((nonPartitionedTopic, ex) -> {
                    if (ex != null) {
                        handleGetTopicException(nonPartitionedTopicName, topicCompletableFuture, ex, requestor);
                        // Failed to getTopic from current broker, remove non-partitioned topic cache,
                        // which added in getTopicBroker.
                        KopBrokerLookupManager.removeTopicManagerCache(nonPartitionedTopicName);
                        return;
                    }
                    if (nonPartitionedTopic.isPresent()) {
                        PersistentTopic persistentTopic = (PersistentTopic) nonPartitionedTopic.get();
                        topicCompletableFuture.complete(Optional.of(persistentTopic));
                    } else {
                        log.error("[{}]Get empty non-partitioned topic for name {}",
                                requestor, nonPartitionedTopicName);
                        KopBrokerLookupManager.removeTopicManagerCache(nonPartitionedTopicName);
                        topicCompletableFuture.complete(Optional.empty());
                    }
                });
                return;
            }
            log.error("[{}]Get empty topic for name {}", requestor, topicName);
            KopBrokerLookupManager.removeTopicManagerCache(topicName);
            topicCompletableFuture.complete(Optional.empty());
        });
        return topicCompletableFuture;
    }

    private void handleGetTopicException(@NonNull final String topicName,
                                         @NonNull
                                         final CompletableFuture<Optional<PersistentTopic>> topicCompletableFuture,
                                         @NonNull final Throwable ex,
                                         @NonNull final Object requestor) {
        // The ServiceUnitNotReadyException is retryable, so we should print a warning log instead of error log
        if (ex instanceof BrokerServiceException.ServiceUnitNotReadyException) {
            log.warn("[{}] Failed to getTopic {}: {}",
                    requestor, topicName, ex.getMessage());
            topicCompletableFuture.complete(Optional.empty());
        } else {
            log.error("[{}] Failed to getTopic {}. exception:",
                    requestor, topicName, ex);
            topicCompletableFuture.completeExceptionally(ex);
        }
    }
}
