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
import java.util.concurrent.atomic.AtomicBoolean;
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

    private final KafkaRequestHandler requestHandler;
    private final BrokerService brokerService;
    private final LookupClient lookupClient;
    private volatile SocketAddress remoteAddress;

    private final InternalServerCnx internalServerCnx;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    KafkaTopicManager(KafkaRequestHandler kafkaRequestHandler) {
        this.requestHandler = kafkaRequestHandler;
        PulsarService pulsarService = kafkaRequestHandler.getPulsarService();
        this.brokerService = pulsarService.getBrokerService();
        this.internalServerCnx = new InternalServerCnx(requestHandler);
        this.lookupClient = KafkaProtocolHandler.getLookupClient(pulsarService);
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
        return requestHandler.getKafkaTopicManagerSharedState().getKafkaTopicConsumerManagerCache().computeIfAbsent(
            topicName,
            remoteAddress,
            () -> {
                final CompletableFuture<KafkaTopicConsumerManager> tcmFuture = new CompletableFuture<>();
                getTopic(topicName).whenComplete((persistentTopic, throwable) -> {
                    if (throwable == null && persistentTopic.isPresent()) {
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
                KopBrokerLookupManager.removeTopicManagerCache(topicName);
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
                        KopBrokerLookupManager.removeTopicManagerCache(nonPartitionedTopicName);
                        return;
                    }
                    if (nonPartitionedTopic.isPresent()) {
                        PersistentTopic persistentTopic = (PersistentTopic) nonPartitionedTopic.get();
                        topicCompletableFuture.complete(Optional.of(persistentTopic));
                    } else {
                        log.error("[{}]Get empty non-partitioned topic for name {}",
                                requestHandler.ctx.channel(), nonPartitionedTopicName);
                        KopBrokerLookupManager.removeTopicManagerCache(nonPartitionedTopicName);
                        topicCompletableFuture.complete(Optional.empty());
                    }
                });
                return;
            }
            log.error("[{}]Get empty topic for name {}", requestHandler.ctx.channel(), topicName);
            KopBrokerLookupManager.removeTopicManagerCache(topicName);
            topicCompletableFuture.complete(Optional.empty());
        });
        // cache for removing producer
        requestHandler.getKafkaTopicManagerSharedState().getTopics().put(topicName, topicCompletableFuture);
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

    public Optional<Producer> registerProducerInPersistentTopic(String topicName, PersistentTopic persistentTopic) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to registerProducerInPersistentTopic for topic '{}'",
                        requestHandler.ctx.channel(), topicName);
            }
            return Optional.empty();
        }
        return Optional.of(requestHandler.getKafkaTopicManagerSharedState()
                .getReferences().computeIfAbsent(topicName, (__) -> registerInPersistentTopic(persistentTopic)));
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

        requestHandler.getKafkaTopicManagerSharedState()
                .handlerKafkaRequestHandlerClosed(remoteAddress, requestHandler);
    }

    public void invalidateCacheForFencedManagerLedgerOnTopic(String fullTopicName) {
        log.info("Invalidating cache for fenced error on topic {} (maybe topic was deleted)", fullTopicName);
        requestHandler.getKafkaTopicManagerSharedState().deReference(fullTopicName);
    }
}
