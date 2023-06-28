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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

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
    private final KafkaTopicLookupService kafkaTopicLookupService;
    private volatile SocketAddress remoteAddress;

    private final InternalServerCnx internalServerCnx;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    KafkaTopicManager(KafkaRequestHandler kafkaRequestHandler) {
        this.requestHandler = kafkaRequestHandler;
        PulsarService pulsarService = kafkaRequestHandler.getPulsarService();
        this.brokerService = pulsarService.getBrokerService();
        this.internalServerCnx = new InternalServerCnx(requestHandler);
        this.lookupClient = kafkaRequestHandler.getLookupClient();
        this.kafkaTopicLookupService = new KafkaTopicLookupService(pulsarService.getBrokerService());
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

    public Optional<Producer> registerProducerInPersistentTopic(String topicName, PersistentTopic persistentTopic) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Failed to registerProducerInPersistentTopic for topic '{}'",
                        requestHandler.ctx.channel(), topicName);
            }
            return Optional.empty();
        }
        return requestHandler
                .getKafkaTopicManagerSharedState()
                .registerProducer(topicName, requestHandler,
                        () -> registerInPersistentTopic(persistentTopic));
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

    public CompletableFuture<Optional<PersistentTopic>> getTopic(String topicName) {
        if (closed.get()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Return null for getTopic({}) since channel is closing",
                        requestHandler.ctx.channel(), topicName);
            }
            return CompletableFuture.completedFuture(Optional.empty());
        }
        CompletableFuture<Optional<PersistentTopic>> topicCompletableFuture =
                kafkaTopicLookupService.getTopic(topicName, requestHandler.ctx.channel());
        return topicCompletableFuture;
    }

    public void invalidateCacheForFencedManagerLedgerOnTopic(String fullTopicName) {
        log.info("Invalidating cache for fenced error on topic {} (maybe topic was deleted)", fullTopicName);
        requestHandler.getKafkaTopicManagerSharedState().deReference(fullTopicName);
    }
}
