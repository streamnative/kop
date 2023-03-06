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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
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

    // cache for references in PersistentTopic: <topicName-requestHandler, producer>
    private final ConcurrentHashMap<ProducerKey, Producer>
            references = new ConcurrentHashMap<>();

    @AllArgsConstructor
    @EqualsAndHashCode
    private static final class ProducerKey {
        final String topicName;
        final KafkaRequestHandler requestHandler;
    }

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
        references.forEach((key, __) -> {
            // perform cleanup
            Producer producer = references.remove(key);
            if (producer != null) {
                PersistentTopic topic = (PersistentTopic) producer.getTopic();
                topic.removeProducer(producer);
            }
        });
        references.clear();
    }

    private void cancelCursorExpireTask() {
        if (cursorExpireTask != null) {
            cursorExpireTask.cancel(true);
            cursorExpireTask = null;
        }
    }

    public Optional<Producer> registerProducer(String topic, KafkaRequestHandler requestHandler,
                                               Supplier<Producer> supplier) {
        ProducerKey key = new ProducerKey(topic, requestHandler);
        return Optional.ofNullable(references.computeIfAbsent(key,
                (__) -> supplier.get()));
    }

    private void removePersistentTopicAndReferenceProducer(final KafkaRequestHandler producerId) {
        references.forEach((key, __) -> {
            if (key.requestHandler == producerId) {
                Producer producer = references.remove(key);
                if (producer != null) {
                    PersistentTopic topic = (PersistentTopic) producer.getTopic();
                    topic.removeProducer(producer);
                }
            }
        });
    }

    public void handlerKafkaRequestHandlerClosed(SocketAddress remoteAddress, KafkaRequestHandler requestHandler) {
        try {
            kafkaTopicConsumerManagerCache.removeAndCloseByAddress(remoteAddress);
            removePersistentTopicAndReferenceProducer(requestHandler);
        } catch (Exception e) {
            log.error("[{}] Failed to close KafkaTopicManager. exception:",
                    requestHandler.ctx.channel(), e);
        }
    }

    public void deReference(String topicName) {
        try {
            KopBrokerLookupManager.removeTopicManagerCache(topicName);
            kafkaTopicConsumerManagerCache.removeAndCloseByTopic(topicName);
        } catch (Exception e) {
            log.error("Failed to close reference for individual topic {}. exception:", topicName, e);
        }
    }

}
