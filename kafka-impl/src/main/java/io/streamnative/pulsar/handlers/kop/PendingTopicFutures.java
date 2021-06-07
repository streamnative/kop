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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.NonNull;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * Pending futures of PersistentTopic.
 * It's used when multiple produce requests encountered while the partition's PersistentTopic was not available.
 */
public class PendingTopicFutures {

    private final RequestStats requestStats;
    private final long enqueueTimestamp;
    private final AtomicInteger count = new AtomicInteger(0);
    private CompletableFuture<TopicThrowablePair> currentTopicFuture;

    public PendingTopicFutures(RequestStats requestStats) {
        this.requestStats = requestStats;
        this.enqueueTimestamp = MathUtils.nowInNano();
    }

    private void registerQueueLatency(boolean success) {
        if (requestStats != null) {
            if (success) {
                requestStats.getMessageQueuedLatencyStats().registerSuccessfulEvent(
                        MathUtils.elapsedNanos(enqueueTimestamp), TimeUnit.NANOSECONDS);
            } else {
                requestStats.getMessageQueuedLatencyStats().registerFailedEvent(
                        MathUtils.elapsedNanos(enqueueTimestamp), TimeUnit.NANOSECONDS);
            }
        }
    }

    public void addListener(CompletableFuture<PersistentTopic> topicFuture,
                            @NonNull Consumer<PersistentTopic> persistentTopicConsumer,
                            @NonNull Consumer<Throwable> exceptionConsumer) {
        if (count.compareAndSet(0, 1)) {
            // The first pending future comes
            currentTopicFuture = topicFuture.thenApply(persistentTopic -> {
                registerQueueLatency(true);
                persistentTopicConsumer.accept(persistentTopic);
                count.decrementAndGet();
                return TopicThrowablePair.withTopic(persistentTopic);
            }).exceptionally(e -> {
                registerQueueLatency(false);
                exceptionConsumer.accept(e.getCause());
                count.decrementAndGet();
                return TopicThrowablePair.withThrowable(e.getCause());
            });
        } else {
            // The next pending future reuses the completed result of the previous topic future
            currentTopicFuture = currentTopicFuture.thenApply(topicThrowablePair -> {
                if (topicThrowablePair.getThrowable() == null) {
                    registerQueueLatency(true);
                    persistentTopicConsumer.accept(topicThrowablePair.getPersistentTopic());
                } else {
                    registerQueueLatency(false);
                    exceptionConsumer.accept(topicThrowablePair.getThrowable());
                }
                count.decrementAndGet();
                return topicThrowablePair;
            }).exceptionally(e -> {
                registerQueueLatency(false);
                exceptionConsumer.accept(e.getCause());
                count.decrementAndGet();
                return TopicThrowablePair.withThrowable(e.getCause());
            });
            count.incrementAndGet();
        }
    }

    @VisibleForTesting
    public int waitAndGetSize() throws ExecutionException, InterruptedException {
        currentTopicFuture.get();
        return count.get();
    }

    @VisibleForTesting
    public int size() {
        return count.get();
    }
}

class TopicThrowablePair {
    @Getter
    private final PersistentTopic persistentTopic;
    @Getter
    private final Throwable throwable;

    public static TopicThrowablePair withTopic(final PersistentTopic persistentTopic) {
        return new TopicThrowablePair(persistentTopic, null);
    }

    public static TopicThrowablePair withThrowable(final Throwable throwable) {
        return new TopicThrowablePair(null, throwable);
    }

    private TopicThrowablePair(final PersistentTopic persistentTopic, final Throwable throwable) {
        this.persistentTopic = persistentTopic;
        this.throwable = throwable;
    }
};
