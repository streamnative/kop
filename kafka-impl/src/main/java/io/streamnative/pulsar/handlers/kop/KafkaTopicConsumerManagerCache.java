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
import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * The cache for {@link KafkaTopicConsumerManager}, aka TCM.
 */
@Slf4j
public class KafkaTopicConsumerManagerCache {

    // The 1st key is the full topic name, the 2nd key is the remote address of Kafka client.
    // Because a topic could have multiple connected consumers, for different consumers we should maintain different
    // KafkaTopicConsumerManagers, which are responsible for maintaining the cursors.
    private final Map<String, Map<SocketAddress, CompletableFuture<KafkaTopicConsumerManager>>>
            cache = new ConcurrentHashMap<>();

    public KafkaTopicConsumerManagerCache() {
        // No ops
    }

    public CompletableFuture<KafkaTopicConsumerManager> computeIfAbsent(
            final String fullTopicName,
            final SocketAddress remoteAddress,
            final Supplier<CompletableFuture<KafkaTopicConsumerManager>> mappingFunction) {
        return cache.computeIfAbsent(fullTopicName, ignored -> new ConcurrentHashMap<>())
                .computeIfAbsent(remoteAddress, ignored -> mappingFunction.get());
    }

    public void forEach(final Consumer<CompletableFuture<KafkaTopicConsumerManager>> action) {
        cache.values().forEach(internalMap -> {
            internalMap.values().forEach(action);
        });
    }

    private static void closeTcmFuture(final CompletableFuture<KafkaTopicConsumerManager> tcmFuture) {
        // Use thenAccept to avoid blocking
        tcmFuture.thenAccept(tcm -> {
            if (tcm != null) {
                tcm.close();
            }
        });
    }

    public void removeAndCloseByTopic(final String fullTopicName) {
        Optional.ofNullable(cache.remove(fullTopicName)).ifPresent(map ->
                map.forEach((remoteAddress, future) -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}][{}] Remove and close TCM", fullTopicName, remoteAddress);
                    }
                    closeTcmFuture(future);
                }));
    }

    public void removeAndCloseByAddress(final SocketAddress remoteAddress) {
        cache.forEach((fullTopicName, internalMap) -> {
            Optional.ofNullable(internalMap.remove(remoteAddress)).ifPresent(future -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] Remove and close TCM", fullTopicName, remoteAddress);
                }
                closeTcmFuture(future);
            });
        });
    }

    public void close() {
        cache.forEach((fullTopicName, internalMap) -> {
            internalMap.forEach((remoteAddress, future) -> {
                try {
                    Optional.ofNullable(future.get(100, TimeUnit.MILLISECONDS))
                            .ifPresent(KafkaTopicConsumerManager::close);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    log.warn("[{}][{}] Failed to get TCM future when trying to close it", fullTopicName, remoteAddress);
                }
            });
        });
    }

    @VisibleForTesting
    public void clear() {
        if (!cache.isEmpty()) {
            log.error("Clearing cache, contents {}",
                    cache, new Exception("TCM cache is not empty! " + cache).fillInStackTrace());
            cache.clear();
        }


    }

    @VisibleForTesting
    public int getCount() {
        final AtomicInteger count = new AtomicInteger(0);
        forEach(ignored -> count.incrementAndGet());
        return count.get();
    }

    @VisibleForTesting
    public @NonNull List<KafkaTopicConsumerManager> getTopicConsumerManagers(final String fullTopicName) {
        return cache.getOrDefault(fullTopicName, Collections.emptyMap()).values().stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
    }
}
