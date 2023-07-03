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
package io.streamnative.pulsar.handlers.kop.storage;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryProducerStateManagerSnapshotBuffer implements ProducerStateManagerSnapshotBuffer {
    private Map<String, ProducerStateManagerSnapshot> latestSnapshots = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Void> write(ProducerStateManagerSnapshot snapshot) {
        return CompletableFuture.runAsync(() -> {
            latestSnapshots.compute(snapshot.getTopicPartition(), (tp, current) -> {
                if (current == null || current.getOffset() <= snapshot.getOffset()) {
                    return snapshot;
                } else {
                    return current;
                }
            });
        });
    }

    @Override
    public CompletableFuture<ProducerStateManagerSnapshot> readLatestSnapshot(String topicPartition) {
        return CompletableFuture.supplyAsync(() -> latestSnapshots.get(topicPartition));
    }
}
