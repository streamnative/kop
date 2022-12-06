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

import java.util.concurrent.CompletableFuture;

/**
 * Stores snapshots of the state of ProducerStateManagers.
 * One ProducerStateManagerSnapshotBuffer handles all the topics for a Tenant.
 */
public interface ProducerStateManagerSnapshotBuffer {

    /**
     * Writes a snapshot to the storage.
     * @param snapshot
     * @return a handle to the operation
     */
    CompletableFuture<Void> write(ProducerStateManagerSnapshot snapshot);

    /**
     * Reads the latest available snapshot for a given partition.
     * @param topicPartition
     * @return
     */
    CompletableFuture<ProducerStateManagerSnapshot> readLatestSnapshot(String topicPartition);

    /**
     * Shutdown and release resources.
     */
    default void shutdown() {}

}
