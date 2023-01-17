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

import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class PulsarPartitionedTopicProducerStateManagerSnapshotBuffer implements ProducerStateManagerSnapshotBuffer {

    private final List<ProducerStateManagerSnapshotBuffer> partitions = new ArrayList<>();

    private ProducerStateManagerSnapshotBuffer pickPartition(String topic) {
        return partitions
                .get(TransactionCoordinator.partitionFor(topic, partitions.size()));
    }

    @Override
    public CompletableFuture<Void> write(ProducerStateManagerSnapshot snapshot) {
        return pickPartition(snapshot.getTopicPartition()).write(snapshot);
    }

    @Override
    public CompletableFuture<ProducerStateManagerSnapshot> readLatestSnapshot(String topicPartition) {
        return pickPartition(topicPartition).readLatestSnapshot(topicPartition);
    }

    public PulsarPartitionedTopicProducerStateManagerSnapshotBuffer(String topicName,
                                                                    SystemTopicClient pulsarClient,
                                                                    Executor executor,
                                                                    int numPartitions) {
        TopicName fullName = TopicName.get(topicName);
        for (int i = 0; i < numPartitions; i++) {
            PulsarTopicProducerStateManagerSnapshotBuffer partition =
                    new PulsarTopicProducerStateManagerSnapshotBuffer(
                            fullName.getPartition(i).toString(),
                            pulsarClient, executor);
            partitions.add(partition);
        }
    }

    @Override
    public void shutdown() {
        partitions.forEach(ProducerStateManagerSnapshotBuffer::shutdown);
    }
}
