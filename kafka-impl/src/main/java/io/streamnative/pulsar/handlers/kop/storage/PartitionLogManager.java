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

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.KafkaTopicLookupService;
import io.streamnative.pulsar.handlers.kop.RequestStats;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Manage {@link PartitionLog}.
 */
@AllArgsConstructor
@Slf4j
public class PartitionLogManager {

    private final Map<String, CompletableFuture<PartitionLog>> logMap = new ConcurrentHashMap<>();
    private final KafkaServiceConfiguration kafkaConfig;
    private final RequestStats requestStats;
    private final Time time;
    private final List<EntryFilter> entryFilters;

    private final KafkaTopicLookupService kafkaTopicLookupService;

    private final Function<String, ProducerStateManagerSnapshotBuffer> producerStateManagerSnapshotBuffer;

    private final OrderedExecutor recoveryExecutor;

    public PartitionLogManager(KafkaServiceConfiguration kafkaConfig,
                               RequestStats requestStats,
                               final List<EntryFilter> entryFilters,
                               Time time,
                               KafkaTopicLookupService kafkaTopicLookupService,
                               Function<String, ProducerStateManagerSnapshotBuffer> producerStateManagerSnapshotBuffer,
                               OrderedExecutor recoveryExecutor) {
        this.kafkaConfig = kafkaConfig;
        this.requestStats = requestStats;
        this.entryFilters = entryFilters;
        this.time = time;
        this.kafkaTopicLookupService = kafkaTopicLookupService;
        this.producerStateManagerSnapshotBuffer = producerStateManagerSnapshotBuffer;
        this.recoveryExecutor = recoveryExecutor;
    }

    public CompletableFuture<PartitionLog> getLog(TopicPartition topicPartition, String namespacePrefix) {
        String kopTopic = KopTopic.toString(topicPartition, namespacePrefix);
        String tenant = TopicName.get(kopTopic).getTenant();
        ProducerStateManagerSnapshotBuffer prodPerTenant = producerStateManagerSnapshotBuffer.apply(tenant);
        return logMap.computeIfAbsent(kopTopic, key -> new PartitionLog(
                kafkaConfig, requestStats, time, topicPartition, key, entryFilters, kafkaTopicLookupService,
                prodPerTenant, recoveryExecutor
        ).initialise());
    }

    CompletableFuture<PartitionLog> removeLog(String topicName) {
        return logMap.remove(topicName);
    }

    @VisibleForTesting
    int size() {
        return logMap.size();
    }

    void updatePurgeAbortedTxnsOffsets() {
        logMap.values().forEach(future -> future.thenApply(PartitionLog::updatePurgeAbortedTxnsOffset));
    }
}

