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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.KafkaTopicLookupService;
import io.streamnative.pulsar.handlers.kop.RequestStats;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.service.plugin.EntryFilterWithClassLoader;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Manage {@link PartitionLog}.
 */
@AllArgsConstructor
@Slf4j
public class PartitionLogManager {

    private final KafkaServiceConfiguration kafkaConfig;
    private final RequestStats requestStats;
    private final Map<String, CompletableFuture<PartitionLog>> logMap;
    private final Time time;
    private final ImmutableMap<String, EntryFilterWithClassLoader> entryfilterMap;

    private final KafkaTopicLookupService kafkaTopicLookupService;

    private final Function<String, ProducerStateManagerSnapshotBuffer> producerStateManagerSnapshotBuffer;

    private final OrderedExecutor recoveryExecutor;

    public PartitionLogManager(KafkaServiceConfiguration kafkaConfig,
                               RequestStats requestStats,
                               final ImmutableMap<String, EntryFilterWithClassLoader> entryfilterMap,
                               Time time,
                               KafkaTopicLookupService kafkaTopicLookupService,
                               Function<String, ProducerStateManagerSnapshotBuffer> producerStateManagerSnapshotBuffer,
                               OrderedExecutor recoveryExecutor) {
        this.kafkaConfig = kafkaConfig;
        this.requestStats = requestStats;
        this.logMap = Maps.newConcurrentMap();
        this.entryfilterMap = entryfilterMap;
        this.time = time;
        this.kafkaTopicLookupService = kafkaTopicLookupService;
        this.producerStateManagerSnapshotBuffer = producerStateManagerSnapshotBuffer;
        this.recoveryExecutor = recoveryExecutor;
    }

    public CompletableFuture<PartitionLog> getLog(TopicPartition topicPartition, String namespacePrefix) {
        String kopTopic = KopTopic.toString(topicPartition, namespacePrefix);
        String tenant = TopicName.get(kopTopic).getTenant();
        ProducerStateManagerSnapshotBuffer prodPerTenant = producerStateManagerSnapshotBuffer.apply(tenant);
        CompletableFuture<PartitionLog> res =  logMap.computeIfAbsent(kopTopic, key -> {
            log.info("init getLog {}", key);
            CompletableFuture<PartitionLog> result = new PartitionLog(kafkaConfig, requestStats,
                    time, topicPartition, key, entryfilterMap,
                    kafkaTopicLookupService,
                    prodPerTenant)
                    .initialise(recoveryExecutor);

            result.whenComplete((___, error) -> {
                if (error != null) {
                    // in case of failure we have to remove the CompletableFuture from the map
                    log.error("Recovery of {} failed", key, error);
                    logMap.remove(key, result);
                }
            });

            return result;
        });
        if (res.isCompletedExceptionally()) {
            logMap.remove(kopTopic, res);
        }
        return res;
    }

    public CompletableFuture<PartitionLog> removeLog(String topicName) {
        log.info("removePartitionLog {}", topicName);
        return logMap.remove(topicName);
    }

    public int size() {
        return logMap.size();
    }

    public CompletableFuture<Void> takeProducerStateSnapshots() {
        List<CompletableFuture<Void>> handles = new ArrayList<>();
        logMap.values().forEach(log -> {
            if (log.isDone() && !log.isCompletedExceptionally()) {
                PartitionLog partitionLog = log.getNow(null);
                if (partitionLog != null) {
                    handles.add(partitionLog
                            .getProducerStateManager()
                            .takeSnapshot(recoveryExecutor)
                            .thenApply(___ -> null));
                }
            }
        });
        return FutureUtil.waitForAll(handles);
    }

    public CompletableFuture<?> purgeAbortedTxns() {
        List<CompletableFuture<Long>> handles = new ArrayList<>();
        logMap.values().forEach(log -> {
            if (log.isDone() && !log.isCompletedExceptionally()) {
                PartitionLog partitionLog = log.getNow(null);
                if (partitionLog != null) {
                    handles.add(partitionLog
                            .purgeAbortedTxns());
                }
            }
        });
        return FutureUtil
                .waitForAll(handles);
    }
}

