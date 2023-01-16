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
import io.streamnative.pulsar.handlers.kop.RequestStats;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.service.plugin.EntryFilterWithClassLoader;

/**
 * Manage {@link PartitionLog}.
 */
@AllArgsConstructor
public class PartitionLogManager {

    private final KafkaServiceConfiguration kafkaConfig;
    private final RequestStats requestStats;
    private final Map<String, PartitionLog> logMap;
    private final Time time;
    private final ImmutableMap<String, EntryFilterWithClassLoader> entryfilterMap;

    public PartitionLogManager(KafkaServiceConfiguration kafkaConfig,
                               RequestStats requestStats,
                               final ImmutableMap<String, EntryFilterWithClassLoader> entryfilterMap,
                               Time time) {
        this.kafkaConfig = kafkaConfig;
        this.requestStats = requestStats;
        this.logMap = Maps.newConcurrentMap();
        this.entryfilterMap = entryfilterMap;
        this.time = time;
    }

    public PartitionLog getLog(TopicPartition topicPartition, String namespacePrefix) {
        String kopTopic = KopTopic.toString(topicPartition, namespacePrefix);

        return logMap.computeIfAbsent(kopTopic, key -> {
                return new PartitionLog(kafkaConfig, requestStats, time, topicPartition, kopTopic, entryfilterMap,
                        new ProducerStateManager(kopTopic));
        });
    }

    public PartitionLog removeLog(String topicName) {
        return logMap.remove(topicName);
    }

    public int size() {
        return logMap.size();
    }
}

