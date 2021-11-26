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

import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

/**
 * Manage {@link PartitionLog}.
 */
@AllArgsConstructor
public class PartitionLogManager {

    private KafkaServiceConfiguration kafkaConfig;
    private final Map<String, PartitionLog> logMap;
    private final Optional<TransactionCoordinator> transactionCoordinator;
    private final EntryFormatter formatter;
    private final Time time;

    public PartitionLogManager(KafkaServiceConfiguration kafkaConfig,
                               EntryFormatter entryFormatter,
                               Optional<TransactionCoordinator> transactionCoordinator,
                               Time time) {
        this.kafkaConfig = kafkaConfig;
        this.logMap = Maps.newConcurrentMap();
        this.transactionCoordinator = transactionCoordinator;
        this.formatter = entryFormatter;
        this.time = time;
    }

    public PartitionLog getLog(TopicPartition topicPartition, String namespacePrefix) {
        String kopTopic = KopTopic.toString(topicPartition, namespacePrefix);
        return logMap.computeIfAbsent(kopTopic, key ->
                new PartitionLog(kafkaConfig, time, topicPartition, namespacePrefix, kopTopic, formatter,
                        this.transactionCoordinator)
        );
    }
}

