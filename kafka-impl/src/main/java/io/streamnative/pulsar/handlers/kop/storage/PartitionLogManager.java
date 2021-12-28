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
import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import io.streamnative.pulsar.handlers.kop.systopic.SystemTopicClientFactory;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.timer.SystemTimer;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

/**
 * Manage {@link PartitionLog}.
 */
@Slf4j
@AllArgsConstructor
public class PartitionLogManager {

    private final KafkaServiceConfiguration kafkaConfig;
    private final SystemTopicClientFactory systemTopicClientFactory;
    private final Map<String, PartitionLog> logMap;
    private final EntryFormatter formatter;
    private final Time time;
    private ScheduledExecutorService recoveryExecutor;

    public PartitionLogManager(KafkaServiceConfiguration kafkaConfig,
                               EntryFormatter entryFormatter,
                               SystemTopicClientFactory systemTopicClientFactory,
                               Time time) {
        this.kafkaConfig = kafkaConfig;
        this.systemTopicClientFactory = systemTopicClientFactory;
        this.logMap = Maps.newConcurrentMap();
        this.formatter = entryFormatter;
        // TODO: move out
        this.recoveryExecutor = Executors.newScheduledThreadPool(
                Runtime.getRuntime().availableProcessors(),
                new DefaultThreadFactory("producer-state-recovery"));
        this.time = time;
    }

    public PartitionLog getLog(TopicPartition topicPartition,
                               String namespacePrefix,
                               SystemTimer timer) {
        String kopTopic = KopTopic.toString(topicPartition, namespacePrefix);

        return logMap.computeIfAbsent(kopTopic, key ->
                new PartitionLog(kafkaConfig, time, topicPartition, namespacePrefix, kopTopic, formatter,
                        new ProducerStateManager(kopTopic,
                                kafkaConfig.getMaxProducerIdExpirationMs(),
                                systemTopicClientFactory.getProducerStateClient(kopTopic),
                                time,
                                timer,
                                kafkaConfig.getKafkaProducerStateSnapshotMinTimeInMillis()),
                        recoveryExecutor));
    }

    public void removeLog(String fullTopicName) {
        if (log.isDebugEnabled()) {
            log.debug("WWWW Remove partition log [{}]", fullTopicName);
        }
        logMap.remove(fullTopicName);
    }
}

