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
package io.streamnative.pulsar.handlers.kop.idempotent;

import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatterFactory;
import io.streamnative.pulsar.handlers.kop.systopic.SystemTopicClientFactory;
import io.streamnative.pulsar.handlers.kop.systopic.SystemTopicProducerStateClient;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;


@AllArgsConstructor
public class LogManager {

    private final Map<String, Log> logMap;
    private final SystemTopicClientFactory systemTopicClientFactory;
    private final KafkaServiceConfiguration config;
    private final EntryFormatter formatter;
    private final Time time;

    public LogManager(KafkaServiceConfiguration config,
                      SystemTopicClientFactory systemTopicClientFactory,
                      Time time) {
        this.logMap = Maps.newConcurrentMap();
        this.formatter = EntryFormatterFactory.create(config);
        this.config = config;
        this.systemTopicClientFactory = systemTopicClientFactory;
        this.time = time;
    }

    public Log getLog(TopicPartition topicPartition, String namespacePrefix) {
        String kopTopic = KopTopic.toString(topicPartition, namespacePrefix);
        SystemTopicProducerStateClient systemTopicClient =
                systemTopicClientFactory.getProducerStateClient(kopTopic);

        return logMap.computeIfAbsent(kopTopic, key ->
                new Log(new ProducerStateManager(
                        kopTopic,
                        config.getMaxProducerIdExpirationMs(),
                        formatter,
                        systemTopicClient,
                        time))
                );
    }
}
