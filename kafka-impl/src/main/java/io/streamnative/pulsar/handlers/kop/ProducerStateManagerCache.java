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

import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.SystemTopicClientFactory;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatterFactory;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.broker.systopic.SystemTopicClient;


/**
 * Broker producer state manager.
 */
public class ProducerStateManagerCache {

    private final Map<String, ProducerStateManager> producerStateManagerMap;
    private final int maxProducerIdExpirationMs;
    private final SystemTopicClientFactory systemTopicClientFactory;
    private final String format;

    public ProducerStateManagerCache(int maxProducerIdExpirationMs, SystemTopicClientFactory systemTopicClientFactory, String format) {
        producerStateManagerMap = Maps.newConcurrentMap();
        this.maxProducerIdExpirationMs = maxProducerIdExpirationMs;
        this.systemTopicClientFactory = systemTopicClientFactory;
        this.format = format;
    }

    public ProducerStateManager getProducerStateManager(TopicPartition topicPartition) {
        return getProducerStateManager(KopTopic.toString(topicPartition));
    }

    public ProducerStateManager getProducerStateManager(String topicPartition) {
        SystemTopicClient systemTopicClient = systemTopicClientFactory.generateProducerStateClient(topicPartition);
        EntryFormatter formatter = EntryFormatterFactory.create(format);
        return producerStateManagerMap.computeIfAbsent(topicPartition, key ->
                new ProducerStateManager(topicPartition,
                        maxProducerIdExpirationMs,
                        formatter,
                        systemTopicClient.newWriterAsync(),
                        systemTopicClient.newReaderAsync()));
    }

}
