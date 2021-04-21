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
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;


/**
 * Broker producer state manager.
 */
public class BrokerProducerStateManager {

    private final Map<String, ProducerStateManager> producerStateManagerMap;
    private int maxProducerIdExpirationMs;

    public BrokerProducerStateManager(int maxProducerIdExpirationMs) {
        producerStateManagerMap = Maps.newConcurrentMap();
        this.maxProducerIdExpirationMs = maxProducerIdExpirationMs;
    }

    public ProducerStateManager getProducerStateManager(TopicPartition topicPartition) {
        return getProducerStateManager(KopTopic.toString(topicPartition));
    }

    public ProducerStateManager getProducerStateManager(String topicPartition) {
        return producerStateManagerMap.computeIfAbsent(topicPartition, key ->
                new ProducerStateManager(topicPartition, maxProducerIdExpirationMs));
    }

}
