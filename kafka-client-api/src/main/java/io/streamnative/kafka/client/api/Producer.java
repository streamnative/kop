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
package io.streamnative.kafka.client.api;

import java.util.concurrent.Future;

/**
 * A common interface of Kafka producer.
 */
public interface Producer<K, V> extends AutoCloseable {

    Future<RecordMetadata> sendAsync(ProduceContext<K, V> context);

    default ProduceContext.ProduceContextBuilder<K, V> newContextBuilder(String topic, V value) {
        return newContextBuilder(topic, value, null);
    }

    default ProduceContext.ProduceContextBuilder<K, V> newContextBuilder(String topic, V value, Integer partition) {
        return ProduceContext.<K, V>builder()
                .producer(this)
                .topic(topic)
                .value(value)
                .partition(partition);
    }
}
