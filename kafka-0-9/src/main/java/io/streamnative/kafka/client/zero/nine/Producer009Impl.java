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
package io.streamnative.kafka.client.zero.nine;

import io.streamnative.kafka.client.api.ProduceContext;
import io.streamnative.kafka.client.api.Producer;
import io.streamnative.kafka.client.api.ProducerConfiguration;
import io.streamnative.kafka.client.api.RecordMetadata;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * The implementation of Kafka producer 0.9.0.0.
 */
public class Producer009Impl<K, V> extends KafkaProducer<K, V> implements Producer<K, V> {

    public Producer009Impl(final ProducerConfiguration conf) {
        super(conf.toProperties());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Future<RecordMetadata> sendAsync(final ProduceContext<K, V> context) {
        send(context.createV0ProducerRecord(ProducerRecord.class), context::complete);
        return context.getFuture();
    }
}
