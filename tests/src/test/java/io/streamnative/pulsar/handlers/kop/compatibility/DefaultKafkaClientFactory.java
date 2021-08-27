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
package io.streamnative.pulsar.handlers.kop.compatibility;

import io.streamnative.kafka.client.api.Consumer;
import io.streamnative.kafka.client.api.ConsumerConfiguration;
import io.streamnative.kafka.client.api.ConsumerRecord;
import io.streamnative.kafka.client.api.KafkaClientFactory;
import io.streamnative.kafka.client.api.ProduceContext;
import io.streamnative.kafka.client.api.Producer;
import io.streamnative.kafka.client.api.ProducerConfiguration;
import io.streamnative.kafka.client.api.RecordMetadata;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;

/**
 * Kafka client factory for the default version of Kafka client.
 */
public class DefaultKafkaClientFactory implements KafkaClientFactory {

    @Override
    public <K, V> Producer<K, V> createProducer(ProducerConfiguration conf) {
        return new DefaultKafkaProducer<>(conf);
    }

    @Override
    public <K, V> Consumer<K, V> createConsumer(ConsumerConfiguration conf) {
        return new DefaultKafkaConsumer<>(conf);
    }

    private static class DefaultKafkaProducer<K, V> extends KafkaProducer<K, V> implements Producer<K, V> {

        public DefaultKafkaProducer(final ProducerConfiguration conf) {
            super(conf.toProperties());
        }

        @SuppressWarnings("unchecked")
        @Override
        public Future<RecordMetadata> sendAsync(final ProduceContext<K, V> context) {
            send(context.createProducerRecord(ProducerRecord.class, RecordHeader::new), context::complete);
            return context.getFuture();
        }
    }

    private static class DefaultKafkaConsumer<K, V> extends KafkaConsumer<K, V> implements Consumer<K, V> {

        public DefaultKafkaConsumer(final ConsumerConfiguration conf) {
            super(conf.toProperties());
        }

        @Override
        public List<ConsumerRecord<K, V>> receive(long timeoutMs) {
            final List<ConsumerRecord<K, V>> records = new ArrayList<>();
            poll(Duration.ofMillis(timeoutMs)).forEach(record -> records.add(ConsumerRecord.create(record)));
            return records;
        }

        @Override
        public Map<String, List<PartitionInfo>> listTopics(long timeoutMS) {
            return listTopics(Duration.ofMillis(timeoutMS));
        }

        @Override
        public void commitOffsetSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
            commitSync(offsets, timeout);
        }
    }
}
