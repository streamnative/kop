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
package io.streamnative.kafka.client.one.zero;

import com.google.common.collect.Maps;
import io.streamnative.kafka.client.api.Consumer;
import io.streamnative.kafka.client.api.ConsumerConfiguration;
import io.streamnative.kafka.client.api.ConsumerRecord;
import io.streamnative.kafka.client.api.TopicOffsetAndMetadata;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * The implementation of Kafka consumer 1.0.0.
 */
public class ConsumerImpl<K, V> extends KafkaConsumer<K, V> implements Consumer<K, V> {

    public ConsumerImpl(final ConsumerConfiguration conf) {
        super(conf.toProperties());
    }

    @Override
    public List<ConsumerRecord<K, V>> receive(long timeoutMs) {
        final List<ConsumerRecord<K, V>> records = new ArrayList<>();
        poll(timeoutMs).forEach(record -> records.add(ConsumerRecord.create(record)));
        return records;
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(long timeoutMS) {
        return listTopics();
    }

    @Override
    public void commitOffsetSync(List<TopicOffsetAndMetadata> offsets, Duration timeout) {
        HashMap<TopicPartition, OffsetAndMetadata> offsetsMap = Maps.newHashMap();
        offsets.forEach(
                offsetAndMetadata -> offsetsMap.put(
                        offsetAndMetadata.createTopicPartition(TopicPartition.class),
                        offsetAndMetadata.createOffsetAndMetadata(OffsetAndMetadata.class)
                )
        );
        commitSync(offsetsMap);
    }
}
