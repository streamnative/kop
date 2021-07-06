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
import io.streamnative.kafka.client.api.ConsumerRecord;
import io.streamnative.kafka.client.api.Header;
import io.streamnative.kafka.client.api.KafkaVersion;
import io.streamnative.kafka.client.api.Producer;
import io.streamnative.kafka.client.api.RecordMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Basic end-to-end test for different versions of Kafka clients with `entryFormat=kafka`.
 */
@Slf4j
public class BasicEndToEndKafkaTest extends BasicEndToEndTestBase {

    public BasicEndToEndKafkaTest() {
        super("kafka");
    }

    @Test(timeOut = 30000)
    public void testEndToEnd() throws Exception {
        final String topic = "test-end-to-end";
        admin.topics().createPartitionedTopic(topic, 1);

        final List<String> keys = new ArrayList<>();
        final List<String> values = new ArrayList<>();
        final List<Header> headers = new ArrayList<>();

        long offset = 0;
        for (KafkaVersion version : kafkaClientFactories.keySet()) {
            final Producer<String, String> producer = kafkaClientFactories.get(version)
                    .createProducer(producerConfiguration(version));

            // send a message that only contains value
            String value = "value-from-" + version.name() + offset;
            values.add("value-from-" + version.name() + offset);

            RecordMetadata metadata = producer.newContextBuilder(topic, value).build().sendAsync().get();
            log.info("Kafka client {} sent {} to {}", version, value, metadata);
            Assert.assertEquals(metadata.getTopic(), topic);
            Assert.assertEquals(metadata.getPartition(), 0);
            Assert.assertEquals(metadata.getOffset(), offset);
            offset++;

            // send a message that contains key and headers, which are optional
            String key = "key-from-" + version.name() + offset;
            value = "value-from-" + version.name() + offset;
            keys.add(key);
            values.add(value);
            headers.add(new Header("header-" + key, "header-" + value));

            metadata = producer.newContextBuilder(topic, value)
                    .key(key)
                    .headers(headers.subList(headers.size() - 1, headers.size()))
                    .build()
                    .sendAsync()
                    .get();
            log.info("Kafka client {} sent {} (key={}) to {}", version, value, key, metadata);
            Assert.assertEquals(metadata.getTopic(), topic);
            Assert.assertEquals(metadata.getPartition(), 0);
            Assert.assertEquals(metadata.getOffset(), offset);
            offset++;

            producer.close();
        }

        for (KafkaVersion version : kafkaClientFactories.keySet()) {
            final Consumer<String, String> consumer = kafkaClientFactories.get(version)
                    .createConsumer(consumerConfiguration(version));
            consumer.subscribe(topic);
            final List<ConsumerRecord<String, String>> records = consumer.receiveUntil(values.size(), 6000);
            Assert.assertEquals(records.stream().map(ConsumerRecord::getValue).collect(Collectors.toList()), values);
            Assert.assertEquals(records.stream()
                    .map(ConsumerRecord::getKey)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList()), keys);
            Assert.assertEquals(records.stream()
                    .map(ConsumerRecord::getHeaders)
                    .filter(Objects::nonNull)
                    .map(headerList -> headerList.get(0))
                    .collect(Collectors.toList()), headers);
            consumer.close();
        }
    }
}
