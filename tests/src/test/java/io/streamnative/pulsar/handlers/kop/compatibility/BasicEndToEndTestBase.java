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
import io.streamnative.kafka.client.api.Header;
import io.streamnative.kafka.client.api.KafkaClientFactory;
import io.streamnative.kafka.client.api.KafkaClientFactoryImpl;
import io.streamnative.kafka.client.api.KafkaVersion;
import io.streamnative.kafka.client.api.Producer;
import io.streamnative.kafka.client.api.ProducerConfiguration;
import io.streamnative.kafka.client.api.RecordMetadata;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Basic end-to-end test for different versions of Kafka clients.
 */
@Slf4j
public class BasicEndToEndTestBase extends KopProtocolHandlerTestBase {

    protected Map<KafkaVersion, KafkaClientFactory> kafkaClientFactories = Arrays.stream(KafkaVersion.values())
            .collect(Collectors.toMap(
                    version -> version,
                    version -> {
                        if (version.equals(KafkaVersion.DEFAULT)) {
                            return new DefaultKafkaClientFactory();
                        } else {
                            return new KafkaClientFactoryImpl(version);
                        }
                    },
                    (k, v) -> {
                        throw new IllegalStateException("Duplicated key: " + k);
                    }, TreeMap::new));

    public BasicEndToEndTestBase(final String entryFormat) {
        super(entryFormat);
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected void testKafkaProduceKafkaConsume() throws Exception {
        final String topic = "test-kafka-produce-kafka-consume";
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
            if (conf.getEntryFormat().equals("pulsar")) {
                // NOTE: PulsarEntryFormatter will encode an empty String as key if key doesn't exist
                Assert.assertEquals(records.stream()
                        .map(ConsumerRecord::getKey)
                        .filter(key -> key != null && !key.isEmpty())
                        .collect(Collectors.toList()), keys);
            } else {
                Assert.assertEquals(records.stream()
                        .map(ConsumerRecord::getKey)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()), keys);
            }
            Assert.assertEquals(records.stream()
                    .map(ConsumerRecord::getHeaders)
                    .filter(Objects::nonNull)
                    .map(headerList -> headerList.get(0))
                    .collect(Collectors.toList()), headers);
            consumer.close();
        }
    }

    protected ProducerConfiguration producerConfiguration(final KafkaVersion version) {
        return ProducerConfiguration.builder()
                .bootstrapServers("localhost:" + getKafkaBrokerPort())
                .keySerializer(version.getStringSerializer())
                .valueSerializer(version.getStringSerializer())
                .build();
    }

    protected ConsumerConfiguration consumerConfiguration(final KafkaVersion version) {
        return ConsumerConfiguration.builder()
                .bootstrapServers("localhost:" + getKafkaBrokerPort())
                .groupId("group-" + version.name())
                .keyDeserializer(version.getStringDeserializer())
                .valueDeserializer(version.getStringDeserializer())
                .fromEarliest(true)
                .build();
    }
}
