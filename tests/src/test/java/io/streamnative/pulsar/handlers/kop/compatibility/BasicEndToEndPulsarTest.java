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
import io.streamnative.kafka.client.api.ProduceContext;
import io.streamnative.kafka.client.api.Producer;
import io.streamnative.kafka.client.api.RecordMetadata;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.Hash;
import org.apache.pulsar.client.impl.JavaStringHash;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

/**
 * Basic end-to-end test for different versions of Kafka clients with `entryFormat=kafka`.
 */
@Slf4j
public class BasicEndToEndPulsarTest extends BasicEndToEndTestBase {

    public BasicEndToEndPulsarTest() {
        super("pulsar");
    }

    @Test(timeOut = 30000)
    protected void testKafkaProduceKafkaConsume() throws Exception {
        super.testKafkaProduceKafkaConsume();
    }

    @Test(timeOut = 60000)
    protected void testKafkaProduceKafkaCommitOffset() throws Exception {
        super.testKafkaProduceKafkaCommitOffset();
    }

    @Test(timeOut = 30000)
    public void testKafkaProducePulsarConsume() throws Exception {
        final String topic = "test-kafka-produce-pulsar-consume";
        admin.topics().createPartitionedTopic(topic, 1);

        final List<String> keys = new ArrayList<>();
        final List<String> values = new ArrayList<>();
        final List<Header> headers = new ArrayList<>();

        long offset = 0;
        for (KafkaVersion version : kafkaClientFactories.keySet()) {
            final Producer<String, String> producer = kafkaClientFactories.get(version)
                    .createProducer(producerConfiguration(version));
            final ProduceContext<String, String> record = producer.newContextBuilder(topic, "value-" + version)
                    .key("key-" + version)
                    .headers(Collections.singletonList(new Header("header-key-" + version, "header-value-" + version)))
                    .build();
            keys.add(record.getKey());
            values.add(record.getValue());
            // message has no header before Kafka 0.11.x version
            if (version.equals(KafkaVersion.KAFKA_0_10_0_0)
                    || version.equals(KafkaVersion.KAFKA_0_9_0_0)) {
                headers.add(null);
            } else {
                headers.add(record.getHeaders().get(0));
            }

            final RecordMetadata metadata = record.sendAsync().get();
            log.info("Kafka client {} sent {} to {}", version, record.getValue(), metadata);
            Assert.assertEquals(metadata.getTopic(), topic);
            Assert.assertEquals(metadata.getPartition(), 0);
            Assert.assertEquals(metadata.getOffset(), offset);
            offset++;

            producer.close();
        }

        final org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("pulsar-sub")
                .subscribe();
        int numReceived = 0;
        final List<Message<byte[]>> receivedMessages = new ArrayList<>();
        while (numReceived < values.size()) {
            final Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
            if (message == null) {
                continue;
            }
            log.info("Pulsar client received {}", message);
            receivedMessages.add(message);
            ++numReceived;
        }
        Assert.assertEquals(receivedMessages.stream()
                .map(Message::getValue)
                .map(value -> new String(value, StandardCharsets.UTF_8))
                .collect(Collectors.toList()), values);
        Assert.assertEquals(receivedMessages.stream()
                .map(Message::getOrderingKey)
                .map(key -> new String(key, StandardCharsets.UTF_8))
                .collect(Collectors.toList()), keys);
        Assert.assertEquals(receivedMessages.stream()
                .map(Message::getProperties)
                .map(BasicEndToEndPulsarTest::convertFirstPropertyToHeader)
                .collect(Collectors.toList()), headers);
        consumer.close();
    }

    @Test(timeOut = 30000)
    public void testPulsarProduceKafkaConsume() throws Exception {
        final String topic = "test-pulsar-produce-kafka-consume";
        admin.topics().createPartitionedTopic(topic, 1);

        final String key = "pulsar-key";
        final String value = "pulsar-value";
        final Header header = new Header("header-pulsar-key", "header-pulsar-value");

        final org.apache.pulsar.client.api.Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .create();
        producer.newMessage(Schema.STRING)
                .orderingKey(key.getBytes(StandardCharsets.UTF_8))
                .value(value)
                .property(header.getKey(), header.getValue())
                .send();
        producer.close();

        for (KafkaVersion version : kafkaClientFactories.keySet()) {
            final Consumer<String, String> consumer = kafkaClientFactories.get(version)
                    .createConsumer(consumerConfiguration(version));
            consumer.subscribe(topic);
            final List<ConsumerRecord<String, String>> records = consumer.receiveUntil(1, 6000);
            Assert.assertEquals(records.size(), 1);
            Assert.assertEquals(records.get(0).getValue(), value);
            Assert.assertEquals(records.get(0).getKey(), key);
            if (!(version.equals(KafkaVersion.KAFKA_0_10_0_0)
                    || version.equals(KafkaVersion.KAFKA_0_9_0_0))) {
                Assert.assertEquals(records.get(0).getHeaders().get(0), header);
            }
            consumer.close();
        }
    }

    @Test(timeOut = 60000)
    public void testPulsarProduceKafkaCommit() throws Exception {
        final String topic = "test-pulsar-produce-kafka-commit-offset";
        int numPartitions = 5;
        admin.topics().createPartitionedTopic(topic, numPartitions);

        Map<Integer, List<String>> valuesMap = Maps.newHashMap();

        int sends = 75;

        // 1.Produce messages with pulsar producer
        org.apache.pulsar.client.api.Producer<String> producer =
                pulsarClient.newProducer(Schema.STRING)
                        .topic(topic)
                        .create();

        Hash hash = JavaStringHash.getInstance();
        for (int i = 0; i < sends; i++) {
            final String key = "pulsar-key-" + i;
            final String value = "pulsar-value-" + i;

            int partition = MathUtils.signSafeMod(hash.makeHash(key), numPartitions);
            List<String> values = valuesMap.computeIfAbsent(partition, k -> new ArrayList<>());
            values.add(value);
            // Record the message corresponding to each partition,
            // and later we will use it to compare the consumption results.
            valuesMap.put(partition, values);

            producer.newMessage(Schema.STRING)
                    .key(key)
                    .value(value)
                    .send();
        }

        producer.close();

        // 2.Consume messages with different kafka client versions
        super.verifyManualCommitOffset(topic, sends, numPartitions, valuesMap);

    }

    private static Header convertFirstPropertyToHeader(final Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }
        final Iterator<Map.Entry<String, String>> iterator = properties.entrySet().iterator();
        if (iterator.hasNext()) {
            final Map.Entry<String, String> entry = iterator.next();
            return new Header(entry.getKey(), entry.getValue());
        }
        return null;
    }
}
