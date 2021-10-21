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
import io.streamnative.kafka.client.api.TopicOffsetAndMetadata;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.collections.Maps;

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

    /**
     * Test Kafka production and Kafka consumption.
     * after kafka client 0.11.x versions, producer will send record with magic=2.
     * Therefore, the previous test will not appear issue: https://github.com/streamnative/kop/issues/656
     * After introducing the kafka 0-10 module, we can reuse the current test,
     * Because 0.10 will produce a message with magic=1, and the kafka-1-0 and default modules
     * will use the apiVersion corresponding to magic=2 to send FETCH requests
     * @throws Exception
     */
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
            // Because there is no header in ProducerRecord before 0.11.x.
            if (!(version.equals(KafkaVersion.KAFKA_0_10_0_0)
                    || version.equals(KafkaVersion.KAFKA_0_9_0_0))) {
                headers.add(new Header("header-" + key, "header-" + value));
            }

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

            // Because there is no header in ProducerRecord before 0.11.x.
            if (!(version.equals(KafkaVersion.KAFKA_0_10_0_0)
                    || version.equals(KafkaVersion.KAFKA_0_9_0_0))) {
                Assert.assertEquals(records.stream()
                        .map(ConsumerRecord::getHeaders)
                        .filter(Objects::nonNull)
                        .map(headerList -> headerList.get(0))
                        .collect(Collectors.toList()), headers);
            }
            consumer.close();
        }
    }

    protected void verifyManualCommitOffset(final String topic,
                                            final int count,
                                            final int numPartitions,
                                            final Map<Integer, List<String>> valuesMap)
            throws IOException {
        for (KafkaVersion version : kafkaClientFactories.keySet()) {
            // 3.Forbidden to commit the offset automatically.
            // We will manually submit the offset later.
            Consumer<String, String> consumer = kafkaClientFactories.get(version)
                    .createConsumer(consumerConfiguration(version, false));

            consumer.subscribe(topic);

            // 4.Consume all messages
            List<ConsumerRecord<String, String>> records = consumer.receiveUntil(count, 6000);
            Assert.assertEquals(count, records.size());
            Map<Integer, List<ConsumerRecord<String, String>>> totalRecordsGroupByPartition = records.stream()
                    .collect(Collectors.groupingBy(ConsumerRecord::getPartition));

            // 5.The results of consumption are grouped according to partitions,
            // and the records of each partition are compared with the partition data recorded above.
            totalRecordsGroupByPartition.keySet().forEach(
                    partition -> Assert.assertEquals(
                            totalRecordsGroupByPartition.get(partition).stream()
                                    .map(ConsumerRecord::getValue)
                                    .collect(Collectors.toList()),
                            valuesMap.get(partition))
            );

            // 6.Group by partition, record the offset and value of all data in the partition,
            // and later we will use it to compare with the consumption result after manually committing offsets.
            Map<Integer, Map<Long, String>> partitionAndOffsetValue = Maps.newConcurrentMap();
            records.forEach(record -> {
                int partition = record.getPartition();
                long offset = record.getOffset();
                String value = record.getValue();

                Map<Long, String> offsetAndValue = partitionAndOffsetValue
                        .computeIfAbsent(partition, k -> new HashMap<>());

                offsetAndValue.put(offset, value);
                partitionAndOffsetValue.put(partition, offsetAndValue);
            });

            // 7.Manually commit offset of each partition as the int value of the partition number,
            // this is for the convenience of calculation
            List<TopicOffsetAndMetadata> commitOffsets = new ArrayList<>();

            int messagesSize = count;
            for (int i = 0; i < numPartitions; i++) {
                commitOffsets.add(new TopicOffsetAndMetadata(topic, i, i));
                messagesSize -= i;
            }

            // 8.Commit offset synchronously
            consumer.commitOffsetSync(commitOffsets, Duration.ofMillis(10000));

            // 9.Close current consumer
            consumer.close();

            // 10.Use the same consumer group to start a new consumer group,
            // and consumers will start to consume from the offset manually committed above
            consumer = kafkaClientFactories.get(version)
                    .createConsumer(consumerConfiguration(version, false));
            consumer.subscribe(topic);

            // 11.Consume messages and verify that the number of messages is the same
            // as the number of messages calculated according to the commit offset
            List<ConsumerRecord<String, String>> commitRecordList = consumer.receiveUntil(messagesSize, 12000);
            Assert.assertEquals(messagesSize, commitRecordList.size());

            // 12.The results of consumption are grouped according to partitions to facilitate
            // the comparison between the results of consumption and the messages produced later
            Map<Integer, List<ConsumerRecord<String, String>>> recordsGroupByPartition = commitRecordList.stream()
                    .collect(Collectors.groupingBy(ConsumerRecord::getPartition));

            // 13.Compare the number of messages consumed by each partition
            // is the same as the number calculated by manual commit offset
            recordsGroupByPartition.keySet().forEach(
                    partition -> Assert.assertEquals(count / numPartitions - partition,
                            recordsGroupByPartition.getOrDefault(partition, Collections.emptyList()).size())
            );

            // 14.Verify the accuracy of the offset and the corresponding message value consumed by each partition
            commitRecordList.forEach(record -> {
                Assert.assertEquals(partitionAndOffsetValue.get(record.getPartition()).get(record.getOffset()),
                        record.getValue());
            });

            consumer.close();
        }
    }

    protected void testKafkaProduceKafkaCommitOffset() throws Exception {
        final String topic = "test-kafka-produce-kafka-commit-offset";
        int numPartitions = 5;
        admin.topics().createPartitionedTopic(topic, numPartitions);

        Map<Integer, List<String>> valuesMap = Maps.newHashMap();

        // The number of messages sent by the producer of each version
        long sends = 25;

        // Record the total number of messages
        int count = 0;
        // 1.Produce messages with different versions
        for (KafkaVersion version : kafkaClientFactories.keySet()) {

            Producer<String, String> producer = kafkaClientFactories.get(version)
                    .createProducer(producerConfiguration(version));

            for (int i = 0; i < sends; i++) {
                String value = "value-commit-from" + version.name() + i;

                // (Round Robin) Select the partition to send record
                int partition = i % numPartitions;
                List<String> values = valuesMap.computeIfAbsent(partition, k -> new ArrayList<>());
                values.add(value);
                // Record the message corresponding to each partition,
                // and later we will use it to compare the consumption results.
                valuesMap.put(partition, values);

                RecordMetadata recordMetadata = producer.newContextBuilder(topic, value, partition).
                        build().sendAsync().get();
                log.info("Kafka client {} sent {} to {}", version, value, recordMetadata);
                Assert.assertEquals(recordMetadata.getTopic(), topic);
                Assert.assertEquals(recordMetadata.getPartition(), partition);
                count++;
            }
        }

        // 2.Consume messages with different versions
        verifyManualCommitOffset(topic, count, numPartitions, valuesMap);
    }

    protected ProducerConfiguration producerConfiguration(final KafkaVersion version) {
        return ProducerConfiguration.builder()
                .bootstrapServers("localhost:" + getKafkaBrokerPort())
                .keySerializer(version.getStringSerializer())
                .valueSerializer(version.getStringSerializer())
                .build();
    }

    protected ConsumerConfiguration consumerConfiguration(final KafkaVersion version) {
        return consumerConfiguration(version, true);
    }

    protected ConsumerConfiguration consumerConfiguration(final KafkaVersion version, Boolean enableAutoCommit) {
        return ConsumerConfiguration.builder()
                .bootstrapServers("localhost:" + getKafkaBrokerPort())
                .groupId("group-" + version.name())
                .keyDeserializer(version.getStringDeserializer())
                .valueDeserializer(version.getStringDeserializer())
                .fromEarliest(true)
                .enableAutoCommit(enableAutoCommit)
                .sessionTimeOutMs("10000")
                .build();
    }

}
