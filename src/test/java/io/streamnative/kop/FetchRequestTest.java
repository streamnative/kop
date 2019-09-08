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
package io.streamnative.kop;

import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.streamnative.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.kop.KafkaCommandDecoder.ResponseAndRequest;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FetchResponse.PartitionData;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;

/**
 * Validate KafkaApisTest.
 */
@Slf4j
public class FetchRequestTest extends KafkaApisTest {
    private void checkFetchResponse(List<TopicPartition> expectedPartitions,
                                    FetchResponse<MemoryRecords> fetchResponse,
                                    int maxPartitionBytes,
                                    int maxResponseBytes,
                                    int numMessagesPerPartition) {

        assertEquals(expectedPartitions.size(), fetchResponse.responseData().size());
        expectedPartitions.forEach(tp -> assertTrue(fetchResponse.responseData().get(tp) != null));

        final AtomicBoolean emptyResponseSeen = new AtomicBoolean(false);
        AtomicInteger responseSize = new AtomicInteger(0);
        AtomicInteger responseBufferSize = new AtomicInteger(0);

        expectedPartitions.forEach(tp -> {
            PartitionData partitionData = fetchResponse.responseData().get(tp);
            assertEquals(Errors.NONE, partitionData.error);
            assertTrue(partitionData.highWatermark > 0);

            MemoryRecords records = (MemoryRecords) partitionData.records;
            AtomicInteger batchesSize = new AtomicInteger(0);
            responseBufferSize.addAndGet(records.sizeInBytes());
            List<MutableRecordBatch> batches = Lists.newArrayList();
            records.batches().forEach(batch -> {
                batches.add(batch);
                batchesSize.addAndGet(batch.sizeInBytes());
            });
            assertTrue(batches.size() < numMessagesPerPartition);
            responseSize.addAndGet(batchesSize.get());

            if (batchesSize.get() == 0 && !emptyResponseSeen.get()) {
                assertEquals(0, records.sizeInBytes());
                emptyResponseSeen.set(true);
            } else if (batchesSize.get() != 0 && !emptyResponseSeen.get()) {
                assertTrue(batchesSize.get() <= maxPartitionBytes);
                assertTrue(maxPartitionBytes >= records.sizeInBytes());
            } else if (batchesSize.get() != 0 && emptyResponseSeen.get()) {
                fail("Expected partition with size 0, but found " + tp + " with size " +  batchesSize.get());
            } else if (records.sizeInBytes() != 0 && emptyResponseSeen.get()) {
                fail("Expected partition buffer with size 0, but found "
                    + tp + " with size " + records.sizeInBytes());
            }
        });

        // In Kop implementation, fetch at least 1 item for each topicPartition in the request.
    }

    private Map<TopicPartition, FetchRequest.PartitionData> createPartitionMap(int maxPartitionBytes,
                                                                               List<TopicPartition> topicPartitions,
                                                                               Map<TopicPartition, Long> offsetMap) {
        return topicPartitions.stream()
            .map(topic ->
                Pair.of(topic,
                    new FetchRequest.PartitionData(
                        offsetMap.getOrDefault(topic, 0L),
                        0L,
                        maxPartitionBytes)))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private KafkaHeaderAndRequest createFetchRequest(int maxResponseBytes,
                                                     int maxPartitionBytes,
                                                     List<TopicPartition> topicPartitions,
                                                     Map<TopicPartition, Long> offsetMap) {

        AbstractRequest.Builder builder = FetchRequest.Builder
            .forConsumer(Integer.MAX_VALUE, 0, createPartitionMap(maxPartitionBytes, topicPartitions, offsetMap))
            .setMaxBytes(maxResponseBytes);

        return buildRequest(builder);
    }

    private List<TopicPartition> createTopics(String topicName, int numTopics, int numPartitions) throws Exception {
        List<TopicPartition> result = Lists.newArrayListWithExpectedSize(numPartitions * numTopics);

        for (int topicIndex = 0; topicIndex < numTopics; topicIndex++) {
            String tName = topicName + "_" + topicIndex;
            kafkaService.getAdminClient().topics().createPartitionedTopic(tName, numPartitions);

            for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
                kafkaService.getAdminClient().topics()
                    .createNonPartitionedTopic(tName + PARTITIONED_TOPIC_SUFFIX + partitionIndex);
                result.add(new TopicPartition(tName, partitionIndex));
            }
        }

        return result;
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost" + ":" + getKafkaBrokerPort());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "FetchRequestTestProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return producer;
    }

    private void produceData(KafkaProducer<String, String> producer,
                             List<TopicPartition> topicPartitions,
                             int numMessagesPerPartition) throws Exception{
        for (int index = 0; index < topicPartitions.size(); index++) {
            TopicPartition tp = topicPartitions.get(index);
            for (int messageIndex = 0; messageIndex < numMessagesPerPartition; messageIndex++) {
                String suffix = tp.toString() + "-" + messageIndex;
                producer
                    .send(
                        new ProducerRecord<>(
                            tp.topic(),
                            tp.partition(),
                            "key " + suffix,
                            "value " + suffix))
                    .get();
            }
        }
    }

    @Test(timeOut = 20000)
    public void testBrokerRespectsPartitionsOrderAndSizeLimits() throws Exception {
        String topicName = "kopBrokerRespectsPartitionsOrderAndSizeLimits";
        int numberTopics = 5;
        int numberPartitions = 6;

        int messagesPerPartition = 9;
        int maxResponseBytes = 800;
        int maxPartitionBytes = 190;

        List<TopicPartition> topicPartitions = createTopics(topicName, numberTopics, numberPartitions);

        List<TopicPartition> partitionsWithLargeMessages = topicPartitions
            .subList(topicPartitions.size() - 2, topicPartitions.size());
        TopicPartition partitionWithLargeMessage1 = partitionsWithLargeMessages.get(0);
        TopicPartition partitionWithLargeMessage2 = partitionsWithLargeMessages.get(1);
        List<TopicPartition> partitionsWithoutLargeMessages = topicPartitions
            .subList(0, topicPartitions.size() - 2);

        @Cleanup
        KafkaProducer<String, String> kProducer = createKafkaProducer();
        produceData(kProducer, topicPartitions, messagesPerPartition);

        kProducer
            .send(
                new ProducerRecord<>(
                    partitionWithLargeMessage1.topic(),
                    partitionWithLargeMessage1.partition(),
                    "larger than partition limit",
                    new String(new byte[maxPartitionBytes + 1])))
            .get();

        kProducer
            .send(
                new ProducerRecord<>(
                    partitionWithLargeMessage2.topic(),
                    partitionWithLargeMessage2.partition(),
                    "larger than partition limit",
                    new String(new byte[maxResponseBytes + 1])))
            .get();

        // 1. Partitions with large messages at the end
        Collections.shuffle(partitionsWithoutLargeMessages);
        List<TopicPartition> shuffledTopicPartitions1 = Lists.newArrayListWithExpectedSize(topicPartitions.size());
        shuffledTopicPartitions1.addAll(partitionsWithoutLargeMessages);
        shuffledTopicPartitions1.addAll(partitionsWithLargeMessages);

        KafkaHeaderAndRequest fetchRequest1 = createFetchRequest(
            maxResponseBytes,
            maxPartitionBytes,
            shuffledTopicPartitions1,
            Collections.EMPTY_MAP);
        CompletableFuture<ResponseAndRequest> responseFuture1 = kafkaRequestHandler.handleFetchRequest(fetchRequest1);
        FetchResponse<MemoryRecords> fetchResponse1 =
            (FetchResponse<MemoryRecords>) responseFuture1.get().getResponse();

        checkFetchResponse(shuffledTopicPartitions1, fetchResponse1,
            maxPartitionBytes, maxResponseBytes, messagesPerPartition);

        // 2. Same as 1, but shuffled again
        Collections.shuffle(partitionsWithoutLargeMessages);
        List<TopicPartition> shuffledTopicPartitions2 = Lists.newArrayListWithExpectedSize(topicPartitions.size());
        shuffledTopicPartitions2.addAll(partitionsWithoutLargeMessages);
        shuffledTopicPartitions2.addAll(partitionsWithLargeMessages);

        KafkaHeaderAndRequest fetchRequest2 = createFetchRequest(
            maxResponseBytes,
            maxPartitionBytes,
            shuffledTopicPartitions2,
            Collections.EMPTY_MAP);
        CompletableFuture<ResponseAndRequest> responseFuture2 = kafkaRequestHandler.handleFetchRequest(fetchRequest2);
        FetchResponse<MemoryRecords> fetchResponse2 =
            (FetchResponse<MemoryRecords>) responseFuture2.get().getResponse();

        checkFetchResponse(shuffledTopicPartitions2, fetchResponse2,
            maxPartitionBytes, maxResponseBytes, messagesPerPartition);

        // 3. Partition with message larger than the partition limit at the start of the list
        Collections.shuffle(partitionsWithoutLargeMessages);
        List<TopicPartition> shuffledTopicPartitions3 = Lists.newArrayListWithExpectedSize(topicPartitions.size());
        shuffledTopicPartitions3.addAll(partitionsWithLargeMessages);
        shuffledTopicPartitions3.addAll(partitionsWithoutLargeMessages);


        Map<TopicPartition, Long> offsetMaps =  Maps.newHashMap();
        offsetMaps.put(partitionWithLargeMessage1, Long.valueOf(messagesPerPartition));
        KafkaHeaderAndRequest fetchRequest3 = createFetchRequest(
            maxResponseBytes,
            maxPartitionBytes,
            shuffledTopicPartitions3,
            offsetMaps);
        CompletableFuture<ResponseAndRequest> responseFuture3 = kafkaRequestHandler.handleFetchRequest(fetchRequest3);
        FetchResponse<MemoryRecords> fetchResponse3 =
            (FetchResponse<MemoryRecords>) responseFuture3.get().getResponse();

        checkFetchResponse(shuffledTopicPartitions3, fetchResponse3,
            maxPartitionBytes, maxResponseBytes, messagesPerPartition);
    }
}
