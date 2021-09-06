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

import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.stats.NullStatsLogger;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest.PartitionData;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

/**
 * Validate KafkaApisTest.
 */
@Slf4j
public class KafkaApisTest extends KopProtocolHandlerTestBase {

    KafkaRequestHandler kafkaRequestHandler;
    SocketAddress serviceAddress;
    private AdminManager adminManager;

    @Override
    protected void resetConfig() {
        super.resetConfig();
        this.conf.setKafkaAdvertisedListeners(PLAINTEXT_PREFIX + "127.0.0.1:" + kafkaBrokerPort + ","
                + SSL_PREFIX + "127.0.0.1:" + kafkaBrokerPortTls);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.namespaces().getNamespaces("public").contains("public/default")) {
            admin.namespaces().createNamespace("public/default");
            admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/default",
                new RetentionPolicies(60, 1000));
        }
        if (!admin.namespaces().getNamespaces("public").contains("public/__kafka")) {
            admin.namespaces().createNamespace("public/__kafka");
            admin.namespaces().setNamespaceReplicationClusters("public/__kafka", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/__kafka",
                new RetentionPolicies(-1, -1));
        }

        log.info("created namespaces, init handler");

        ProtocolHandler handler = pulsar.getProtocolHandlers().protocol("kafka");
        GroupCoordinator groupCoordinator = ((KafkaProtocolHandler) handler).getGroupCoordinator();
        TransactionCoordinator transactionCoordinator = ((KafkaProtocolHandler) handler).getTransactionCoordinator();

        adminManager = new AdminManager(pulsar.getAdminClient(), conf);
        kafkaRequestHandler = new KafkaRequestHandler(
            pulsar,
            (KafkaServiceConfiguration) conf,
            groupCoordinator,
            transactionCoordinator,
            adminManager,
            pulsar.getLocalMetadataStore().getMetadataCache(LocalBrokerData.class),
            false,
            getPlainEndPoint(),
            NullStatsLogger.INSTANCE);
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockCtx).channel();
        kafkaRequestHandler.ctx = mockCtx;

        serviceAddress = new InetSocketAddress(pulsar.getBindAddress(), kafkaBrokerPort);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        adminManager.shutdown();
        super.internalCleanup();
    }

    KafkaHeaderAndRequest buildRequest(AbstractRequest.Builder builder) {
        AbstractRequest request = builder.build();
        builder.apiKey();

        ByteBuffer serializedRequest = request
            .serialize(new RequestHeader(builder.apiKey(), request.version(), "fake_client_id", 0));

        ByteBuf byteBuf = Unpooled.copiedBuffer(serializedRequest);

        RequestHeader header = RequestHeader.parse(serializedRequest);

        ApiKeys apiKey = header.apiKey();
        short apiVersion = header.apiVersion();
        Struct struct = apiKey.parseRequest(apiVersion, serializedRequest);
        AbstractRequest body = AbstractRequest.parseRequest(apiKey, apiVersion, struct);
        return new KafkaHeaderAndRequest(header, body, byteBuf, serviceAddress);
    }

    void checkInvalidPartition(CompletableFuture<AbstractResponse> future,
                                                              String topic,
                                                              int invalidPartitionId) {
        TopicPartition invalidTopicPartition = new TopicPartition(topic, invalidPartitionId);
        PartitionData partitionOffsetCommitData = new PartitionData(15L, "");
        Map<TopicPartition, PartitionData> offsetData = Maps.newHashMap();
        offsetData.put(invalidTopicPartition, partitionOffsetCommitData);
        KafkaHeaderAndRequest request = buildRequest(new OffsetCommitRequest.Builder("groupId", offsetData));
        kafkaRequestHandler.handleOffsetCommitRequest(request, future);
    }

    @Test(timeOut = 20000, enabled = false)
    // https://github.com/streamnative/kop/issues/51
    public void testOffsetCommitWithInvalidPartition() throws Exception {
        String topicName = "kopOffsetCommitWithInvalidPartition";

        CompletableFuture<AbstractResponse> invalidResponse1 = new CompletableFuture<>();
        // invalid partition id -1;
        checkInvalidPartition(invalidResponse1, topicName, -1);
        AbstractResponse response1 = invalidResponse1.get();
        TopicPartition topicPartition1 = new TopicPartition(topicName, -1);
        assertEquals(((OffsetCommitResponse) response1).responseData().get(topicPartition1),
            Errors.UNKNOWN_TOPIC_OR_PARTITION);

        // invalid partition id 1.
        CompletableFuture<AbstractResponse> invalidResponse2 = new CompletableFuture<>();
        checkInvalidPartition(invalidResponse2, topicName, 1);
        TopicPartition topicPartition2 = new TopicPartition(topicName, 1);
        AbstractResponse response2 = invalidResponse2.get();
        assertEquals(((OffsetCommitResponse) response2).responseData().get(topicPartition2),
            Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    // TODO: Add transaction support https://github.com/streamnative/kop/issues/39
    // testTxnOffsetCommitWithInvalidPartition
    // testAddPartitionsToTxnWithInvalidPartition
    // shouldThrowUnsupportedVersionExceptionOnHandleAddOffsetToTxnRequestWhenInterBrokerProtocolNotSupported
    // shouldThrowUnsupportedVersionExceptionOnHandleAddPartitionsToTxnRequestWhenInterBrokerProtocolNotSupported
    // shouldThrowUnsupportedVersionExceptionOnHandleTxnOffsetCommitRequestWhenInterBrokerProtocolNotSupported
    // shouldThrowUnsupportedVersionExceptionOnHandleEndTxnRequestWhenInterBrokerProtocolNotSupported
    // shouldThrowUnsupportedVersionExceptionOnHandleWriteTxnMarkersRequestWhenInterBrokerProtocolNotSupported
    // shouldRespondWithUnsupportedForMessageFormatOnHandleWriteTxnMarkersWhenMagicLowerThanRequired
    // shouldRespondWithUnknownTopicWhenPartitionIsNotHosted
    // shouldRespondWithUnsupportedMessageFormatForBadPartitionAndNoErrorsForGoodPartition
    // shouldRespondWithUnknownTopicOrPartitionForBadPartitionAndNoErrorsForGoodPartition
    // shouldAppendToLogOnWriteTxnMarkersWhenCorrectMagicVersion

    // these 2 test cases test HighWatermark and LastStableOffset. they are the same for Pulsar,
    // so combine it in one test case.
    // Test ListOffset for earliest get the earliest message in topic.
    // testReadUncommittedConsumerListOffsetEarliestOffsetEqualsHighWatermark
    // testReadCommittedConsumerListOffsetEarliestOffsetEqualsLastStableOffset
    @Test(timeOut = 20000)
    public void testReadUncommittedConsumerListOffsetEarliestOffsetEquals() throws Exception {
        String topicName = "testReadUncommittedConsumerListOffsetEarliest";
        TopicPartition tp = new TopicPartition(topicName, 0);

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 1);

        // 1. prepare topic:
        //    use kafka producer to produce 10 messages.
        //    use pulsar consumer to get message offset.
        @Cleanup
        KProducer kProducer = new KProducer(topicName, false, getKafkaBrokerPort());
        int totalMsgs = 10;
        String messageStrPrefix = topicName + "_message_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer()
                .send(new ProducerRecord<>(
                    topicName,
                    i,
                    messageStr))
                .get();
            log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
        }

        // 2. real test, for ListOffset request verify Earliest get earliest
        Map<TopicPartition, Long> targetTimes = Maps.newHashMap();
        targetTimes.put(tp, ListOffsetRequest.EARLIEST_TIMESTAMP);

        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
            .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
            .setTargetTimes(targetTimes);

        KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        kafkaRequestHandler.handleListOffsetRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();
        ListOffsetResponse listOffsetResponse = (ListOffsetResponse) response;
        assertEquals(listOffsetResponse.responseData().get(tp).error, Errors.NONE);
        assertEquals(listOffsetResponse.responseData().get(tp).offset.intValue(), 0);
        assertEquals(listOffsetResponse.responseData().get(tp).timestamp, Long.valueOf(0));
    }

    // these 2 test cases test Read Commit / UnCommit.
    // they are the same for Pulsar, so combine it in one test case.
    // Test ListOffset for latest get the earliest message in topic.
    // testReadUncommittedConsumerListOffsetLatest
    // testReadCommittedConsumerListOffsetLatest
    @Test(timeOut = 20000)
    public void testConsumerListOffsetLatest() throws Exception {
        String topicName = "testConsumerListOffsetLatest";
        TopicPartition tp = new TopicPartition(topicName, 0);

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 1);

        // 1. prepare topic:
        //    use kafka producer to produce 10 messages.
        //    use pulsar consumer to get message offset.
        @Cleanup
        KProducer kProducer = new KProducer(topicName, false, getKafkaBrokerPort());
        int totalMsgs = 10;
        String messageStrPrefix = topicName + "_message_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer()
                .send(new ProducerRecord<>(
                    topicName,
                    i,
                    messageStr))
                .get();
            log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
        }

        // 2. real test, for ListOffset request verify Earliest get earliest
        Map<TopicPartition, Long> targetTimes = Maps.newHashMap();
        targetTimes.put(tp, ListOffsetRequest.LATEST_TIMESTAMP);

        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
            .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
            .setTargetTimes(targetTimes);

        KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        kafkaRequestHandler
            .handleListOffsetRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();
        ListOffsetResponse listOffsetResponse = (ListOffsetResponse) response;
        assertEquals(listOffsetResponse.responseData().get(tp).error, Errors.NONE);
        assertEquals(listOffsetResponse.responseData().get(tp).offset.intValue(), (totalMsgs));
        assertEquals(listOffsetResponse.responseData().get(tp).timestamp, Long.valueOf(0));
    }

    /**
     * Test the sending speed of fetch request when the readable data is less than fetch.minBytes.
     */
    @Test(timeOut = 60000)
    public void testFetchMinBytes() throws Exception {
        String topicName = "testMinBytesTopic";
        TopicPartition tp = new TopicPartition(topicName, 0);

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 1);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(tp);

        int maxWaitMs = 3000;
        int minBytes = 1;
        // case1: consuming an empty topic.
        KafkaConsumer<String, String> consumer1 = createKafkaConsumer(maxWaitMs, minBytes);
        consumer1.assign(topicPartitions);
        Long startTime1 = System.currentTimeMillis();
        consumer1.poll(Duration.ofMillis(maxWaitMs));
        Long endTime1 = System.currentTimeMillis();
        log.info("cost time1:" + (endTime1 - startTime1));

        // case2: consuming an topic after producing data.
        KafkaProducer<String, String> kProducer = createKafkaProducer();
        produceData(kProducer, topicPartitions, 10);

        KafkaConsumer<String, String> consumer2 = createKafkaConsumer(maxWaitMs, minBytes);
        consumer2.assign(topicPartitions);
        consumer2.seekToBeginning(topicPartitions);
        Long startTime2 = System.currentTimeMillis();
        consumer2.poll(Duration.ofMillis(maxWaitMs));
        Long endTime2 = System.currentTimeMillis();
        log.info("cost time2:" + (endTime2 - startTime2));

        // When consuming an empty topic, minBytes=1, because there is no readable data,
        // it will delay maxWait time before receiving the response.
        assertTrue(endTime1 - startTime1 >= maxWaitMs);
        // When the amount of readable data is not less than minBytes,
        // the time-consuming is usually less than maxWait time.
        assertTrue(endTime2 - startTime2 < maxWaitMs);
    }

    @Test(timeOut = 80000)
    public void testConsumerListOffset() throws Exception {
        String topicName = "listOffset";
        TopicPartition tp = new TopicPartition(topicName, 0);

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 1);

        // 1. prepare topic:
        @Cleanup
        KProducer kProducer = new KProducer(topicName, false, getKafkaBrokerPort());
        int totalMsgs = 10;
        String messageStrPrefix = topicName + "_message_";

        // produce 10 message with offset and timestamp :
        //  message               timestamp        offset
        // listOffset_message_0   2                0
        // listOffset_message_1   4                1
        // listOffset_message_2   6                2
        // listOffset_message_3   8                3
        // listOffset_message_4   10               4
        // listOffset_message_5   12               5
        // listOffset_message_6   14               6
        // listOffset_message_7   16               7
        // listOffset_message_8   18               8
        // listOffset_message_9   20               9

        long[] timestamps = new long[totalMsgs];

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            long timestamp = (i + 1) * 2;
            timestamps[i] = timestamp;
            kProducer.getProducer()
                    .send(new ProducerRecord<>(
                            topicName,
                            0,
                            timestamp,
                            i,
                            messageStr))
                    .get();
            log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
        }

        // 2. real test, test earliest
        ListOffsetResponse listOffsetResponse = listOffset(ListOffsetRequest.EARLIEST_TIMESTAMP, tp);
        System.out.println("offset for earliest " + listOffsetResponse.responseData().get(tp).offset.intValue());
        assertEquals(listOffsetResponse.responseData().get(tp).error, Errors.NONE);
        assertEquals(listOffsetResponse.responseData().get(tp).offset.intValue(), 0);

        listOffsetResponse = listOffset(ListOffsetRequest.LATEST_TIMESTAMP, tp);
        System.out.println("offset for latest " + listOffsetResponse.responseData().get(tp).offset.intValue());
        assertEquals(listOffsetResponse.responseData().get(tp).error, Errors.NONE);
        assertEquals(listOffsetResponse.responseData().get(tp).offset.intValue(), totalMsgs);

        listOffsetResponse = listOffset(0, tp);
        System.out.println("offset for timestamp=0 " + listOffsetResponse.responseData().get(tp).offset.intValue());
        assertEquals(listOffsetResponse.responseData().get(tp).error, Errors.NONE);
        assertEquals(listOffsetResponse.responseData().get(tp).offset.intValue(), 0);

        listOffsetResponse = listOffset(1, tp);
        System.out.println("offset for timestamp=1 " + listOffsetResponse.responseData().get(tp).offset.intValue());
        assertEquals(listOffsetResponse.responseData().get(tp).error, Errors.NONE);
        assertEquals(listOffsetResponse.responseData().get(tp).offset.intValue(), 0);

        // when handle listOffset, result should be like:
        //  timestamp        offset
        //  2                0
        //  3                1
        //  4                1
        //  5                2
        //  6                2
        //  7                3
        //  8                3
        //  9                4
        //  10               4
        //  11               5
        //  12               5
        //  13               6
        //  14               6
        //  15               7
        //  16               7
        //  17               8
        //  18               8
        //  19               9
        //  20               9
        //  21               10

        for (int i = 0; i < totalMsgs; i++) {
            long searchTime = timestamps[i];
            listOffsetResponse = listOffset(searchTime, tp);
            assertEquals(listOffsetResponse.responseData().get(tp).error, Errors.NONE);
            assertEquals(listOffsetResponse.responseData().get(tp).offset.intValue(), i);

            searchTime++;
            listOffsetResponse = listOffset(searchTime, tp);
            assertEquals(listOffsetResponse.responseData().get(tp).offset.intValue(), i + 1);
        }
    }

    private ListOffsetResponse listOffset(long timestamp, TopicPartition tp) throws Exception {
        Map<TopicPartition, Long> targetTimes = Maps.newHashMap();
        targetTimes.put(tp, timestamp);

        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
                .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                .setTargetTimes(targetTimes);

        KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        kafkaRequestHandler
                .handleListOffsetRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();
        return (ListOffsetResponse) response;
    }

    /// Add test for FetchRequest
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
            FetchResponse.PartitionData partitionData = fetchResponse.responseData().get(tp);
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
            admin.topics().createPartitionedTopic(tName, numPartitions);

            for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
                admin.topics()
                    .createNonPartitionedTopic(tName + PARTITIONED_TOPIC_SUFFIX + partitionIndex);
                result.add(new TopicPartition(tName, partitionIndex));
            }
        }

        return result;
    }

    // get the existing topics that created by pulsar and return kafka format topicName.
    private List<String> getCreatedTopics(String topicName, int numTopics) {
        List<String> result = Lists.newArrayListWithExpectedSize(numTopics);

        for (int topicIndex = 0; topicIndex < numTopics; topicIndex++) {
            String tName = topicName + "_" + topicIndex;
            result.add(tName);
        }

        return result;
    }

    private KafkaHeaderAndRequest createTopicMetadataRequest(List<String> topics) {
        AbstractRequest.Builder builder = new MetadataRequest.Builder(topics, true);
        return buildRequest(builder);
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

    private KafkaConsumer<String, String> createKafkaConsumer(int maxWait, int minBytes) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "test_client");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost" + ":" + getKafkaBrokerPort());
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, maxWait);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, minBytes);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new KafkaConsumer<>(props);
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

    @Ignore
    @Test(timeOut = 20000)
    public void testBrokerRespectsPartitionsOrderAndSizeLimits() throws Exception {
        String topicName = "kopBrokerRespectsPartitionsOrderAndSizeLimits";
        int numberTopics = 8;
        int numberPartitions = 6;

        int messagesPerPartition = 9;
        int maxResponseBytes = 800;
        int maxPartitionBytes = 900;

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
        CompletableFuture<AbstractResponse> responseFuture1 = new CompletableFuture<>();
        kafkaRequestHandler.handleFetchRequest(fetchRequest1, responseFuture1);
        FetchResponse<MemoryRecords> fetchResponse1 =
            (FetchResponse<MemoryRecords>) responseFuture1.get();

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
        CompletableFuture<AbstractResponse> responseFuture2 = new CompletableFuture<>();
        kafkaRequestHandler.handleFetchRequest(fetchRequest2, responseFuture2);
        FetchResponse<MemoryRecords> fetchResponse2 =
            (FetchResponse<MemoryRecords>) responseFuture2.get();

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
        CompletableFuture<AbstractResponse> responseFuture3 = new CompletableFuture<>();
        kafkaRequestHandler.handleFetchRequest(fetchRequest3, responseFuture3);
        FetchResponse<MemoryRecords> fetchResponse3 =
            (FetchResponse<MemoryRecords>) responseFuture3.get();

        checkFetchResponse(shuffledTopicPartitions3, fetchResponse3,
            maxPartitionBytes, maxResponseBytes, messagesPerPartition);
    }

    // verify Metadata request handling.
    @Test(timeOut = 20000)
    public void testBrokerHandleTopicMetadataRequest() throws Exception {
        String topicName = "kopBrokerHandleTopicMetadataRequest";
        int numberTopics = 5;
        int numberPartitions = 6;

        List<String> kafkaTopics = getCreatedTopics(topicName, numberTopics);
        for (String topic : kafkaTopics) {
            admin.topics().createPartitionedTopic(topic, numberPartitions);
        }

        KafkaHeaderAndRequest metadataRequest = createTopicMetadataRequest(kafkaTopics);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        kafkaRequestHandler.handleTopicMetadataRequest(metadataRequest, responseFuture);

        MetadataResponse metadataResponse = (MetadataResponse) responseFuture.get();

        // verify all served by same broker : localhost:port
        assertEquals(metadataResponse.brokers().size(), 1);
        // NOTE: the listener's hostname is "localhost", but the advertised listener's hostname is "127.0.0.1"
        assertEquals(metadataResponse.brokers().iterator().next().host(), "127.0.0.1");

        // check metadata response
        Collection<TopicMetadata> topicMetadatas = metadataResponse.topicMetadata();

        log.debug("a. dumpTopicMetadata: ");
        topicMetadatas.forEach(topicMetadata -> {
            log.debug("      topicMetadata: {}", topicMetadata);
            log.debug("b.    dumpPartitionMetadata: ");
            topicMetadata.partitionMetadata().forEach(partition -> {
                log.debug("            PartitionMetadata: {}", partition);
            });
        });

        assertEquals(topicMetadatas.size(), numberTopics);

        topicMetadatas.forEach(topicMetadata -> {
            assertTrue(topicMetadata.topic().startsWith(topicName + "_"));
            assertEquals(topicMetadata.partitionMetadata().size(), numberPartitions);
        });
    }

    @Test(timeOut = 20000, enabled = false)
    // https://github.com/streamnative/kop/issues/51
    public void testGetOffsetsForUnknownTopic() throws Exception {
        String topicName = "kopTestGetOffsetsForUnknownTopic";

        TopicPartition tp = new TopicPartition(topicName, 0);
        Map<TopicPartition, Long> targetTimes = Maps.newHashMap();
        targetTimes.put(tp, ListOffsetRequest.LATEST_TIMESTAMP);

        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
            .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
            .setTargetTimes(targetTimes);

        KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        kafkaRequestHandler
            .handleListOffsetRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();
        ListOffsetResponse listOffsetResponse = (ListOffsetResponse) response;
        assertEquals(listOffsetResponse.responseData().get(tp).error,
            Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }
}
