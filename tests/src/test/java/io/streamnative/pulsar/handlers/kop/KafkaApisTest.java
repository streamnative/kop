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

import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_FETCH_MAX_BYTES;
import static org.apache.kafka.clients.consumer.ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest.PartitionData;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseCallbackWrapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
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
        DefaultThreadFactory defaultThreadFactory = new DefaultThreadFactory("pulsar-ph-kafka");
        EventLoopGroup dedicatedWorkerGroup =
                EventLoopUtil.newEventLoopGroup(1, false, defaultThreadFactory);
        DefaultEventLoop eventExecutors = new DefaultEventLoop(dedicatedWorkerGroup);
        kafkaRequestHandler = newRequestHandler();
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockCtx).channel();
        doReturn(eventExecutors).when(mockCtx).executor();
        kafkaRequestHandler.ctx = mockCtx;

        serviceAddress = new InetSocketAddress(pulsar.getBindAddress(), kafkaBrokerPort);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private KafkaHeaderAndRequest buildRequest(AbstractRequest.Builder builder) {
        return buildRequest(builder, serviceAddress);
    }

    static KafkaHeaderAndRequest buildRequest(AbstractRequest.Builder builder,
                                              SocketAddress serviceAddress) {
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
        PartitionData partitionOffsetCommitData = KafkaCommonTestUtils.newOffsetCommitRequestPartitionData(15L, "");
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
            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
            }
        }

        // 2. real test, for ListOffset request verify Earliest get earliest
        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
            .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
            .setTargetTimes(KafkaCommonTestUtils.newListOffsetTargetTimes(tp, ListOffsetRequest.EARLIEST_TIMESTAMP));

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
            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
            }
        }

        // 2. real test, for ListOffset request verify Earliest get earliest
        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
            .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
            .setTargetTimes(KafkaCommonTestUtils.newListOffsetTargetTimes(tp, ListOffsetRequest.LATEST_TIMESTAMP));

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

    @Test(timeOut = 60000)
    public void testFetchMaxBytes() throws Exception {
        String topicName = "testMaxBytesTopic";
        String clientId = "testClient";
        int maxBytes = 500;
        int maxPartitionBytes = 100;

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 2);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        TopicPartition tp1 = new TopicPartition(topicName, 0);
        TopicPartition tp2 = new TopicPartition(topicName, 1);
        topicPartitions.add(tp1);
        topicPartitions.add(tp2);

        // producing data and then consuming.
        KafkaProducer<String, String> kProducer = createKafkaProducer();
        produceData(kProducer, topicPartitions, 20);
        KafkaConsumer<String, String> consumer = createKafkaConsumer(5000, 1, maxBytes, maxPartitionBytes, clientId);
        consumer.assign(topicPartitions);
        consumer.seekToBeginning(topicPartitions);

        for (int i = 0; i < 3; i++) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            int fetchPartitionSize1 = records.records(tp1).stream().mapToInt((record) -> {
                return record.serializedKeySize() + record.serializedValueSize();
            }).sum();

            int fetchPartitionSize2 = records.records(tp2).stream().mapToInt((record) -> {
                return record.serializedKeySize() + record.serializedValueSize();
            }).sum();

            assertTrue(fetchPartitionSize1 <= maxPartitionBytes);
            assertTrue(fetchPartitionSize2 <= maxPartitionBytes);
            assertTrue(fetchPartitionSize1 + fetchPartitionSize2 <= maxBytes);
        }


        KafkaConsumer<String, String> consumer2 = createKafkaConsumer(5000, 1);
        consumer2.assign(topicPartitions);
        consumer2.seekToBeginning(topicPartitions);

        for (int i = 0; i < 3; i++) {
            ConsumerRecords<String, String> records = consumer2.poll(Duration.ofMillis(1000));
            int fetchPartitionSize1 = records.records(tp1).stream().mapToInt((record) -> {
                return record.serializedKeySize() + record.serializedValueSize();
            }).sum();

            int fetchPartitionSize2 = records.records(tp2).stream().mapToInt((record) -> {
                return record.serializedKeySize() + record.serializedValueSize();
            }).sum();

            if (i != 0) {
                assertTrue(fetchPartitionSize1 > maxPartitionBytes);
                assertTrue(fetchPartitionSize2 > maxPartitionBytes);
                assertTrue(fetchPartitionSize1 + fetchPartitionSize2 > maxBytes);
            }
        }
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
        ConsumerRecords<String, String> emptyResult = consumer1.poll(Duration.ofMillis(maxWaitMs));
        Long endTime1 = System.currentTimeMillis();
        log.info("cost time1:" + (endTime1 - startTime1));
        assertEquals(0, emptyResult.count());

        // case2: consuming an topic after producing data.
        @Cleanup
        KafkaProducer<String, String> kProducer = createKafkaProducer();
        produceData(kProducer, topicPartitions, 10);

        @Cleanup
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
            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
            }
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
        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
                .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                .setTargetTimes(KafkaCommonTestUtils.newListOffsetTargetTimes(tp, timestamp));

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
                Pair.of(topic, KafkaCommonTestUtils.newFetchRequestPartitionData(
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

    private KafkaConsumer<String, String> createKafkaConsumer(int maxWait, int minBytes,
                                                              int maxBytes, int maxPartitionBytes,
                                                              String clientId) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost" + ":" + getKafkaBrokerPort());
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, maxWait);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, minBytes);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, maxBytes);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionBytes);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new KafkaConsumer<>(props);
    }

    private KafkaConsumer<String, String> createKafkaConsumer(int maxWaitMs, int minBytes) {
        return createKafkaConsumer(maxWaitMs, minBytes, DEFAULT_FETCH_MAX_BYTES,
                DEFAULT_MAX_PARTITION_FETCH_BYTES, "defaultClient");
    }

    private void produceData(KafkaProducer<String, String> producer,
                             List<TopicPartition> topicPartitions,
                             int numMessagesPerPartition) throws Exception{
        for (int index = 0; index < topicPartitions.size(); index++) {
            TopicPartition tp = topicPartitions.get(index);
            for (int messageIndex = 0; messageIndex < numMessagesPerPartition; messageIndex++) {
                String suffix = tp.toString() + "-" + messageIndex;
                TimeUnit.MILLISECONDS.sleep(100);
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

        if (log.isDebugEnabled()) {
            log.debug("a. dumpTopicMetadata: ");
            topicMetadatas.forEach(topicMetadata -> {
                log.debug("      topicMetadata: {}", topicMetadata);
                log.debug("b.    dumpPartitionMetadata: ");
                topicMetadata.partitionMetadata().forEach(partition -> {
                    log.debug("            PartitionMetadata: {}", partition);
                });
            });
        }

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
        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
            .forConsumer(false, IsolationLevel.READ_UNCOMMITTED)
            .setTargetTimes(KafkaCommonTestUtils.newListOffsetTargetTimes(tp, ListOffsetRequest.LATEST_TIMESTAMP));

        KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        kafkaRequestHandler
            .handleListOffsetRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();
        ListOffsetResponse listOffsetResponse = (ListOffsetResponse) response;
        assertEquals(listOffsetResponse.responseData().get(tp).error,
            Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    @Test(timeOut = 20000)
    public void testHandleFindCoordinatorRequestWithStoreGroupIdFailed()
            throws ExecutionException, InterruptedException {
        String groupId = "test";

        KafkaRequestHandler spyHandler = spy(kafkaRequestHandler);
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(new Exception("Store failed."));
        doReturn(future).when(spyHandler).storeGroupId(eq(groupId), anyString());

        FindCoordinatorRequest.Builder builder =
                new FindCoordinatorRequest.Builder(FindCoordinatorRequest.CoordinatorType.GROUP, groupId);

        KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleFindCoordinatorRequest(request, responseFuture);

        AbstractResponse abstractResponse = responseFuture.get();
        assertNotNull(abstractResponse);
        verify(spyHandler, times(1)).findBroker(any());
    }

    @Test(timeOut = 20000)
    public void testIdempotentProduce() throws Exception {
        String namespace = "public/idempotent";
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setDeduplicationStatus(namespace, true);
        String fullTopicName = "persistent://" + namespace + "/testIdempotentProduceTopic";

        admin.topics().createPartitionedTopic(fullTopicName, 1);

        Properties producerProperties = newKafkaProducerProperties();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            producer.send(new ProducerRecord<>(fullTopicName, "test"));
        }
        final TopicPartition topicPartition = new TopicPartition(fullTopicName, 0);

        // single message
        verifySendMessageToPartition(topicPartition,
                newIdempotentRecords(0, (short) 0, 0, 1), Errors.NONE, 1);
        verifySendMessageToPartition(topicPartition,
                newIdempotentRecords(0, (short) 0, 0, 1), Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1);
        verifySendMessageToPartition(topicPartition,
                newIdempotentRecords(0, (short) 0, 1, 1), Errors.NONE, 2);
        verifySendMessageToPartition(topicPartition,
                newIdempotentRecords(1, (short) 0, 0, 1), Errors.NONE, 3);
        verifySendMessageToPartition(topicPartition,
                newIdempotentRecords(1, (short) 1, 0, 1), Errors.NONE, 4);

        // batch message
        verifySendMessageToPartition(topicPartition,
                newIdempotentRecords(2, (short) 0, 0, 10), Errors.NONE, 5);
        verifySendMessageToPartition(topicPartition,
                newIdempotentRecords(2, (short) 0, 10, 10), Errors.NONE, 15);
        verifySendMessageToPartition(topicPartition,
                newIdempotentRecords(2, (short) 0, 10, 10), Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, -1);
    }

    private void verifySendMessageToPartition(final TopicPartition topicPartition,
                                              final MemoryRecords records,
                                              final Errors expectedError,
                                              final long expectedOffset)
            throws ExecutionException, InterruptedException {
        final KafkaHeaderAndRequest request = buildRequest(ProduceRequest.Builder.forCurrentMagic(
                (short) -1, 30000, Collections.singletonMap(topicPartition, records)));
        final CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
        kafkaRequestHandler.handleProduceRequest(request, future);
        final ProduceResponse.PartitionResponse response =
                ((ProduceResponse) future.get()).responses().get(topicPartition);
        assertNotNull(response);
        assertEquals(response.error, expectedError);
        assertEquals(response.baseOffset, expectedOffset);
    }

    private static MemoryRecords newIdempotentRecords(
            long producerId, short producerEpoch, int baseSequence, int recordsNum) {
        final MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024),
                CompressionType.NONE,
                0L,
                producerId,
                producerEpoch,
                baseSequence,
                false);
        for (int i = 0; i < recordsNum; i++) {
            builder.append(System.currentTimeMillis(), null, "msg".getBytes(StandardCharsets.UTF_8));
        }
        return builder.build();
    }

    private static MemoryRecords newNormalRecords() {
        final MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024),
                RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0L);
        builder.append(System.currentTimeMillis(), null, "msg".getBytes(StandardCharsets.UTF_8));
        return builder.build();
    }

    private static MemoryRecords newAbortTxnMarker() {
        final MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024),
                RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0L,
                0L,
                System.currentTimeMillis(),
                (short) 0,
                0,
                true,
                true/* isControlBatch */,
                0);
        builder.appendEndTxnMarker(System.currentTimeMillis(), new EndTransactionMarker(ControlRecordType.ABORT, 0));
        return builder.build();
    }

    @Test(timeOut = 20000)
    public void testIllegalManagedLedger() throws Exception {
        final String topic = "testIllegalManagedLedger";
        admin.topics().createPartitionedTopic(topic, 1);

        final TopicPartition topicPartition = new TopicPartition(topic, 0);

        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());
        // Trigger the creation of PersistentTopic
        final RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, "hello")).get();
        assertEquals(metadata.offset(), 0);

        verifySendMessageToPartition(topicPartition, newNormalRecords(), Errors.NONE, 1L);
        verifySendMessageToPartition(topicPartition, newAbortTxnMarker(), Errors.NONE, 2L);

        final Optional<Topic> optionalTopic = pulsar.getBrokerService()
                .getTopicIfExists("persistent://public/default/" + topic + "-partition-0")
                .get();
        assertTrue(optionalTopic.isPresent());
        final PersistentTopic persistentTopic = (PersistentTopic) optionalTopic.get();
        persistentTopic.getManagedLedger().close();
        // Now, the managed ledger is closed
        verifySendMessageToPartition(topicPartition, newNormalRecords(), Errors.NOT_LEADER_FOR_PARTITION, -1L);
        verifySendMessageToPartition(topicPartition, newAbortTxnMarker(), Errors.NOT_LEADER_FOR_PARTITION, -1L);
    }


    /**
     * Test the sending speed of fetch request when the readable data is less than fetch.minBytes.
     */
    @Test(timeOut = 60000)
    public void testFetchMinBytesSingleConsumer() throws Exception {
        final String topic = "testMinBytesTopic";
        final TopicPartition topicPartition = new TopicPartition(topic, 0);
        admin.topics().createPartitionedTopic(topic, 1);
        triggerTopicLookup(topic, 1);
        kafkaRequestHandler.getTopicManager().setRemoteAddress(new InetSocketAddress(42));
        final int maxWaitMs = 3000;
        final int minBytes = 1;

        @Cleanup
        final KafkaHeaderAndRequest request = buildRequest(FetchRequest.Builder.forConsumer(maxWaitMs, minBytes,
                Collections.singletonMap(topicPartition, new FetchRequest.PartitionData(
                        0L, -1L, 1024 * 1024, Optional.empty()
                ))));
        final CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
        final long startTime = System.currentTimeMillis();
        kafkaRequestHandler.handleFetchRequest(request, future);

        // Trigger the fetch
        final int numMessages = 10;
        final KafkaProducer<String, String> producer = createKafkaProducer();
        produceData(producer, Collections.singletonList(new TopicPartition(topic, 0)), numMessages);
        AbstractResponse abstractResponse = ((ResponseCallbackWrapper)
                future.get(maxWaitMs + 1000, TimeUnit.MILLISECONDS)).getResponse();
        assertTrue(abstractResponse instanceof FetchResponse);
        final FetchResponse<MemoryRecords> response = (FetchResponse<MemoryRecords>) abstractResponse;
        assertEquals(response.error(), Errors.NONE);
        final long endTime = System.currentTimeMillis();
        log.info("Take {} ms to process FETCH request, record count: {}",
                endTime - startTime, response.responseData().size());
        assertTrue(endTime - startTime <= maxWaitMs);

        Long waitingFetchesTriggered = kafkaRequestHandler.getRequestStats().getWaitingFetchesTriggered().get();
        assertEquals((long) waitingFetchesTriggered, 1);
    }
}
