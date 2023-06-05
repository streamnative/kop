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


import static io.streamnative.pulsar.handlers.kop.KafkaCommonTestUtils.getListOffsetsPartitionResponse;
import static io.streamnative.pulsar.handlers.kop.utils.TopicNameUtils.getPartitionedTopicNameWithoutPartitions;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndResponse;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataManager;
import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import io.streamnative.pulsar.handlers.kop.utils.KafkaResponseUtils;
import io.streamnative.pulsar.handlers.kop.utils.TopicNameUtils;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.KopResponseUtils;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for {@link KafkaRequestHandler}.
 */
@Slf4j
public class KafkaRequestHandlerTest extends KopProtocolHandlerTestBase {

    private KafkaRequestHandler handler;

    @DataProvider(name = "metadataVersions")
    public static Object[][] metadataVersions() {
        return new Object[][]{ { (short) 0 }, { (short) 1 } };
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setDefaultNumPartitions(2);
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.namespaces().getNamespaces("public").contains("public/__kafka")) {
            admin.namespaces().createNamespace("public/__kafka");
            admin.namespaces().setNamespaceReplicationClusters("public/__kafka", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/__kafka",
                new RetentionPolicies(-1, -1));
        }

        admin.tenants().createTenant("my-tenant",
                TenantInfo.builder()
                        .adminRoles(Collections.emptySet())
                        .allowedClusters(Collections.singleton(configClusterName))
                        .build());
        admin.namespaces().createNamespace("my-tenant/my-ns");

        log.info("created namespaces, init handler");

        handler = newRequestHandler();
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockCtx).channel();
        handler.ctx = mockCtx;
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testByteBufToRequest() {
        int correlationId = 7777;
        String clientId = "KopClientId";

        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest.Builder().build();
        RequestHeader header = new RequestHeader(
            ApiKeys.API_VERSIONS,
            ApiKeys.API_VERSIONS.latestVersion(),
            clientId,
            correlationId);

        // 1. serialize request into ByteBuf
        ByteBuffer serializedRequest = KopResponseUtils.serializeRequest(header, apiVersionsRequest);
        int size = serializedRequest.remaining();
        ByteBuf inputBuf = Unpooled.buffer(size);
        inputBuf.writeBytes(serializedRequest);

        // 2. turn Bytebuf into KafkaHeaderAndRequest.
        KafkaHeaderAndRequest request = handler.byteBufToRequest(inputBuf, null);

        // 3. verify byteBufToRequest works well.
        assertEquals(request.getHeader().data(), header.data());
        assertTrue(request.getRequest() instanceof ApiVersionsRequest);
    }


    @Test
    public void testResponseToByteBuf() {
        int correlationId = 7777;
        String clientId = "KopClientId";

        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest.Builder().build();
        RequestHeader requestHeader = new RequestHeader(
            ApiKeys.API_VERSIONS,
            ApiKeys.API_VERSIONS.latestVersion(),
            clientId,
            correlationId);

        KafkaHeaderAndRequest kopRequest = new KafkaHeaderAndRequest(
            requestHeader,
            apiVersionsRequest,
            Unpooled.buffer(20),
            null);

        ApiVersionsResponse apiVersionsResponse = ApiVersionsResponse
                .defaultApiVersionsResponse(ApiMessageType.ListenerType.BROKER);
        KafkaHeaderAndResponse kopResponse = KafkaHeaderAndResponse.responseForRequest(
            kopRequest, apiVersionsResponse);

        // 1. serialize response into ByteBuf
        ByteBuf serializedResponse = KafkaCommandDecoder.responseToByteBuf(kopResponse.getResponse(), kopRequest, true);

        // 2. verify responseToByteBuf works well.
        ByteBuffer byteBuffer = serializedResponse.nioBuffer();
        ResponseHeader responseHeader = ResponseHeader.parse(byteBuffer, kopResponse.getHeader().headerVersion());
        assertEquals(responseHeader.correlationId(), correlationId);

        ApiVersionsResponse parsedResponse = ApiVersionsResponse.parse(
            byteBuffer, kopResponse.getApiVersion());

        assertEquals(parsedResponse.data().apiKeys().size(), apiVersionsResponse.data().apiKeys().size());
    }

    @Test
    public void testNewNode() {
        String host = "192.168.168.168";
        int port = 7777;
        InetSocketAddress socketAddress = new InetSocketAddress(host, port);
        Node node = KafkaRequestHandler.newNode(socketAddress);

        assertEquals(node.host(), host);
        assertEquals(node.port(), port);
    }

    @Test
    public void testNewPartitionMetadata() {
        String host = "192.168.168.168";
        int port = 7777;
        int partitionIndex = 7;
        InetSocketAddress socketAddress = new InetSocketAddress(host, port);
        Node node = KafkaRequestHandler.newNode(socketAddress);
        TopicName topicName = TopicName.get("persistent://test-tenants/test-ns/topicName");
        TopicName topicNamePartition =
            TopicName.get("persistent://test-tenants/test-ns/topic" + PARTITIONED_TOPIC_SUFFIX + partitionIndex);

        KafkaResponseUtils.BrokerLookupResult metadata = KafkaRequestHandler.newPartitionMetadata(topicName, node);
        assertEquals(metadata.error, Errors.NONE);
        assertEquals(metadata.getTopicPartition().partition(), 0);


        metadata = KafkaRequestHandler.newPartitionMetadata(topicNamePartition, node);
        assertEquals(metadata.error, Errors.NONE);
        assertEquals(metadata.getTopicPartition().partition(), partitionIndex);

        metadata = KafkaRequestHandler.newFailedPartitionMetadata(topicName);
        assertEquals(metadata.error, Errors.NOT_LEADER_OR_FOLLOWER);
        assertEquals(metadata.getTopicPartition().partition(), 0);


        metadata = KafkaRequestHandler.newFailedPartitionMetadata(topicNamePartition);
        assertEquals(metadata.error, Errors.NOT_LEADER_OR_FOLLOWER);
        assertEquals(metadata.getTopicPartition().partition(), partitionIndex);
    }

    @Test
    public void testGetPartitionedNameWithoutPartition() {
        String localName = "topicName";
        String topicString = "persistent://test-tenants/test-ns/" + localName;
        int partitionIndex = 7;

        TopicName topicName = TopicName.get(topicString);
        TopicName topicNamePartition =
            TopicName.get(topicString + PARTITIONED_TOPIC_SUFFIX + partitionIndex);

        assertEquals(topicString, getPartitionedTopicNameWithoutPartitions(topicName));
        assertEquals(topicString, getPartitionedTopicNameWithoutPartitions(topicNamePartition));
    }

    private void createTopicsByKafkaAdmin(AdminClient admin, Map<String, Integer> topicToNumPartitions)
            throws ExecutionException, InterruptedException {
        final short replicationFactor = 1; // replication factor will be ignored
        admin.createTopics(topicToNumPartitions.entrySet().stream().map(entry -> {
            final String topic = entry.getKey();
            final int numPartitions = entry.getValue();
            return new NewTopic(topic, numPartitions, replicationFactor);
        }).collect(Collectors.toList())).all().get();
    }

    private void verifyTopicsCreatedByPulsarAdmin(Map<String, Integer> topicToNumPartitions)
            throws PulsarAdminException {
        for (Map.Entry<String, Integer> entry : topicToNumPartitions.entrySet()) {
            final String topic = entry.getKey();
            final int numPartitions = entry.getValue();
            assertEquals(this.admin.topics().getPartitionedTopicMetadata(topic).partitions, numPartitions);
        }
    }

    private void verifyTopicsDeletedByPulsarAdmin(Map<String, Integer> topicToNumPartitions)
            throws PulsarAdminException {
        for (Map.Entry<String, Integer> entry : topicToNumPartitions.entrySet()) {
            final String topic = entry.getKey();
            try {
                admin.topics().getPartitionedTopicMetadata(topic);
                fail("getPartitionedTopicMetadata should fail if topic doesn't exist");
            } catch (PulsarAdminException.NotFoundException expected) {
            }
        }
    }

    private void deleteTopicsByKafkaAdmin(AdminClient admin, Set<String> topicsToDelete)
            throws ExecutionException, InterruptedException {
        admin.deleteTopics(topicsToDelete).all().get();
    }


    @Test(timeOut = 10000)
    public void testCreateAndDeleteTopics() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(props);
        Map<String, Integer> topicToNumPartitions = new HashMap<>() {{
            put("testCreateTopics-0", 1);
            put("testCreateTopics-1", 3);
            put("my-tenant/my-ns/testCreateTopics-2", 1);
            put("persistent://my-tenant/my-ns/testCreateTopics-3", 5);
        }};
        // create
        createTopicsByKafkaAdmin(kafkaAdmin, topicToNumPartitions);
        verifyTopicsCreatedByPulsarAdmin(topicToNumPartitions);
        // delete
        deleteTopicsByKafkaAdmin(kafkaAdmin, topicToNumPartitions.keySet());
        verifyTopicsDeletedByPulsarAdmin(topicToNumPartitions);
    }

    @Test(timeOut = 20000)
    public void testCreateInvalidTopics() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(props);
        Map<String, Integer> topicToNumPartitions = Collections.singletonMap("xxx/testCreateInvalidTopics-0", 1);
        try {
            createTopicsByKafkaAdmin(kafkaAdmin, topicToNumPartitions);
            fail("create a invalid topic should fail");
        } catch (Exception e) {
            log.info("Failed to create topics: {} caused by {}", topicToNumPartitions, e.getCause());
            assertTrue(e.getCause() instanceof UnknownServerException);
        }
        topicToNumPartitions = Collections.singletonMap("testCreateInvalidTopics-1", -1234);
        try {
            createTopicsByKafkaAdmin(kafkaAdmin, topicToNumPartitions);
            fail("create a invalid topic should fail");
        } catch (Exception e) {
            log.info("Failed to create topics: {} caused by {}", topicToNumPartitions, e.getCause());
            assertTrue(e.getCause() instanceof InvalidRequestException);
        }
    }

    @Test(timeOut = 10000)
    public void testCreateExistedTopic() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(props);
        final Map<String, Integer> topicToNumPartitions = Collections.singletonMap("testCreatedExistedTopic", 1);
        try {
            createTopicsByKafkaAdmin(kafkaAdmin, topicToNumPartitions);
        } catch (ExecutionException | InterruptedException e) {
            fail(e.getMessage());
        }
        try {
            createTopicsByKafkaAdmin(kafkaAdmin, topicToNumPartitions);
            fail("Create the existed topic should fail");
        } catch (ExecutionException e) {
            log.info("Failed to create existed topic: {}", e.getMessage());
            assertTrue(e.getCause() instanceof TopicExistsException);
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
    }

    @Test(timeOut = 10000)
    public void testCreateTopicWithDefaultPartitions() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        final String topic = "testCreatedTopicWithDefaultPartitions";

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(props);
        final Map<String, Integer> topicToNumPartitions = Collections.singletonMap(
                topic,
                CreateTopicsRequest.NO_NUM_PARTITIONS);
        createTopicsByKafkaAdmin(kafkaAdmin, topicToNumPartitions);
        assertEquals(admin.topics().getPartitionedTopicMetadata(topic).partitions, conf.getDefaultNumPartitions());
    }

    @Test(timeOut = 10000)
    public void testDeleteNotExistedTopics() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(props);
        Set<String> topics = new HashSet<>();
        topics.add("testDeleteNotExistedTopics");
        try {
            deleteTopicsByKafkaAdmin(kafkaAdmin, topics);
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof UnknownTopicOrPartitionException);
        }
    }

    @Test(timeOut = 10000)
    public void testDescribeTopics() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(props);

        final String topicNotExisted = "testDescribeTopics-topic-not-existed";
        try {
            kafkaAdmin.describeTopics(new HashSet<>(Collections.singletonList(topicNotExisted))).all().get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof UnknownTopicOrPartitionException);
        }

        final Map<String, Integer> expectedTopicPartitions = new HashMap<>() {{
            put("testDescribeTopics-topic-1", 1);
            put("testDescribeTopics-topic-2", 3);
        }};
        for (Map.Entry<String, Integer> entry : expectedTopicPartitions.entrySet()) {
            admin.topics().createPartitionedTopic(entry.getKey(), entry.getValue());
        }

        final Map<String, Integer> result = kafkaAdmin
                .describeTopics(expectedTopicPartitions.keySet())
                .all().get().entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().partitions().size()
                ));
        assertEquals(result, expectedTopicPartitions);
    }

    @Test(timeOut = 10000)
    public void testProduceCallback() throws Exception {
        final String topic = "test-produce-callback";
        final int numMessages = 10;
        final String messagePrefix = "msg-";

        admin.topics().createPartitionedTopic(topic, 1);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        @Cleanup
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        final CountDownLatch latch = new CountDownLatch(numMessages);
        final List<Long> offsets = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            final int index = i;
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, i, messagePrefix + i),
                    (recordMetadata, e) -> {
                        if (e != null) {
                            log.error("Failed to send {}: {}", index, e);
                            offsets.add(-1L);
                        } else {
                            offsets.add(recordMetadata.offset());
                        }
                        latch.countDown();
                    });
            // The first half messages are sent in batch, the second half messages are sent synchronously.
            if (i >= numMessages / 2) {
                future.get();
            }
        }
        latch.await();
        final List<Long> expectedOffsets = LongStream.range(0, numMessages).boxed().collect(Collectors.toList());
        log.info("Actual offsets: {}", offsets);
        assertEquals(offsets, expectedOffsets);
    }

    @Test(timeOut = 10000)
    public void testConvertOffsetCommitRetentionMsIfSetDefaultValue() {

        String memberId = "test_member_id";
        int generationId = 0;
        long currentTime = 100;
        int configRetentionMs = 1000;
        TopicPartition topicPartition = new TopicPartition("test", 1);

        // build input params
        OffsetCommitRequestData.OffsetCommitRequestTopic offsetCommitRequestTopic =
                KafkaCommonTestUtils.newOffsetCommitRequestPartitionData(topicPartition, 1L, "");

        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(new OffsetCommitRequestData()
                .setGenerationId(generationId)
                .setMemberId(memberId)
                .setGroupId("test-groupId")
                .setTopics(Collections.singletonList(offsetCommitRequestTopic)));

        OffsetCommitRequest offsetCommitRequest = builder.build();

        Map<TopicPartition, OffsetCommitRequestData.OffsetCommitRequestPartition> offsetData =
                ImmutableMap.of(topicPartition, offsetCommitRequestTopic.partitions().get(0));

        // convert
        Map<TopicPartition, OffsetAndMetadata> converted =
                handler.convertOffsetCommitRequestRetentionMs(offsetData,
                        offsetCommitRequest.data().retentionTimeMs(),
                        builder.latestAllowedVersion(),
                        currentTime,
                        configRetentionMs);

        OffsetAndMetadata convertedOffsetAndMetadata = converted.get(topicPartition);

        // verify
        Assert.assertEquals(convertedOffsetAndMetadata.commitTimestamp(), currentTime);
        Assert.assertEquals(convertedOffsetAndMetadata.expireTimestamp(), currentTime + configRetentionMs);

    }

    @Test(timeOut = 10000)
    public void testConvertOffsetCommitRetentionMsIfRetentionMsSet() {

        long currentTime = 100;
        int offsetsConfigRetentionMs = 1000;
        int requestSetRetentionMs = 10000;
        TopicPartition topicPartition = new TopicPartition("test", 1);

        // build input params
        Map<TopicPartition, OffsetCommitRequestData.OffsetCommitRequestPartition> offsetData = new HashMap<>();
        OffsetCommitRequestData.OffsetCommitRequestTopic offsetCommitRequestTopic =
                KafkaCommonTestUtils.newOffsetCommitRequestPartitionData(topicPartition, 1L, "");
        offsetData.put(topicPartition, offsetCommitRequestTopic.partitions().get(0));

        // convert
        Map<TopicPartition, OffsetAndMetadata> converted =
                handler.convertOffsetCommitRequestRetentionMs(
                        offsetData,
                        requestSetRetentionMs,
                        (short) 4, // V2 adds retention time to the request and V5 removes retention time
                        currentTime,
                        offsetsConfigRetentionMs);

        OffsetAndMetadata convertedOffsetAndMetadata = converted.get(topicPartition);

        // verify
        Assert.assertEquals(convertedOffsetAndMetadata.commitTimestamp(), currentTime);
        Assert.assertEquals(convertedOffsetAndMetadata.expireTimestamp(), currentTime + requestSetRetentionMs);

    }

    // test for
    // https://github.com/streamnative/kop/issues/303
    @Test(timeOut = 10000)
    public void testOffsetCommitRequestRetentionMs() throws Exception {
        String group = "test-groupId";
        String memberId = "test_member_id";
        int generationId = -1; // use for avoid mock group state and member
        TopicPartition topicPartition = new TopicPartition("test", 1);

        // build input params
        Map<TopicPartition, OffsetCommitRequestData.OffsetCommitRequestPartition> offsetData = new HashMap<>();
        OffsetCommitRequestData.OffsetCommitRequestTopic offsetCommitRequestTopic =
                KafkaCommonTestUtils.newOffsetCommitRequestPartitionData(topicPartition, 1L, "");
        offsetData.put(topicPartition, offsetCommitRequestTopic.partitions().get(0));
        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(new OffsetCommitRequestData()
                .setGenerationId(generationId)
                .setMemberId(memberId)
                .setGroupId("test-groupId")
                .setTopics(Collections.singletonList(offsetCommitRequestTopic)));
        OffsetCommitRequest offsetCommitRequest = builder.build();

        RequestHeader header = new RequestHeader(ApiKeys.OFFSET_COMMIT, offsetCommitRequest.version(),
                "", 0);
        KafkaHeaderAndRequest headerAndRequest = new KafkaHeaderAndRequest(header,
                offsetCommitRequest, PulsarByteBufAllocator.DEFAULT.heapBuffer(), null);

        // handle request
        CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
        handler.handleOffsetCommitRequest(headerAndRequest, future);

        // wait for save offset
        future.get();

        // verify
        GroupMetadataManager groupMetadataManager = handler.getGroupCoordinator().getGroupManager();
        GroupMetadata metadata = groupMetadataManager.getGroup(group).get();
        OffsetAndMetadata offsetAndMetadata = metadata.offset(topicPartition, handler.currentNamespacePrefix()).get();

        // offset in cache
        Assert.assertNotNull(offsetAndMetadata);

        // trigger clean expire offset logic
        Map<TopicPartition, OffsetAndMetadata> removeExpiredOffsets =
                metadata.removeExpiredOffsets(Time.SYSTEM.milliseconds());

        // there is only one offset just saved. it should not being removed.
        Assert.assertTrue(removeExpiredOffsets.isEmpty(),
                "expect no expired offset. but " + removeExpiredOffsets + " expired.");

        metadata = groupMetadataManager.getGroup(group).get();
        offsetAndMetadata = metadata.offset(topicPartition, handler.currentNamespacePrefix()).get();

        // not cleanup
        Assert.assertNotNull(offsetAndMetadata);

    }

    @Test(timeOut = 10000)
    public void testListOffsetsForNotExistedTopic() throws Exception {
        final TopicPartition topicPartition = new TopicPartition("testListOffsetsForNotExistedTopic", 0);
        final CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        final RequestHeader header =
                new RequestHeader(ApiKeys.LIST_OFFSETS, ApiKeys.LIST_OFFSETS.latestVersion(), "client", 0);
        final ListOffsetsRequest request =
                ListOffsetsRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                        .setTargetTimes(KafkaCommonTestUtils
                                .newListOffsetTargetTimes(topicPartition, ListOffsetsRequest.EARLIEST_TIMESTAMP))
                        .build(ApiKeys.LIST_OFFSETS.latestVersion());
        handler.handleListOffsetRequest(
                new KafkaHeaderAndRequest(header, request, PulsarByteBufAllocator.DEFAULT.heapBuffer(), null),
                responseFuture);
        final ListOffsetsResponse response = (ListOffsetsResponse) responseFuture.get();
        ListOffsetsResponseData.ListOffsetsPartitionResponse listOffsetsPartitionResponse =
                getListOffsetsPartitionResponse(topicPartition, response.data());
        assertEquals(listOffsetsPartitionResponse.errorCode(), Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }

    @Test(timeOut = 10000, dataProvider = "metadataVersions")
    public void testMetadataForNonPartitionedTopic(short version) throws Exception {
        final String topic = "testMetadataForNonPartitionedTopic-" + version;
        admin.topics().createNonPartitionedTopic(topic);

        final RequestHeader header = new RequestHeader(ApiKeys.METADATA, version, "client", 0);
        MetadataRequestData data = new MetadataRequestData()
                .setTopics(Collections.singletonList(new MetadataRequestData.MetadataRequestTopic()
                        .setName(topic)))
                .setAllowAutoTopicCreation(false);
        // TO NOT USE the MetadataRequest.Builder, otherwise you cannot use version = 0
        final MetadataRequest request = new MetadataRequest(data, version);
        final CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        handler.handleTopicMetadataRequest(
                new KafkaHeaderAndRequest(header, request, PulsarByteBufAllocator.DEFAULT.heapBuffer(), null),
                responseFuture);
        final MetadataResponse response = (MetadataResponse) responseFuture.get();
        assertEquals(response.topicMetadata().size(), 1);
        assertEquals(response.errors().size(), 0);
    }

    @Test(timeOut = 10000)
    public void testDeleteTopicsAndCheckChildPath() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(props);
        Map<String, Integer> topicToNumPartitions = new HashMap<>() {{
            put("testCreateTopics-0", 1);
            put("testCreateTopics-1", 3);
            put("my-tenant/my-ns/testCreateTopics-2", 1);
            put("persistent://my-tenant/my-ns/testCreateTopics-3", 5);
        }};
        // create
        createTopicsByKafkaAdmin(kafkaAdmin, topicToNumPartitions);
        verifyTopicsCreatedByPulsarAdmin(topicToNumPartitions);
        // delete
        deleteTopicsByKafkaAdmin(kafkaAdmin, topicToNumPartitions.keySet());
        verifyTopicsDeletedByPulsarAdmin(topicToNumPartitions);
        // check deleted topics path
        Set<String> deletedTopics = handler.getPulsarService()
                .getBrokerService()
                .getPulsar()
                .getLocalMetadataStore()
                .getChildren(KopEventManager.getDeleteTopicsPath())
                .join()
                .stream()
                .map((TopicNameUtils::getTopicNameWithUrlDecoded))
                .collect(Collectors.toSet());

        assertEquals(topicToNumPartitions.keySet(), deletedTopics);
    }

    @Test
    public void testEmptyReplacingIndex() {
        final String namespace = "public/default";
        final String topic = "test-topic";

        // 1. original tp
        final TopicPartition tp0 = new TopicPartition(namespace + "/" + topic, 0);

        // 2. full topic and tp
        final String fullNameTopic = "persistent://" + namespace + "/" + topic;
        final TopicPartition fullTp0 = new TopicPartition(fullNameTopic, 0);

        final HashMap<TopicPartition, String> replacedMap = Maps.newHashMap();
        // 3. before replace, replacedMap has a fullName tp
        replacedMap.put(fullTp0, "");

        // 4. replacingIndex is an empty map
        final Map<TopicPartition, TopicPartition> emptyReplacingIndex = Collections.emptyMap();

        handler.replaceTopicPartition(replacedMap, emptyReplacingIndex);

        assertEquals(1, replacedMap.size());

        // 5. after replace, replacedMap has a short topic name
        replacedMap.forEach(((topicPartition, s) -> assertEquals(topicPartition, tp0)));
    }

    @Test
    public void testNonEmptyReplacingIndex() {
        final String namespace = "public/default";
        final String topic = "test-topic";

        // 1. original tp
        final TopicPartition tp0 = new TopicPartition(namespace + "/" + topic, 0);

        // 2. full topic and tp
        final String fullNameTopic = "persistent://" + namespace + "/" + topic;
        final TopicPartition fullTp0 = new TopicPartition(fullNameTopic, 0);

        final HashMap<TopicPartition, String> replacedMap = Maps.newHashMap();
        // 3. before replace, replacedMap has a fullName tp
        replacedMap.put(fullTp0, "");

        // 4. replacingIndex is not an empty map
        final Map<TopicPartition, TopicPartition> nonEmptyReplacingIndex = Maps.newHashMap();
        nonEmptyReplacingIndex.put(fullTp0, tp0);

        handler.replaceTopicPartition(replacedMap, nonEmptyReplacingIndex);

        assertEquals(1, replacedMap.size());

        // 5. after replace, replacedMap has a short topic name
        replacedMap.forEach(((topicPartition, s) -> assertEquals(topicPartition, tp0)));
    }

    @Test(timeOut = 20000)
    public void testCreatePartitionsForNonExistedTopic() throws Exception {
        final String topic = "test-create-partitions-existed";

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(newKafkaAdminClientProperties());

        HashMap<String, NewPartitions> newPartitionsMap = Maps.newHashMap();
        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        newPartitionsMap.put(topic, newPartitions);

        try {
            kafkaAdmin.createPartitions(newPartitionsMap).all().get();
            fail("should have failed");
        } catch (ExecutionException e) {
            assertTrue((e.getCause() instanceof UnknownTopicOrPartitionException));
            assertTrue(e.getMessage().contains("Topic '" + topic + "' doesn't exist."));
        }

    }

    @Test(timeOut = 20000)
    public void testCreatePartitionsWithNegative() throws Exception {
        final String topic = "test-create-partitions-negative";
        final int oldPartitions = 5;
        NewTopic newTopic = new NewTopic(topic, oldPartitions, (short) 1);

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(newKafkaAdminClientProperties());

        kafkaAdmin.createTopics(Collections.singleton(newTopic)).all().get();

        HashMap<String, NewPartitions> newPartitionsMap = Maps.newHashMap();
        final int numPartitions = -1;
        NewPartitions newPartitions = NewPartitions.increaseTo(numPartitions);
        newPartitionsMap.put(topic, newPartitions);

        try {
            kafkaAdmin.createPartitions(newPartitionsMap).all().get();
            fail("should have failed");
        } catch (ExecutionException e) {
            assertTrue((e.getCause() instanceof InvalidPartitionsException));
            assertTrue(e.getMessage().contains("The partition '" + numPartitions + "' is negative"));
        }

    }

    @Test(timeOut = 20000)
    public void testCreatePartitionsWithDecrease() throws Exception {
        final String topic = "test-create-partitions-decrease";
        final int oldPartitions = 5;
        NewTopic newTopic = new NewTopic(topic, oldPartitions, (short) 1);

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(newKafkaAdminClientProperties());

        kafkaAdmin.createTopics(Collections.singleton(newTopic)).all().get();

        HashMap<String, NewPartitions> newPartitionsMap = Maps.newHashMap();
        final int numPartitions = 2;
        NewPartitions newPartitions = NewPartitions.increaseTo(numPartitions);
        newPartitionsMap.put(topic, newPartitions);

        try {
            kafkaAdmin.createPartitions(newPartitionsMap).all().get();
            fail("should have failed");
        } catch (ExecutionException e) {
            assertTrue((e.getCause() instanceof InvalidPartitionsException));
            assertTrue(e.getMessage().contains("Topic currently has '" + oldPartitions + "' partitions, "
                    + "which is higher than the requested '" + numPartitions + "'."));
        }

    }

    @Test(timeOut = 20000)
    public void testCreatePartitionsWithAssignment() throws Exception {
        final String topic = "test-create-partitions-assignment";
        final int oldPartitions = 5;
        NewTopic newTopic = new NewTopic(topic, oldPartitions, (short) 1);

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(newKafkaAdminClientProperties());

        kafkaAdmin.createTopics(Collections.singleton(newTopic)).all().get();

        HashMap<String, NewPartitions> newPartitionsMap = Maps.newHashMap();
        final int numPartitions = 7;
        ArrayList<List<Integer>> assignments = Lists.newArrayList();
        assignments.add(Collections.singletonList(1000));
        assignments.add(Collections.singletonList(1001));
        NewPartitions newPartitions = NewPartitions.increaseTo(numPartitions, assignments);
        newPartitionsMap.put(topic, newPartitions);

        try {
            kafkaAdmin.createPartitions(newPartitionsMap).all().get();
            fail("should have failed");
        } catch (ExecutionException e) {
            log.error("Error is", e);
            assertTrue((e.getCause() instanceof InvalidRequestException));
            String expected = "Kop server currently doesn't support manual assignment replica sets '"
                    + newPartitions.assignments() + "' the number of partitions must be specified ";
            assertTrue(e.getMessage()
                    .contains(expected), "Message '" + e.getMessage() + "' does not contain '" + expected + "'");
        }

    }

    @Test(timeOut = 20000)
    public void testCreatePartitions() throws ExecutionException, InterruptedException {
        final String topic = "test-create-partitions-success";
        final int oldPartitions = 5;
        NewTopic newTopic = new NewTopic(topic, oldPartitions, (short) 1);

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(newKafkaAdminClientProperties());

        kafkaAdmin.createTopics(Collections.singleton(newTopic)).all().get();

        HashMap<String, NewPartitions> newPartitionsMap = Maps.newHashMap();
        final int numPartitions = 10;
        NewPartitions newPartitions = NewPartitions.increaseTo(numPartitions);
        newPartitionsMap.put(topic, newPartitions);

        kafkaAdmin.createPartitions(newPartitionsMap).all().get();
        Map<String, TopicDescription> topicDescriptionMap =
                kafkaAdmin.describeTopics(Collections.singletonList(topic)).all().get();
        assertTrue(topicDescriptionMap.containsKey(topic));
        assertEquals(numPartitions, topicDescriptionMap.get(topic).partitions().size());

    }

    @Test
    public void testMaxMessageSize() throws PulsarAdminException {
        String topicName = "testMaxMessageSizeTopic";

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 1);
        TopicPartition tp = new TopicPartition(topicName, 0);

        // producing data and then consuming.
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost" + ":" + getKafkaBrokerPort());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "testMaxMessageSize");
        //set max request size 7M
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "7340124");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        Throwable causeException = null;
        //send record size is 3M
        try {
            producer.send(new ProducerRecord<>(
                    tp.topic(),
                    tp.partition(),
                    "null",
                    new byte[1024 * 1024 * 3])).get();
        } catch (Throwable e) {
            causeException = e.getCause();
        }
        assertNull(causeException);

        //send record size is 6M > default 5M
        try {
            producer.send(new ProducerRecord<>(
                    tp.topic(),
                    tp.partition(),
                    "null",
                    new byte[1024 * 1024 * 6])
            ).get();
        } catch (Throwable e) {
            causeException = e.getCause();
        }
        assertNotNull(causeException);
        assertTrue(causeException instanceof RecordTooLargeException);
    }

    @Test
    public void testNetworkMetrics() throws Exception {
        String topicName = "testNetworkMetrics";

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 1);
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost" + ":" + getKafkaBrokerPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        @Cleanup
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>(topicName, "key", "value")).get();

        KafkaProtocolHandler protocolHandler = getProtocolHandler();
        long bytesIn = protocolHandler.getRequestStats().getNetworkTotalBytesIn().get();
        long bytesOut = protocolHandler.getRequestStats().getNetworkTotalBytesOut().get();

        assertTrue(bytesIn > 0);
        assertTrue(bytesOut > 0);
    }


    @DataProvider(name = "allowAutoTopicCreation")
    public static Object[][] allowAutoTopicCreation() {
        return new Object[][]{
                { true, true, true },
                { true, true, false },
                { true, false, true },
                { true, false, false },
                { true, null, true },
                { true, null, false },
                { false, true, true },
                { false, true, false },
                { false, false, true },
                { false, false, false },
                { false, null, true },
                { false, null, false }
        };
    }

    // verify Metadata request handling.
    @Test(timeOut = 20000, dataProvider = "allowAutoTopicCreation")
    public void testBrokerHandleTopicMetadataRequestAllowAutoTopicCreation(boolean brokerAllowAutoTopicCreation,
                                                                           Boolean overrideNameSpaceAutoTopicCreation,
                                                                           boolean allowAutoTopicCreationInRequest)
            throws Exception {
        boolean original = conf.isAllowAutoTopicCreation();
        try {
            conf.setAllowAutoTopicCreation(brokerAllowAutoTopicCreation);
            boolean expectedAllowTopicCreation = overrideNameSpaceAutoTopicCreation != null
                    ? overrideNameSpaceAutoTopicCreation : conf.isAllowAutoTopicCreation();
            if (overrideNameSpaceAutoTopicCreation != null) {
                // override per-namespace
                admin.namespaces().setAutoTopicCreation("public/default", AutoTopicCreationOverride
                        .builder()
                        .allowAutoTopicCreation(overrideNameSpaceAutoTopicCreation)
                        .defaultNumPartitions(conf.getDefaultNumPartitions())
                        .topicType("partitioned")
                        .build());
                Policies policies = admin.namespaces().getPolicies("public/default");
                assertEquals(policies.autoTopicCreationOverride.isAllowAutoTopicCreation(),
                        overrideNameSpaceAutoTopicCreation.booleanValue());
            } else {
                admin.namespaces().removeAutoTopicCreation("public/default");
                Policies policies = admin.namespaces().getPolicies("public/default");
                assertNull(policies.autoTopicCreationOverride);
            }

            String topicName = "kopBrokerHandleTopicMetadataRequest-" + brokerAllowAutoTopicCreation + "-"
                    + overrideNameSpaceAutoTopicCreation + "-"
                    + allowAutoTopicCreationInRequest;
            List<String> kafkaTopics = Collections.singletonList(topicName);
            KafkaHeaderAndRequest metadataRequest =
                    createTopicMetadataRequest(kafkaTopics, allowAutoTopicCreationInRequest);
            CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
            handler.handleTopicMetadataRequest(metadataRequest, responseFuture);
            MetadataResponse metadataResponse = (MetadataResponse) responseFuture.get();

            final Errors expectedError;
            if (expectedAllowTopicCreation) {
                if (allowAutoTopicCreationInRequest) {
                    // topic will be created
                    expectedError = null;
                    assertEquals(1, metadataResponse.topicMetadata().size());
                    assertEquals(topicName, metadataResponse.topicMetadata().iterator().next().topic());
                } else {
                    // topic does not exist and it is not created
                    expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION;
                }
            } else {
                if (allowAutoTopicCreationInRequest) {
                    // topic does not exist and it cannot be created
                    expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION;
                } else {
                    // topic does not exist and it is not created
                    expectedError = Errors.UNKNOWN_TOPIC_OR_PARTITION;
                }
            }
            log.info("errors {}", metadataResponse.errors());
            assertEquals(expectedError, metadataResponse.errors().get(topicName));
        } finally {
            conf.setAllowAutoTopicCreation(original);
            admin.namespaces().removeAutoTopicCreation("public/default");
        }
    }

    @Test(timeOut = 30000)
    public void testCommitOffsetRetryWhenProducerClosed()
            throws ExecutionException, InterruptedException, PulsarAdminException {
        String topic = "testCommitOffsetRetryWhenProducerClosed";
        String groupId = "test-commit-offset-group";
        admin.topics().createPartitionedTopic(topic, 1);
        int numMessages = 10;
        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());
        for (int i = 0; i < numMessages; i++) {
            producer.send(new ProducerRecord<>(topic, "value")).get();
        }

        @Cleanup
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(newKafkaConsumerProperties(groupId));
        consumer.subscribe(Collections.singleton(topic));

        int fetchMessages = 0;

        // Make sure only close once.
        final AtomicBoolean flag = new AtomicBoolean(true);

        while (fetchMessages < numMessages) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(record -> {
                    consumer.commitSync();
                    if (flag.get()) {
                        handler.getGroupCoordinator().getOffsetsProducers().values()
                                .forEach(producerCompletableFuture -> {
                            try {
                                producerCompletableFuture.get().close();
                            } catch (PulsarClientException | InterruptedException | ExecutionException e) {
                                log.error("Close offset producer failed.");
                            }
                        });
                        flag.set(false);
                    }
                });
                fetchMessages += records.count();
            } catch (KafkaException ex) {
                log.error("Have kafka exception: ", ex);
                throw ex;
            }
        }
        assertEquals(fetchMessages, numMessages);
    }

    private KafkaHeaderAndRequest createTopicMetadataRequest(List<String> topics, boolean allowAutoTopicCreation) {
        AbstractRequest.Builder builder = new MetadataRequest.Builder(topics, allowAutoTopicCreation);
        return buildRequest(builder);
    }


    private KafkaHeaderAndRequest buildRequest(AbstractRequest.Builder builder) {
        SocketAddress serviceAddress = InetSocketAddress.createUnresolved("localhost", 1111);
        return KafkaCommonTestUtils.buildRequest(builder, serviceAddress);
    }

}
