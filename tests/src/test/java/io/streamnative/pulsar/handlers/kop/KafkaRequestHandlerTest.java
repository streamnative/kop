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


import static io.streamnative.pulsar.handlers.kop.utils.TopicNameUtils.getKafkaTopicNameFromPulsarTopicname;
import static io.streamnative.pulsar.handlers.kop.utils.TopicNameUtils.getPartitionedTopicNameWithoutPartitions;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndResponse;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataManager;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import io.streamnative.pulsar.handlers.kop.stats.NullStatsLogger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
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
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
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
    private AdminManager adminManager;

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

        ProtocolHandler handler1 = pulsar.getProtocolHandlers().protocol("kafka");
        GroupCoordinator groupCoordinator = ((KafkaProtocolHandler) handler1).getGroupCoordinator();
        TransactionCoordinator transactionCoordinator = ((KafkaProtocolHandler) handler1).getTransactionCoordinator();

        adminManager = new AdminManager(pulsar.getAdminClient(), conf);
        handler = new KafkaRequestHandler(
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
        handler.ctx = mockCtx;
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        adminManager.shutdown();
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
        ByteBuffer serializedRequest = apiVersionsRequest.serialize(header);
        int size = serializedRequest.remaining();
        ByteBuf inputBuf = Unpooled.buffer(size);
        inputBuf.writeBytes(serializedRequest);

        // 2. turn Bytebuf into KafkaHeaderAndRequest.
        KafkaHeaderAndRequest request = handler.byteBufToRequest(inputBuf);

        // 3. verify byteBufToRequest works well.
        assertEquals(request.getHeader().toStruct(), header.toStruct());
        assertTrue(request.getRequest() instanceof ApiVersionsRequest);
    }


    @Test
    public void testResponseToByteBuf() throws Exception {
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

        ApiVersionsResponse apiVersionsResponse = ApiVersionsResponse.defaultApiVersionsResponse();
        KafkaHeaderAndResponse kopResponse = KafkaHeaderAndResponse.responseForRequest(
            kopRequest, apiVersionsResponse);

        // 1. serialize response into ByteBuf
        ByteBuf serializedResponse = handler.responseToByteBuf(kopResponse.getResponse(), kopRequest);

        // 2. verify responseToByteBuf works well.
        ByteBuffer byteBuffer = serializedResponse.nioBuffer();
        ResponseHeader responseHeader = ResponseHeader.parse(byteBuffer);
        assertEquals(responseHeader.correlationId(), correlationId);

        ApiVersionsResponse parsedResponse = ApiVersionsResponse.parse(
            byteBuffer, kopResponse.getApiVersion());

        assertEquals(parsedResponse.apiVersions().size(), apiVersionsResponse.apiVersions().size());
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

        PartitionMetadata metadata = KafkaRequestHandler.newPartitionMetadata(topicName, node);
        assertEquals(metadata.error(), Errors.NONE);
        assertEquals(metadata.partition(), 0);


        metadata = KafkaRequestHandler.newPartitionMetadata(topicNamePartition, node);
        assertEquals(metadata.error(), Errors.NONE);
        assertEquals(metadata.partition(), partitionIndex);

        metadata = KafkaRequestHandler.newFailedPartitionMetadata(topicName);
        assertEquals(metadata.error(), Errors.NOT_LEADER_FOR_PARTITION);
        assertEquals(metadata.partition(), 0);


        metadata = KafkaRequestHandler.newFailedPartitionMetadata(topicNamePartition);
        assertEquals(metadata.error(), Errors.NOT_LEADER_FOR_PARTITION);
        assertEquals(metadata.partition(), partitionIndex);
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

    @Test
    public void testGetKafkaTopicNameFromPulsarTopicName() {
        String localName = "localTopicName2";
        String topicString = "persistent://test-tenants/test-ns/" + localName;
        int partitionIndex = 77;

        TopicName topicName = TopicName.get(topicString);
        TopicName topicNamePartition =
            TopicName.get(topicString + PARTITIONED_TOPIC_SUFFIX + partitionIndex);

        assertEquals(localName, getKafkaTopicNameFromPulsarTopicname(topicName));
        assertEquals(localName, getKafkaTopicNameFromPulsarTopicname(topicNamePartition));
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
        Map<String, Integer> topicToNumPartitions = new HashMap<String, Integer>(){{
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

        final Map<String, Integer> expectedTopicPartitions = new HashMap<String, Integer>() {{
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
    public void testDescribeConfigs() throws Exception {
        final String topic = "testDescribeConfigs";
        admin.topics().createPartitionedTopic(topic, 1);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(props);
        final Map<String, String> entries = KafkaLogConfig.getEntries();

        kafkaAdmin.describeConfigs(Collections.singletonList(new ConfigResource(ConfigResource.Type.TOPIC, topic)))
                .all().get().forEach((resource, config) -> {
            assertEquals(resource.name(), topic);
            config.entries().forEach(entry -> assertEquals(entry.value(), entries.get(entry.name())));
        });

        final String invalidTopic = "invalid-topic";
        try {
            kafkaAdmin.describeConfigs(Collections.singletonList(
                    new ConfigResource(ConfigResource.Type.TOPIC, invalidTopic))).all().get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof UnknownTopicOrPartitionException);
            assertTrue(e.getMessage().contains("Topic " + invalidTopic + " doesn't exist"));
        }

        admin.topics().createNonPartitionedTopic(invalidTopic);
        try {
            kafkaAdmin.describeConfigs(Collections.singletonList(
                    new ConfigResource(ConfigResource.Type.TOPIC, invalidTopic))).all().get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof InvalidTopicException);
            assertTrue(e.getMessage().contains("Topic " + invalidTopic + " is non-partitioned"));
        }
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
    public void testConvertOffsetCommitRetentionMsIfSetDefaultValue() throws Exception {

        String memberId = "test_member_id";
        int generationId = 0;
        long currentTime = 100;
        int configRetentionMs = 1000;
        TopicPartition topicPartition = new TopicPartition("test", 1);

        // build input params
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<>();
        offsetData.put(topicPartition,
                new OffsetCommitRequest.PartitionData(1L, ""));
        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder("test-groupId", offsetData)
                .setGenerationId(generationId)
                .setMemberId(memberId)
                .setRetentionTime(OffsetCommitRequest.DEFAULT_RETENTION_TIME);
        OffsetCommitRequest offsetCommitRequest = builder.build();


        // convert
        Map<TopicPartition, OffsetAndMetadata> converted =
                handler.convertOffsetCommitRequestRetentionMs(offsetCommitRequest,
                builder.latestAllowedVersion(),
                currentTime,
                configRetentionMs);

        OffsetAndMetadata convertedOffsetAndMetadata = converted.get(topicPartition);

        // verify
        Assert.assertEquals(convertedOffsetAndMetadata.commitTimestamp(), currentTime);
        Assert.assertEquals(convertedOffsetAndMetadata.expireTimestamp(), currentTime + configRetentionMs);

    }

    @Test(timeOut = 10000)
    public void testConvertOffsetCommitRetentionMsIfRetentionMsSet() throws Exception {

        String memberId = "test_member_id";
        int generationId = 0;
        long currentTime = 100;
        int offsetsConfigRetentionMs = 1000;
        int requestSetRetentionMs = 10000;
        TopicPartition topicPartition = new TopicPartition("test", 1);

        // build input params
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<>();
        offsetData.put(topicPartition,
                new OffsetCommitRequest.PartitionData(1L, ""));
        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder("test-groupId", offsetData)
                .setGenerationId(generationId)
                .setMemberId(memberId)
                .setRetentionTime(requestSetRetentionMs);
        OffsetCommitRequest offsetCommitRequest = builder.build();


        // convert
        Map<TopicPartition, OffsetAndMetadata> converted =
                handler.convertOffsetCommitRequestRetentionMs(offsetCommitRequest,
                builder.latestAllowedVersion(),
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
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<>();
        offsetData.put(topicPartition,
                new OffsetCommitRequest.PartitionData(1L, ""));
        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder("test-groupId", offsetData)
                .setGenerationId(generationId)
                .setMemberId(memberId)
                .setRetentionTime(OffsetCommitRequest.DEFAULT_RETENTION_TIME);
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
        OffsetAndMetadata offsetAndMetadata = metadata.offset(topicPartition).get();

        // offset in cache
        Assert.assertNotNull(offsetAndMetadata);

        // trigger clean expire offset logic
        Map<TopicPartition, OffsetAndMetadata> removeExpiredOffsets =
                metadata.removeExpiredOffsets(Time.SYSTEM.milliseconds());

        // there is only one offset just saved. it should not being removed.
        Assert.assertTrue(removeExpiredOffsets.isEmpty(),
                "expect no expired offset. but " + removeExpiredOffsets + " expired.");

        metadata = groupMetadataManager.getGroup(group).get();
        offsetAndMetadata = metadata.offset(topicPartition).get();

        // not cleanup
        Assert.assertNotNull(offsetAndMetadata);

    }

    @Test(timeOut = 10000)
    public void testListOffsetsForNotExistedTopic() throws Exception {
        final TopicPartition topicPartition = new TopicPartition("testListOffsetsForNotExistedTopic", 0);
        final CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        final RequestHeader header =
                new RequestHeader(ApiKeys.LIST_OFFSETS, ApiKeys.LIST_OFFSETS.latestVersion(), "client", 0);
        final ListOffsetRequest request =
                ListOffsetRequest.Builder.forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                        .setTargetTimes(Collections.singletonMap(topicPartition, ListOffsetRequest.EARLIEST_TIMESTAMP))
                        .build(ApiKeys.LIST_OFFSETS.latestVersion());
        handler.handleListOffsetRequest(
                new KafkaHeaderAndRequest(header, request, PulsarByteBufAllocator.DEFAULT.heapBuffer(), null),
                responseFuture);
        final ListOffsetResponse response = (ListOffsetResponse) responseFuture.get();
        assertTrue(response.responseData().containsKey(topicPartition));
        assertEquals(response.responseData().get(topicPartition).error, Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    @Test(timeOut = 10000, dataProvider = "metadataVersions")
    public void testMetadataForNonPartitionedTopic(short version) throws Exception {
        final String topic = "testMetadataForNonPartitionedTopic-" + version;
        admin.topics().createNonPartitionedTopic(topic);

        final RequestHeader header = new RequestHeader(ApiKeys.METADATA, version, "client", 0);
        final MetadataRequest request = new MetadataRequest(Collections.singletonList(topic), false, version);
        final CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        handler.handleTopicMetadataRequest(
                new KafkaHeaderAndRequest(header, request, PulsarByteBufAllocator.DEFAULT.heapBuffer(), null),
                responseFuture);
        final MetadataResponse response = (MetadataResponse) responseFuture.get();
        assertEquals(response.topicMetadata().size(), 1);
        assertEquals(response.errors().size(), 0);
    }
}
