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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.security.auth.Resource;
import io.streamnative.pulsar.handlers.kop.security.auth.ResourceType;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.*;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for {@link KafkaRequestHandler} with authorization enabled.
 */
@Slf4j
public class KafkaRequestHandlerWithAuthorizationTest extends KopProtocolHandlerTestBase {

    private static final String TENANT = "KafkaAuthorizationTest";
    private static final String NAMESPACE = "ns1";
    private static final String SHORT_TOPIC = "topic1";
    private static final String TOPIC = "persistent://" + TENANT + "/" + NAMESPACE + "/" + SHORT_TOPIC;
    private static final int DEFAULT_PARTITION_NUM = 2;
    private SocketAddress serviceAddress;

    private static final String ADMIN_USER = "admin_user";

    private String adminToken;

    private KafkaRequestHandler handler;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));
        ServiceConfiguration authConf = new ServiceConfiguration();
        authConf.setProperties(properties);
        provider.initialize(authConf);

        adminToken = AuthTokenUtils.createToken(secretKey, ADMIN_USER, Optional.empty());

        super.resetConfig();
        conf.setDefaultNumPartitions(DEFAULT_PARTITION_NUM);
        conf.setSaslAllowedMechanisms(Sets.newHashSet("PLAIN"));
        conf.setKafkaMetadataTenant("internal");
        conf.setKafkaMetadataNamespace("__kafka");
        conf.setKafkaTenant(TENANT);
        conf.setKafkaNamespace(NAMESPACE);
        conf.setKafkaTransactionCoordinatorEnabled(true);

        conf.setClusterName(super.configClusterName);
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationAllowWildcardsMatching(true);
        conf.setSuperUserRoles(Sets.newHashSet(ADMIN_USER));
        conf.setAuthenticationProviders(
                Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters("token:" + adminToken);
        conf.setProperties(properties);

        super.internalSetup();
        log.info("success internal setup");

        if (!admin.namespaces().getNamespaces(TENANT).contains(TENANT + "/__kafka")) {
            admin.namespaces().createNamespace(TENANT + "/__kafka");
            admin.namespaces().setNamespaceReplicationClusters(TENANT + "/__kafka", Sets.newHashSet("test"));
            admin.namespaces().setRetention(TENANT + "/__kafka",
                    new RetentionPolicies(-1, -1));
        }

        admin.topics().createPartitionedTopic(TOPIC, DEFAULT_PARTITION_NUM);

        log.info("created namespaces, init handler");

        DefaultThreadFactory defaultThreadFactory = new DefaultThreadFactory("pulsar-ph-kafka");
        EventLoopGroup dedicatedWorkerGroup =
                EventLoopUtil.newEventLoopGroup(1, false, defaultThreadFactory);
        DefaultEventLoop eventExecutors = new DefaultEventLoop(dedicatedWorkerGroup);

        handler = newRequestHandler();
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockCtx).channel();
        doReturn(eventExecutors).when(mockCtx).executor();
        handler.ctx = mockCtx;

        serviceAddress = new InetSocketAddress(pulsar.getBindAddress(), kafkaBrokerPort);

        // Make sure group coordinator already handle immigration
        handleGroupImmigration();
    }

    @Override
    protected void createAdmin() throws Exception {
        super.admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(this.conf.getBrokerClientAuthenticationPlugin(),
                        this.conf.getBrokerClientAuthenticationParameters()).build());
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "metadataVersions")
    public static Object[][] metadataVersions() {
        return new Object[][]{ { (short) 0 }, { (short) 1 } };
    }

    @Test(timeOut = 10000, dataProvider = "metadataVersions")
    public void testMetadataForPartitionedTopicFailed(short version) throws Exception {
        final String topic = "testMetadataForPartitionedTopic-" + version;
        admin.topics().createNonPartitionedTopic("persistent://" + TENANT + "/" + NAMESPACE + "/" + topic);

        final RequestHeader header = new RequestHeader(ApiKeys.METADATA, version, "client", 0);
        MetadataRequestData data = new MetadataRequestData()
                .setTopics(Collections.singletonList(new MetadataRequestData.MetadataRequestTopic()
                        .setName(topic)))
                .setAllowAutoTopicCreation(false);
        // TO NOT USE the MetadataRequest.Builder, otherwise you cannot use version = 0
        final MetadataRequest request = new MetadataRequest(data, version);
        final CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        handler.handleTopicMetadataRequest(
                new KafkaCommandDecoder.KafkaHeaderAndRequest(
                        header, request, PulsarByteBufAllocator.DEFAULT.heapBuffer(), null),
                responseFuture);
        final MetadataResponse response = (MetadataResponse) responseFuture.get();
        assertEquals(response.topicMetadata().size(), 1);
        assertEquals(response.errors().size(), 1);
        assertEquals(response.errors().get(topic), Errors.TOPIC_AUTHORIZATION_FAILED);
    }

    @Test(timeOut = 10000, dataProvider = "metadataVersions")
    public void testMetadataForPartitionedTopicSuccess(short version) throws Exception {
        final String topic = TOPIC + "-" + version;
        KafkaRequestHandler spyHandler = spy(handler);
        doReturn(CompletableFuture.completedFuture(true))
                .when(spyHandler)
                .authorize(eq(AclOperation.DESCRIBE), eq(Resource.of(ResourceType.TOPIC, topic)));
        final RequestHeader header = new RequestHeader(ApiKeys.METADATA, version, "client", 0);
        MetadataRequestData data = new MetadataRequestData()
                .setTopics(Collections.singletonList(new MetadataRequestData.MetadataRequestTopic()
                        .setName(topic)))
                .setAllowAutoTopicCreation(true);
        // TO NOT USE the MetadataRequest.Builder, otherwise you cannot use version = 0
        final MetadataRequest request = new MetadataRequest(data, version);
        final CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleTopicMetadataRequest(
                new KafkaCommandDecoder.KafkaHeaderAndRequest(
                        header, request, PulsarByteBufAllocator.DEFAULT.heapBuffer(), null),
                responseFuture);
        final MetadataResponse response = (MetadataResponse) responseFuture.get();
        assertEquals(response.topicMetadata().size(), 1);
        assertEquals(response.errors().size(), 0);
    }

    @Test(timeOut = 10000)
    public void testMetadataListTopic() throws Exception {
        final String topic = TOPIC;
        KafkaRequestHandler spyHandler = spy(handler);
        for (int i = 0; i < DEFAULT_PARTITION_NUM; i++) {
            doReturn(CompletableFuture.completedFuture(true))
                    .when(spyHandler)
                    .authorize(eq(AclOperation.DESCRIBE),
                            eq(Resource.of(ResourceType.NAMESPACE, TopicName.get(topic).getNamespace()))
                    );
        }

        final RequestHeader header = new RequestHeader(ApiKeys.METADATA, (short) 1, "client", 0);
        MetadataRequestData data = new MetadataRequestData()
                .setTopics(Collections.emptyList())
                .setAllowAutoTopicCreation(true);
        // TO NOT USE the MetadataRequest.Builder, otherwise you cannot use version = 0
        final MetadataRequest request = new MetadataRequest(data, (short) 0);
        final CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleTopicMetadataRequest(
                new KafkaCommandDecoder.KafkaHeaderAndRequest(
                        header, request, PulsarByteBufAllocator.DEFAULT.heapBuffer(), null),
                responseFuture);
        final MetadataResponse response = (MetadataResponse) responseFuture.get();
        String localName = TopicName.get(topic).getLocalName();

        HashMap<String, MetadataResponse.TopicMetadata> topicMap = new HashMap<>();
        response.topicMetadata().forEach(metadata -> topicMap.put(metadata.topic(), metadata));
        assertTrue(topicMap.containsKey(localName));
        assertEquals(topicMap.get(localName).partitionMetadata().size(), DEFAULT_PARTITION_NUM);
        assertNull(response.errors().get(localName));

        response.errors().forEach((t, errors) -> {
            if (!localName.equals(t)) {
                assertEquals(errors, Errors.TOPIC_AUTHORIZATION_FAILED);
            }
        });
    }

    @Test(timeOut = 20000)
    public void testHandleProduceRequest() throws ExecutionException, InterruptedException {
        KafkaRequestHandler spyHandler = spy(handler);

        final String topic = TOPIC;
        final String topic2 = "topic2";

        // Build input params
        Map<TopicPartition, MemoryRecords> partitionRecords = new HashMap<>();
        TopicPartition topicPartition1 = new TopicPartition(topic, 0);
        TopicPartition topicPartition2 = new TopicPartition(topic2, 0);
        partitionRecords.put(topicPartition1,
                MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("test".getBytes())));
        partitionRecords.put(topicPartition2,
                MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("test2".getBytes())));
        ProduceRequestData requestData = new ProduceRequestData()
                .setAcks((short) 1)
                .setTimeoutMs(5000);
        requestData.topicData().add(new ProduceRequestData.TopicProduceData()
                .setName(topicPartition1.topic())
                .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
                        .setIndex(topicPartition1.partition())
                        .setRecords(partitionRecords.get(topicPartition1))))
                );
        requestData.topicData().add(new ProduceRequestData.TopicProduceData()
                .setName(topicPartition2.topic())
                .setPartitionData(Collections.singletonList(new ProduceRequestData.PartitionProduceData()
                        .setIndex(topicPartition2.partition())
                        .setRecords(partitionRecords.get(topicPartition2))))
        );
        final ProduceRequest request =
                new ProduceRequest.Builder(ApiKeys.PRODUCE.latestVersion(),
                        ApiKeys.PRODUCE.latestVersion(), requestData).build();

        // authorize topic2
        doReturn(CompletableFuture.completedFuture(true))
                .when(spyHandler)
                .authorize(eq(AclOperation.WRITE),
                        eq(Resource.of(ResourceType.TOPIC, KopTopic.toString(topicPartition2,
                                handler.currentNamespacePrefix())))
                );

        // Handle request
        final RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 1, "client", 0);
        final CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleProduceRequest(new KafkaCommandDecoder.KafkaHeaderAndRequest(
                header,
                request,
                PulsarByteBufAllocator.DEFAULT.heapBuffer(),
                null), responseFuture);
        final ProduceResponse response = (ProduceResponse) responseFuture.get();

        //Topic: "topic2" authorize success. Error is not TOPIC_AUTHORIZATION_FAILED
        assertEquals(response.responses().get(topicPartition2).error, Errors.NOT_LEADER_OR_FOLLOWER);
        //Topic: `TOPIC` authorize failed.
        assertEquals(response.responses().get(topicPartition1).error, Errors.TOPIC_AUTHORIZATION_FAILED);
    }

    @Test(timeOut = 20000)
    public void testHandleListOffsetRequestAuthorizationSuccess() throws Exception {
        KafkaRequestHandler spyHandler = spy(handler);
        String topicName = "persistent://" + TENANT + "/" + NAMESPACE + "/"
                + "testHandleListOffsetRequestAuthorizationSuccess";

        // Mock all authorize call
        doReturn(CompletableFuture.completedFuture(true))
                .when(spyHandler)
                .authorize(eq(AclOperation.DESCRIBE),
                        eq(Resource.of(ResourceType.TOPIC, TopicName.get(topicName).getPartition(0).toString()))
                );

        // Create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 1);
        TopicPartition tp = new TopicPartition(topicName, 0);

        @Cleanup
        KProducer kProducer = new KProducer(topicName,
                false,
                "localhost",
                getKafkaBrokerPort(),
                TENANT + "/" + NAMESPACE,
                "token:" + adminToken
        );
        int totalMsgs = 10;
        String messageStrPrefix = topicName + "_message_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer()
                    .send(new ProducerRecord<>(topicName, i, messageStr))
                    .get();
            log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
        }

        // Test for ListOffset request verify Earliest get earliest
        ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
                .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                .setTargetTimes(KafkaCommonTestUtils
                        .newListOffsetTargetTimes(tp, ListOffsetsRequest.EARLIEST_TIMESTAMP));

        KafkaCommandDecoder.KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleListOffsetRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();
        ListOffsetsResponse listOffsetResponse = (ListOffsetsResponse) response;
        ListOffsetsResponseData.ListOffsetsPartitionResponse listOffsetsPartitionResponse =
                getListOffsetsPartitionResponse(tp, listOffsetResponse.data());
        assertEquals(listOffsetsPartitionResponse.errorCode(), Errors.NONE.code());
        assertEquals(listOffsetsPartitionResponse.offset(), 0L);
        assertEquals(listOffsetsPartitionResponse.timestamp(), 0L);
    }

    @Test(timeOut = 20000)
    public void testHandleListOffsetRequestAuthorizationFailed() throws Exception {
        KafkaRequestHandler spyHandler = spy(handler);
        String topicName = "persistent://" + TENANT + "/" + NAMESPACE + "/"
                + "testHandleListOffsetRequestAuthorizationFailed";

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 1);
        TopicPartition tp = new TopicPartition(topicName, 0);

        ListOffsetsRequest.Builder builder = ListOffsetsRequest.Builder
                .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                .setTargetTimes(KafkaCommonTestUtils
                        .newListOffsetTargetTimes(tp, ListOffsetsRequest.EARLIEST_TIMESTAMP));

        KafkaCommandDecoder.KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleListOffsetRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();
        ListOffsetsResponse listOffsetResponse = (ListOffsetsResponse) response;
        ListOffsetsResponseData.ListOffsetsPartitionResponse listOffsetsPartitionResponse =
                getListOffsetsPartitionResponse(tp, listOffsetResponse.data());
        assertEquals(listOffsetsPartitionResponse.errorCode(), Errors.TOPIC_AUTHORIZATION_FAILED.code());
    }


    @Test(timeOut = 20000)
    public void testHandleOffsetFetchRequestAuthorizationSuccess()
            throws PulsarAdminException, ExecutionException, InterruptedException {
        KafkaRequestHandler spyHandler = spy(handler);
        String topicName = "persistent://" + TENANT + "/" + NAMESPACE + "/"
                + "testHandleOffsetFetchRequestAuthorizationSuccess";
        String groupId = "DemoKafkaOnPulsarConsumer";

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 1);
        TopicPartition tp = new TopicPartition(new KopTopic(topicName,
                handler.currentNamespacePrefix()).getFullName(), 0);
        doReturn(CompletableFuture.completedFuture(true))
                .when(spyHandler)
                .authorize(eq(AclOperation.DESCRIBE),
                        eq(Resource.of(ResourceType.TOPIC, new KopTopic(tp.topic(),
                                handler.currentNamespacePrefix()).getFullName()))
                );
        OffsetFetchRequest.Builder builder =
                new OffsetFetchRequest.Builder(groupId, false,
                        Collections.singletonList(tp), false);

        KafkaCommandDecoder.KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();

        spyHandler.handleOffsetFetchRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();

        assertTrue(response instanceof OffsetFetchResponse);
        OffsetFetchResponse offsetFetchResponse = (OffsetFetchResponse) response;
        assertEquals(offsetFetchResponse.responseData().size(), 1);
        assertEquals(offsetFetchResponse.error(), Errors.NONE);
        offsetFetchResponse.responseData()
                .forEach((topicPartition, partitionData) -> assertEquals(partitionData.error, Errors.NONE));
    }

    @Test(timeOut = 20000)
    public void testHandleOffsetFetchRequestAuthorizationFailed()
            throws PulsarAdminException, ExecutionException, InterruptedException {
        KafkaRequestHandler spyHandler = spy(handler);
        String topicName = "persistent://" + TENANT + "/" + NAMESPACE + "/"
                + "testHandleOffsetFetchRequestAuthorizationFailed";
        String groupId = "DemoKafkaOnPulsarConsumer";

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 1);
        TopicPartition tp = new TopicPartition(new KopTopic(topicName,
                handler.currentNamespacePrefix()).getFullName(), 0);
        OffsetFetchRequest.Builder builder =
                new OffsetFetchRequest.Builder(groupId, false, Collections.singletonList(tp), false);

        KafkaCommandDecoder.KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();

        spyHandler.handleOffsetFetchRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();

        assertTrue(response instanceof OffsetFetchResponse);
        OffsetFetchResponse offsetFetchResponse = (OffsetFetchResponse) response;
        assertEquals(offsetFetchResponse.responseData().size(), 1);
        assertEquals(offsetFetchResponse.error(), Errors.NONE);
        offsetFetchResponse.responseData().forEach((topicPartition, partitionData) -> assertEquals(partitionData.error,
                Errors.TOPIC_AUTHORIZATION_FAILED));
    }

    @Test(timeOut = 20000)
    public void testOffsetCommitRequestAuthorizationFailed() throws Exception {
        String group = "test-failed-groupId";
        String memberId = "test_failed_member_id";
        TopicPartition topicPartition = new TopicPartition("test", 1);

        // Build input params
        OffsetCommitRequestData offsetData = new OffsetCommitRequestData()
                .setGroupId(group).setMemberId(memberId);
        offsetData.topics().add(KafkaCommonTestUtils
                .newOffsetCommitRequestPartitionData(topicPartition, 1L, ""));
        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(offsetData);
        KafkaCommandDecoder.KafkaHeaderAndRequest headerAndRequest = buildRequest(builder);

        // Handle request
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        handler.handleOffsetCommitRequest(headerAndRequest, responseFuture);
        AbstractResponse response = responseFuture.get();
        assertTrue(response instanceof OffsetCommitResponse);
        OffsetCommitResponse offsetCommitResponse = (OffsetCommitResponse) response;
        assertEquals(offsetCommitResponse.data().topics().size(), 1);
        assertEquals(offsetCommitResponse.data().topics().get(0).partitions().size(), 1);
        assertFalse(offsetCommitResponse.errorCounts().isEmpty());
        offsetCommitResponse.data().topics().forEach(topic -> {
            topic.partitions()
                .forEach((partitionResult) -> assertEquals(partitionResult.errorCode(),
                        Errors.TOPIC_AUTHORIZATION_FAILED.code()));
        });
    }

    @Test(timeOut = 20000)
    public void testOffsetCommitRequestPartAuthorizationFailed() throws Exception {
        String group = "test-failed-groupId";
        String memberId = "test_failed_member_id";
        TopicPartition topicPartition1 = new TopicPartition("test", 1);
        TopicPartition topicPartition2 = new TopicPartition("test1", 2);
        TopicPartition topicPartition3 = new TopicPartition("test2", 3);

        // Build input params
        OffsetCommitRequestData offsetData = new OffsetCommitRequestData()
                .setGroupId(group).setMemberId(memberId);
        offsetData.topics().add(KafkaCommonTestUtils
                .newOffsetCommitRequestPartitionData(topicPartition1, 1L, ""));
        offsetData.topics().add(KafkaCommonTestUtils
                .newOffsetCommitRequestPartitionData(topicPartition2, 2L, ""));
        offsetData.topics().add(KafkaCommonTestUtils
                .newOffsetCommitRequestPartitionData(topicPartition3, 3L, ""));
        offsetData.setGroupId(group);

        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(offsetData);
        KafkaCommandDecoder.KafkaHeaderAndRequest headerAndRequest = buildRequest(builder);

        // Topic: `test` authorize success.
        KafkaRequestHandler spyHandler = spy(handler);
        doReturn(CompletableFuture.completedFuture(true))
                .when(spyHandler)
                .authorize(eq(AclOperation.READ),
                        eq(Resource.of(ResourceType.TOPIC, new KopTopic(topicPartition1.topic(),
                                handler.currentNamespacePrefix()).getFullName()))
                );

        // Handle request
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleOffsetCommitRequest(headerAndRequest, responseFuture);

        AbstractResponse response = responseFuture.get();
        assertTrue(response instanceof OffsetCommitResponse);
        OffsetCommitResponse offsetCommitResponse = (OffsetCommitResponse) response;
        assertEquals(offsetCommitResponse.data().topics().size(), 3);
        assertEquals(offsetCommitResponse.errorCounts().size(), 2);
        assertEquals(offsetCommitResponse.data().topics().stream()
                .filter(t->t.name().equals(topicPartition2.topic())).findFirst().get()
                .partitions().stream()
                .filter(p->p.partitionIndex() == topicPartition2.partition())
                .findFirst().get().errorCode(), Errors.TOPIC_AUTHORIZATION_FAILED.code());
        assertEquals(offsetCommitResponse.data().topics().stream()
                .filter(t->t.name().equals(topicPartition3.topic())).findFirst().get()
                .partitions().stream()
                .filter(p->p.partitionIndex() == topicPartition3.partition())
                .findFirst().get().errorCode(), Errors.TOPIC_AUTHORIZATION_FAILED.code());

    }

    @Test(timeOut = 20000)
    public void testHandleTxnOffsetCommitAuthorizationFailed() throws ExecutionException, InterruptedException {
        String group = "test-failed-groupId";
        TopicPartition topicPartition = new TopicPartition("test", 1);
        Map<TopicPartition, TxnOffsetCommitRequest.CommittedOffset> offsetData = Maps.newHashMap();
        offsetData.put(topicPartition, KafkaCommonTestUtils.newTxnOffsetCommitRequestCommittedOffset(1L, ""));
        TxnOffsetCommitRequest.Builder builder =
                new TxnOffsetCommitRequest.Builder(
                        "1", group, 1, (short) 1, offsetData, false);
        KafkaCommandDecoder.KafkaHeaderAndRequest headerAndRequest = buildRequest(builder);

        // Handle request
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        handler.handleTxnOffsetCommit(headerAndRequest, responseFuture);
        AbstractResponse response = responseFuture.get();
        assertTrue(response instanceof TxnOffsetCommitResponse);
        TxnOffsetCommitResponse txnOffsetCommitResponse = (TxnOffsetCommitResponse) response;

        assertEquals(txnOffsetCommitResponse.errorCounts().size(), 1);
        txnOffsetCommitResponse.errors().values()
                .forEach(errors -> assertEquals(errors, Errors.TOPIC_AUTHORIZATION_FAILED));
    }

    @Test(timeOut = 20000)
    public void testHandleTxnOffsetCommitPartAuthorizationFailed() throws ExecutionException, InterruptedException {
        String group = "test-failed-groupId";
        TopicPartition topicPartition1 = new TopicPartition("test1", 1);
        TopicPartition topicPartition2 = new TopicPartition("test2", 1);
        TopicPartition topicPartition3 = new TopicPartition("test3", 1);

        Map<TopicPartition, TxnOffsetCommitRequest.CommittedOffset> offsetData = Maps.newHashMap();
        offsetData.put(topicPartition1,
                KafkaCommonTestUtils.newTxnOffsetCommitRequestCommittedOffset(1L, ""));
        offsetData.put(topicPartition2,
                KafkaCommonTestUtils.newTxnOffsetCommitRequestCommittedOffset(1L, ""));
        offsetData.put(topicPartition3,
                KafkaCommonTestUtils.newTxnOffsetCommitRequestCommittedOffset(1L, ""));

        TxnOffsetCommitRequest.Builder builder =
                new TxnOffsetCommitRequest.Builder(
                        "1", group, 1, (short) 1, offsetData, false);
        KafkaCommandDecoder.KafkaHeaderAndRequest headerAndRequest = buildRequest(builder);

        // Topic: `test1` authorize success.
        KafkaRequestHandler spyHandler = spy(handler);
        doReturn(CompletableFuture.completedFuture(true))
                .when(spyHandler)
                .authorize(eq(AclOperation.READ),
                        eq(Resource.of(ResourceType.TOPIC, new KopTopic(topicPartition1.topic(),
                                handler.currentNamespacePrefix()).getFullName()))
                );

        // Handle request
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleTxnOffsetCommit(headerAndRequest, responseFuture);
        AbstractResponse response = responseFuture.get();
        assertTrue(response instanceof TxnOffsetCommitResponse);
        TxnOffsetCommitResponse txnOffsetCommitResponse = (TxnOffsetCommitResponse) response;

        assertEquals(txnOffsetCommitResponse.errorCounts().size(), 2);
        assertEquals(txnOffsetCommitResponse.errors().get(topicPartition1), Errors.NONE);
        assertEquals(txnOffsetCommitResponse.errors().get(topicPartition2), Errors.TOPIC_AUTHORIZATION_FAILED);
        assertEquals(txnOffsetCommitResponse.errors().get(topicPartition3), Errors.TOPIC_AUTHORIZATION_FAILED);
    }

    @Test(timeOut = 20000)
    public void testAddPartitionsToTxnAuthorizationFailed() throws ExecutionException, InterruptedException {
        TopicPartition topicPartition1 = new TopicPartition("test", 1);
        TopicPartition topicPartition2 = new TopicPartition("test1", 2);
        TopicPartition topicPartition3 = new TopicPartition("test2", 3);
        List<TopicPartition> topicPartitions = Arrays.asList(topicPartition1, topicPartition2, topicPartition3);

        AddPartitionsToTxnRequest.Builder builder =
                new AddPartitionsToTxnRequest.Builder(
                        "1", 1, (short) 1, topicPartitions);
        KafkaCommandDecoder.KafkaHeaderAndRequest headerAndRequest = buildRequest(builder);

        // Topic: `test` authorize success.
        KafkaRequestHandler spyHandler = spy(handler);
        doReturn(CompletableFuture.completedFuture(true))
                .when(spyHandler)
                .authorize(eq(AclOperation.WRITE),
                        eq(Resource.of(ResourceType.TOPIC, KopTopic.toString(topicPartition1,
                                handler.currentNamespacePrefix())))
                );
        // Handle request
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleAddPartitionsToTxn(headerAndRequest, responseFuture);
        AbstractResponse response = responseFuture.get();
        assertTrue(response instanceof AddPartitionsToTxnResponse);
        AddPartitionsToTxnResponse addPartitionsToTxnResponse = (AddPartitionsToTxnResponse) response;

        assertEquals(addPartitionsToTxnResponse.errorCounts().size(), 2);

        // OPERATION_NOT_ATTEMPTED Or TOPIC_AUTHORIZATION_FAILED
        assertEquals(addPartitionsToTxnResponse.errors().size(), 3);

        assertEquals(addPartitionsToTxnResponse.errors().get(topicPartition1), Errors.OPERATION_NOT_ATTEMPTED);
        assertEquals(addPartitionsToTxnResponse.errors().get(topicPartition2), Errors.TOPIC_AUTHORIZATION_FAILED);
        assertEquals(addPartitionsToTxnResponse.errors().get(topicPartition3), Errors.TOPIC_AUTHORIZATION_FAILED);
    }

    @Test(timeOut = 20000)
    public void testAddPartitionsToTxnPartAuthorizationFailed() throws ExecutionException, InterruptedException {
        TopicPartition topicPartition = new TopicPartition("test", 1);
        AddPartitionsToTxnRequest.Builder builder =
                new AddPartitionsToTxnRequest.Builder(
                        "1", 1, (short) 1, Collections.singletonList(topicPartition));
        KafkaCommandDecoder.KafkaHeaderAndRequest headerAndRequest = buildRequest(builder);
        // Handle request
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        handler.handleAddPartitionsToTxn(headerAndRequest, responseFuture);
        AbstractResponse response = responseFuture.get();
        assertTrue(response instanceof AddPartitionsToTxnResponse);
        AddPartitionsToTxnResponse addPartitionsToTxnResponse = (AddPartitionsToTxnResponse) response;

        assertEquals(addPartitionsToTxnResponse.errorCounts().size(), 1);
        addPartitionsToTxnResponse.errors().values()
                .forEach(errors -> assertEquals(errors, Errors.TOPIC_AUTHORIZATION_FAILED));
    }

    @Test(timeOut = 20000)
    public void testCreatePartitionsAuthorizationFailed() throws Exception {
        final String topic = "test-create-partitions-failed";
        final String fullTopic = "persistent://" + TENANT + "/" + NAMESPACE + "/" + topic;
        final int oldPartitions = 5;

        admin.topics().createPartitionedTopic(fullTopic, oldPartitions);

        CreatePartitionsRequest.Builder builder = new CreatePartitionsRequest.Builder(
                KafkaCommonTestUtils.newPartitionsMap(fullTopic, 10)
                        .setTimeoutMs(5000)
                        .setValidateOnly(false));

        KafkaCommandDecoder.KafkaHeaderAndRequest headerAndRequest = buildRequest(builder);

        // Handle request
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        handler.handleCreatePartitions(headerAndRequest, responseFuture);

        AbstractResponse response = responseFuture.get();
        assertTrue(response instanceof CreatePartitionsResponse);
        CreatePartitionsResponse createPartitionsResponse = (CreatePartitionsResponse) response;
        assertEquals(createPartitionsResponse.errorCounts().size(), 1);
        Map<String, CreatePartitionsResponseData.CreatePartitionsTopicResult> errors =
                createPartitionsResponse.data().results().stream()
                .collect(Collectors.toMap(r -> r.name(), r -> r));
        assertTrue(errors.containsKey(fullTopic));
        assertEquals(errors.get(fullTopic).errorCode(), Errors.TOPIC_AUTHORIZATION_FAILED.code());

    }

    @Test(timeOut = 20000)
    public void testCreatePartitionsPartAuthorizationFailed() throws Exception {
        final String topic1 = "test-create-partitions-failed-1";
        final String topic2 = "test-create-partitions-failed-2";
        final String topic3 = "test-create-partitions-failed-3";
        final String fullTopic1 = "persistent://" + TENANT + "/" + NAMESPACE + "/" + topic1;
        final String fullTopic2 = "persistent://" + TENANT + "/" + NAMESPACE + "/" + topic2;
        final String fullTopic3 = "persistent://" + TENANT + "/" + NAMESPACE + "/" + topic3;
        final int oldPartitions = 5;

        admin.topics().createPartitionedTopic(fullTopic1, oldPartitions);
        admin.topics().createPartitionedTopic(fullTopic2, oldPartitions);
        admin.topics().createPartitionedTopic(fullTopic3, oldPartitions);

        CreatePartitionsRequest.Builder builder = new CreatePartitionsRequest.Builder(
                KafkaCommonTestUtils.newPartitionsMap(Arrays.asList(fullTopic1, fullTopic2, fullTopic3), 10)
                        .setTimeoutMs(5000)
                        .setValidateOnly(false));

        KafkaCommandDecoder.KafkaHeaderAndRequest headerAndRequest = buildRequest(builder);

        KafkaRequestHandler spyHandler = spy(handler);
        doReturn(CompletableFuture.completedFuture(true))
                .when(spyHandler)
                .authorize(eq(AclOperation.ALTER),
                        eq(Resource.of(ResourceType.TOPIC, fullTopic1))
                );

        // Handle request
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleCreatePartitions(headerAndRequest, responseFuture);

        AbstractResponse response = responseFuture.get();
        assertTrue(response instanceof CreatePartitionsResponse);
        CreatePartitionsResponse createPartitionsResponse = (CreatePartitionsResponse) response;
        assertEquals(createPartitionsResponse.errorCounts().size(), 2);
        Map<String, CreatePartitionsResponseData.CreatePartitionsTopicResult> errors =
                createPartitionsResponse.data().results().stream()
                .collect(Collectors.toMap(r -> r.name(), r -> r));
        assertEquals(errors.size(), 3);
        assertEquals(errors.get(fullTopic1).errorCode(), Errors.NONE.code());
        assertEquals(errors.get(fullTopic2).errorCode(), Errors.TOPIC_AUTHORIZATION_FAILED.code());
        assertEquals(errors.get(fullTopic3).errorCode(), Errors.TOPIC_AUTHORIZATION_FAILED.code());

    }

    @Test(timeOut = 20000)
    public void testCreatePartitionsAuthorizationSuccess() throws Exception {
        KafkaRequestHandler spyHandler = spy(handler);
        final String topic = "test-create-partitions-success";
        final String fullTopic = "persistent://" + TENANT + "/" + NAMESPACE + "/" + topic;
        final int oldPartitions = 5;

        admin.topics().createPartitionedTopic(fullTopic, oldPartitions);

        doReturn(CompletableFuture.completedFuture(true))
                .when(spyHandler)
                .authorize(eq(AclOperation.ALTER),
                        eq(Resource.of(ResourceType.TOPIC, fullTopic))
                );

        CreatePartitionsRequest.Builder builder = new CreatePartitionsRequest.Builder(
                KafkaCommonTestUtils.newPartitionsMap(fullTopic, 10)
                        .setTimeoutMs(5000)
                        .setValidateOnly(false));

        KafkaCommandDecoder.KafkaHeaderAndRequest headerAndRequest = buildRequest(builder);

        // Handle request
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleCreatePartitions(headerAndRequest, responseFuture);

        AbstractResponse response = responseFuture.get();
        assertTrue(response instanceof CreatePartitionsResponse);
        CreatePartitionsResponse createPartitionsResponse = (CreatePartitionsResponse) response;
        assertEquals(createPartitionsResponse.data().results().size(), 1);
        assertTrue(createPartitionsResponse.data().results().stream()
                .anyMatch(r->r.name().equals(fullTopic)
                        && r.errorCode() == Errors.NONE.code()));

    }
    private KafkaCommandDecoder.KafkaHeaderAndRequest buildRequest(AbstractRequest.Builder builder) {
        return KafkaCommonTestUtils.buildRequest(builder, serviceAddress);
    }

    private void handleGroupImmigration() {
        GroupCoordinator groupCoordinator = handler.getGroupCoordinator();
        for (int i = 0; i < conf.getOffsetsTopicNumPartitions(); i++) {
            try {
                groupCoordinator.handleGroupImmigration(i).get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("Failed to handle group immigration.", e);
            }
        }
    }

}
