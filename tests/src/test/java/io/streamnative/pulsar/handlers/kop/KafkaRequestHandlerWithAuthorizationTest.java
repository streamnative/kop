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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.security.auth.Resource;
import io.streamnative.pulsar.handlers.kop.security.auth.ResourceType;
import io.streamnative.pulsar.handlers.kop.stats.NullStatsLogger;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddPartitionsToTxnRequest;
import org.apache.kafka.common.requests.AddPartitionsToTxnResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
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
    private AdminManager adminManager;

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

        ProtocolHandler handler1 = pulsar.getProtocolHandlers().protocol("kafka");
        GroupCoordinator groupCoordinator = ((KafkaProtocolHandler) handler1)
                .getGroupCoordinator(conf.getKafkaMetadataTenant());
        TransactionCoordinator transactionCoordinator = ((KafkaProtocolHandler) handler1)
                .getTransactionCoordinator(conf.getKafkaMetadataTenant());

        adminManager = new AdminManager(pulsar.getAdminClient(), conf);
        handler = new KafkaRequestHandler(
                pulsar,
                (KafkaServiceConfiguration) conf,
                new TenantContextManager() {
                    @Override
                    public GroupCoordinator getGroupCoordinator(String tenant) {
                        return groupCoordinator;
                    }

                    @Override
                    public TransactionCoordinator getTransactionCoordinator(String tenant) {
                        return transactionCoordinator;
                    }
                },
                adminManager,
                pulsar.getLocalMetadataStore().getMetadataCache(LocalBrokerData.class),
                false,
                getPlainEndPoint(),
                NullStatsLogger.INSTANCE);
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockCtx).channel();
        handler.ctx = mockCtx;

        serviceAddress = new InetSocketAddress(pulsar.getBindAddress(), kafkaBrokerPort);
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
        adminManager.shutdown();
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
        final MetadataRequest request =
                new MetadataRequest(Collections.singletonList(topic), false, version);
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
        final MetadataRequest request =
                new MetadataRequest(Collections.singletonList(topic), true, version);
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
                            eq(Resource.of(ResourceType.TOPIC, TopicName.get(topic).getPartition(i).toString()))
                    );
        }

        final RequestHeader header = new RequestHeader(ApiKeys.METADATA, (short) 1, "client", 0);
        final MetadataRequest request =
                new MetadataRequest(Collections.emptyList(), true, (short) 1);
        final CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleTopicMetadataRequest(
                new KafkaCommandDecoder.KafkaHeaderAndRequest(
                        header, request, PulsarByteBufAllocator.DEFAULT.heapBuffer(), null),
                responseFuture);
        final MetadataResponse response = (MetadataResponse) responseFuture.get();
        String localName = TopicName.get(topic).getLocalName();

        HashMap<String, MetadataResponse.TopicMetadata> topicMap = new HashMap<>();
        response.topicMetadata().forEach(metadata -> {
            topicMap.put(metadata.topic(), metadata);
        });
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
        final RequestHeader header = new RequestHeader(ApiKeys.PRODUCE, (short) 1, "client", 0);
        final ProduceRequest request = createProduceRequest(TOPIC);
        final CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();

        spyHandler.handleProduceRequest(new KafkaCommandDecoder.KafkaHeaderAndRequest(
                header,
                request,
                PulsarByteBufAllocator.DEFAULT.heapBuffer(),
                null), responseFuture);
        AbstractResponse response = responseFuture.get();
        assertEquals((int) response.errorCounts().get(Errors.TOPIC_AUTHORIZATION_FAILED), 1);
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
        Map<TopicPartition, Long> targetTimes = Maps.newHashMap();
        targetTimes.put(tp, ListOffsetRequest.EARLIEST_TIMESTAMP);

        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
                .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                .setTargetTimes(targetTimes);

        KafkaCommandDecoder.KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleListOffsetRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();
        ListOffsetResponse listOffsetResponse = (ListOffsetResponse) response;
        assertEquals(listOffsetResponse.responseData().get(tp).error, Errors.NONE);
        assertEquals(listOffsetResponse.responseData().get(tp).offset.intValue(), 0);
        assertEquals(listOffsetResponse.responseData().get(tp).timestamp, Long.valueOf(0));
    }

    @Test(timeOut = 20000)
    public void testHandleListOffsetRequestAuthorizationFailed() throws Exception {
        KafkaRequestHandler spyHandler = spy(handler);
        String topicName = "persistent://" + TENANT + "/" + NAMESPACE + "/"
                + "testHandleListOffsetRequestAuthorizationFailed";

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 1);
        TopicPartition tp = new TopicPartition(topicName, 0);

        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
                .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                .setTargetTimes(new HashMap<TopicPartition, Long>(){{
                    put(tp, ListOffsetRequest.EARLIEST_TIMESTAMP);
                }});

        KafkaCommandDecoder.KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleListOffsetRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();
        ListOffsetResponse listOffsetResponse = (ListOffsetResponse) response;
        assertEquals(listOffsetResponse.responseData().get(tp).error, Errors.TOPIC_AUTHORIZATION_FAILED);
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
        TopicPartition tp = new TopicPartition(new KopTopic(topicName).getFullName(), 0);
        doReturn(CompletableFuture.completedFuture(true))
                .when(spyHandler)
                .authorize(eq(AclOperation.DESCRIBE),
                        eq(Resource.of(ResourceType.TOPIC, new KopTopic(tp.topic()).getFullName()))
                );
        OffsetFetchRequest.Builder builder =
                new OffsetFetchRequest.Builder(groupId, Collections.singletonList(tp));

        KafkaCommandDecoder.KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();

        spyHandler.handleOffsetFetchRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();

        assertTrue(response instanceof OffsetFetchResponse);
        OffsetFetchResponse offsetFetchResponse = (OffsetFetchResponse) response;
        assertEquals(offsetFetchResponse.responseData().size(), 1);
        assertEquals(offsetFetchResponse.error(), Errors.NONE);
        offsetFetchResponse.responseData().forEach((topicPartition, partitionData) -> {
            assertEquals(partitionData.error, Errors.NONE);
        });
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
        TopicPartition tp = new TopicPartition(new KopTopic(topicName).getFullName(), 0);
        OffsetFetchRequest.Builder builder =
                new OffsetFetchRequest.Builder(groupId, Collections.singletonList(tp));

        KafkaCommandDecoder.KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();

        spyHandler.handleOffsetFetchRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();

        assertTrue(response instanceof OffsetFetchResponse);
        OffsetFetchResponse offsetFetchResponse = (OffsetFetchResponse) response;
        assertEquals(offsetFetchResponse.responseData().size(), 1);
        assertEquals(offsetFetchResponse.error(), Errors.NONE);
        offsetFetchResponse.responseData().forEach((topicPartition, partitionData) -> {
            assertEquals(partitionData.error, Errors.TOPIC_AUTHORIZATION_FAILED);
        });
    }

    @Test(timeOut = 20000)
    public void testOffsetCommitRequestAuthorizationFailed() throws Exception {
        String group = "test-failed-groupId";
        String memberId = "test_failed_member_id";
        TopicPartition topicPartition = new TopicPartition("test", 1);

        // Build input params
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = Maps.newHashMap();
        offsetData.put(topicPartition,
                new OffsetCommitRequest.PartitionData(1L, ""));
        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(group, offsetData)
                .setMemberId(memberId)
                .setRetentionTime(OffsetCommitRequest.DEFAULT_RETENTION_TIME);
        KafkaCommandDecoder.KafkaHeaderAndRequest headerAndRequest = buildRequest(builder);

        // Handle request
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        handler.handleOffsetCommitRequest(headerAndRequest, responseFuture);
        AbstractResponse response = responseFuture.get();
        assertTrue(response instanceof OffsetCommitResponse);
        OffsetCommitResponse offsetCommitResponse = (OffsetCommitResponse) response;
        assertEquals(offsetCommitResponse.responseData().size(), 1);
        assertFalse(offsetCommitResponse.errorCounts().isEmpty());
        offsetCommitResponse.responseData().forEach((__, error) -> {
            assertEquals(error, Errors.TOPIC_AUTHORIZATION_FAILED);
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
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = Maps.newHashMap();
        offsetData.put(topicPartition1,
                new OffsetCommitRequest.PartitionData(1L, ""));
        offsetData.put(topicPartition2,
                new OffsetCommitRequest.PartitionData(2L, ""));
        offsetData.put(topicPartition3,
                new OffsetCommitRequest.PartitionData(3L, ""));

        OffsetCommitRequest.Builder builder = new OffsetCommitRequest.Builder(group, offsetData)
                .setMemberId(memberId)
                .setRetentionTime(OffsetCommitRequest.DEFAULT_RETENTION_TIME);
        KafkaCommandDecoder.KafkaHeaderAndRequest headerAndRequest = buildRequest(builder);

        // Topic: `test` authorize success.
        KafkaRequestHandler spyHandler = spy(handler);
        doReturn(CompletableFuture.completedFuture(true))
                .when(spyHandler)
                .authorize(eq(AclOperation.READ),
                        eq(Resource.of(ResourceType.TOPIC, new KopTopic(topicPartition1.topic()).getFullName()))
                );

        // Handle request
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleOffsetCommitRequest(headerAndRequest, responseFuture);

        AbstractResponse response = responseFuture.get();
        assertTrue(response instanceof OffsetCommitResponse);
        OffsetCommitResponse offsetCommitResponse = (OffsetCommitResponse) response;
        assertEquals(offsetCommitResponse.responseData().size(), 3);
        assertEquals(offsetCommitResponse.errorCounts().size(), 2);
        assertEquals(offsetCommitResponse.responseData().get(topicPartition2), Errors.TOPIC_AUTHORIZATION_FAILED);
        assertEquals(offsetCommitResponse.responseData().get(topicPartition3), Errors.TOPIC_AUTHORIZATION_FAILED);

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
                        eq(Resource.of(ResourceType.TOPIC, KopTopic.toString(topicPartition1)))
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
        addPartitionsToTxnResponse.errors().values().forEach(errors -> {
            assertEquals(errors, Errors.TOPIC_AUTHORIZATION_FAILED);
        });
    }

    @Test(timeOut = 20000)
    public void testCreatePartitionsAuthorizationFailed() throws Exception {
        final String topic = "test-create-partitions-failed";
        final String fullTopic = "persistent://" + TENANT + "/" + NAMESPACE + "/" + topic;
        final int oldPartitions = 5;

        admin.topics().createPartitionedTopic(fullTopic, oldPartitions);

        HashMap<String, NewPartitions> newPartitionsMap = Maps.newHashMap();
        final int numPartitions = 10;
        NewPartitions newPartitions = NewPartitions.increaseTo(numPartitions);
        newPartitionsMap.put(fullTopic, newPartitions);

        CreatePartitionsRequest.Builder builder = new CreatePartitionsRequest.Builder(
                newPartitionsMap, 5000, false);

        KafkaCommandDecoder.KafkaHeaderAndRequest headerAndRequest = buildRequest(builder);

        // Handle request
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        handler.handleCreatePartitions(headerAndRequest, responseFuture);

        AbstractResponse response = responseFuture.get();
        assertTrue(response instanceof CreatePartitionsResponse);
        CreatePartitionsResponse createPartitionsResponse = (CreatePartitionsResponse) response;
        assertEquals(createPartitionsResponse.errorCounts().size(), 1);
        assertTrue(createPartitionsResponse.errors().containsKey(fullTopic));
        assertEquals(createPartitionsResponse.errors().get(fullTopic).error(), Errors.TOPIC_AUTHORIZATION_FAILED);

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

        HashMap<String, NewPartitions> newPartitionsMap = Maps.newHashMap();
        final int numPartitions = 10;
        NewPartitions newPartitions = NewPartitions.increaseTo(numPartitions);
        newPartitionsMap.put(fullTopic1, newPartitions);
        newPartitionsMap.put(fullTopic2, newPartitions);
        newPartitionsMap.put(fullTopic3, newPartitions);

        CreatePartitionsRequest.Builder builder = new CreatePartitionsRequest.Builder(
                newPartitionsMap, 5000, false);

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
        assertEquals(createPartitionsResponse.errors().size(), 3);
        assertEquals(createPartitionsResponse.errors().get(fullTopic1).error(), Errors.NONE);
        assertEquals(createPartitionsResponse.errors().get(fullTopic2).error(), Errors.TOPIC_AUTHORIZATION_FAILED);
        assertEquals(createPartitionsResponse.errors().get(fullTopic3).error(), Errors.TOPIC_AUTHORIZATION_FAILED);

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

        HashMap<String, NewPartitions> newPartitionsMap = Maps.newHashMap();
        final int numPartitions = 10;
        NewPartitions newPartitions = NewPartitions.increaseTo(numPartitions);
        newPartitionsMap.put(fullTopic, newPartitions);

        CreatePartitionsRequest.Builder builder = new CreatePartitionsRequest.Builder(
                newPartitionsMap, 5000, false);

        KafkaCommandDecoder.KafkaHeaderAndRequest headerAndRequest = buildRequest(builder);

        // Handle request
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleCreatePartitions(headerAndRequest, responseFuture);

        AbstractResponse response = responseFuture.get();
        assertTrue(response instanceof CreatePartitionsResponse);
        CreatePartitionsResponse createPartitionsResponse = (CreatePartitionsResponse) response;
        assertEquals(createPartitionsResponse.errors().size(), 1);
        assertTrue(createPartitionsResponse.errors().containsKey(fullTopic));
        assertEquals(createPartitionsResponse.errors().get(fullTopic).error(), Errors.NONE);

    }

    KafkaCommandDecoder.KafkaHeaderAndRequest buildRequest(AbstractRequest.Builder builder) {
        AbstractRequest request = builder.build();
        builder.apiKey();

        ByteBuffer serializedRequest = request
                .serialize(new RequestHeader(
                        builder.apiKey(),
                        request.version(),
                        "fake_client_id",
                        0)
                );

        ByteBuf byteBuf = Unpooled.copiedBuffer(serializedRequest);

        RequestHeader header = RequestHeader.parse(serializedRequest);

        ApiKeys apiKey = header.apiKey();
        short apiVersion = header.apiVersion();
        Struct struct = apiKey.parseRequest(apiVersion, serializedRequest);
        AbstractRequest body = AbstractRequest.parseRequest(apiKey, apiVersion, struct);
        return new KafkaCommandDecoder.KafkaHeaderAndRequest(header, body, byteBuf, serviceAddress);
    }

    private ProduceRequest createProduceRequest(String topic) {
        Map<TopicPartition, MemoryRecords> partitionRecords = new HashMap<>();
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        partitionRecords.put(topicPartition,
                MemoryRecords.withRecords(CompressionType.NONE, new SimpleRecord("test".getBytes())));
        return ProduceRequest.Builder.forCurrentMagic((short) 1, 5000, partitionRecords).build();
    }

}
