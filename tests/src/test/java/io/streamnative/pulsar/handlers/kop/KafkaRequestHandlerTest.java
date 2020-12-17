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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndResponse;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test for {@link KafkaRequestHandler}.
 */
@Slf4j
public class KafkaRequestHandlerTest extends KopProtocolHandlerTestBase {

    private KafkaRequestHandler handler;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName,
                new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        } else {
            admin.clusters().updateCluster(configClusterName,
                new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        }

        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        } else {
            admin.tenants().updateTenant("public",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        }
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

        admin.tenants().createTenant("my-tenant",
                new TenantInfo(Sets.newHashSet(), Sets.newHashSet(super.configClusterName)));
        admin.namespaces().createNamespace("my-tenant/my-ns");

        log.info("created namespaces, init handler");

        ProtocolHandler handler1 = pulsar.getProtocolHandlers().protocol("kafka");
        GroupCoordinator groupCoordinator = ((KafkaProtocolHandler) handler1).getGroupCoordinator();

        handler = new KafkaRequestHandler(
            pulsar,
            (KafkaServiceConfiguration) conf,
            groupCoordinator,
            false);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testAutoReadEnableDisable() {
        final ByteBuf buffer = Unpooled.copiedBuffer("Test", CharsetUtil.US_ASCII);
        final EmbeddedChannel channel = new EmbeddedChannel(handler);
        ByteBuf[] buffers = new ByteBuf[1000000];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = buffer.duplicate().retain();
        }
        AtomicBoolean isChannelWritable = new AtomicBoolean(true);
        ExecutorService service = Executors.newSingleThreadExecutor();

        service.execute(() -> {
            int count = 0;
            while (count < 10) {
                boolean isWritable = handler.getCtx().channel().isWritable();
                if (!isWritable) {
                    isChannelWritable.set(false);
                    break;
                }
                count += 1;
                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (Exception ignored) {}

            }
        });
        channel.writeOutbound(buffers);
        Assert.assertFalse(isChannelWritable.get());
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

    void createTopicsByKafkaAdmin(AdminClient admin, Map<String, Integer> topicToNumPartitions)
            throws ExecutionException, InterruptedException {
        final short replicationFactor = 1; // replication factor will be ignored
        admin.createTopics(topicToNumPartitions.entrySet().stream().map(entry -> {
            final String topic = entry.getKey();
            final int numPartitions = entry.getValue();
            return new NewTopic(topic, numPartitions, replicationFactor);
        }).collect(Collectors.toList())).all().get();
    }

    void verifyTopicsByPulsarAdmin(Map<String, Integer> topicToNumPartitions)
            throws PulsarAdminException {
        for (Map.Entry<String, Integer> entry : topicToNumPartitions.entrySet()) {
            final String topic = entry.getKey();
            final int numPartitions = entry.getValue();
            assertEquals(this.admin.topics().getPartitionedTopicMetadata(topic).partitions, numPartitions);
        }
    }

    @Test(timeOut = 10000)
    public void testCreateTopics() throws Exception {
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
        createTopicsByKafkaAdmin(kafkaAdmin, topicToNumPartitions);
        verifyTopicsByPulsarAdmin(topicToNumPartitions);
    }

    @Test(timeOut = 10000)
    public void testCreateInvalidTopics() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(props);
        Map<String, Integer> topicToNumPartitions = new HashMap<String, Integer>(){{
            put("xxx/testCreateInvalidTopics-0", 1);
        }};
        try {
            createTopicsByKafkaAdmin(kafkaAdmin, topicToNumPartitions);
            fail("create a invalid topic should fail");
        } catch (Exception e) {
            log.info("Failed to create topics: {}", topicToNumPartitions);
            assertTrue(e.getCause() instanceof TimeoutException);
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
    }

    @Test(timeOut = 10000)
    public void testProduceCallback() throws Exception {
        final String topic = "test-produce-callback";
        final int numMessages = 10;
        final String messagePrefix = "msg-";

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        @Cleanup
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        Map<Integer, Long> indexToOffset = new ConcurrentHashMap<>();
        for (int i = 0; i < numMessages; i++) {
            final int index = i;
            producer.send(new ProducerRecord<>(topic, i, messagePrefix + i), (recordMetadata, e) -> {
                if (e != null) {
                    log.error("Failed to send {}: {}", index, e);
                    fail("Failed to send " + index + ": " + e.getMessage());
                }
                assertEquals(recordMetadata.topic(), topic);
                assertEquals(recordMetadata.partition(), 0);
                indexToOffset.put(index, recordMetadata.offset());
                MessageIdImpl id = (MessageIdImpl) MessageIdUtils.getMessageId(indexToOffset.get(index));
                log.info("Success write {} to {} ({}, {})", index, recordMetadata.offset(),
                        id.getLedgerId(), id.getEntryId());
            }).get();
            // TODO: here we disable batching for Kafka producer, because when batching is enabled, Pulsar consumers
            //   may receive wrong messages order from Kafka producer. This issue may be similar to
            //   https://github.com/streamnative/kop/issues/243
        }

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("subscription-name")
                .subscribe();
        for (int i = 0; i < numMessages; i++) {
            Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
            assertNotNull(message);
            consumer.acknowledge(message);
            assertTrue(indexToOffset.containsKey(i));

            MessageIdImpl id = (MessageIdImpl) MessageIdUtils.getMessageId(indexToOffset.get(i));
            byte[] positionInSendResponse = id.toByteArray();
            byte[] positionReceived = message.getMessageId().toByteArray();
            log.info("Successfully send {} to ({}, {}) {}, received: {}", i, id.getLedgerId(), id.getEntryId(),
                    positionInSendResponse, positionReceived);
            // The result of MessageIdUtils#getMessageId only contains ledger id and entry id, so we need to cut the
            // extra bytes of positionInSendResponse.
            assertEquals(positionInSendResponse, Arrays.copyOf(positionReceived, positionInSendResponse.length));
        }
    }
}
