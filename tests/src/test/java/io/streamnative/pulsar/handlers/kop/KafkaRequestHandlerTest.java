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
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndResponse;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
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
}
