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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.streamnative.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.kop.KafkaCommandDecoder.ResponseAndRequest;
import io.streamnative.kop.utils.MessageIdUtils;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest.PartitionData;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Validate KafkaApisTest
 */
@Slf4j
public class KafkaApisTest extends MockKafkaServiceBaseTest {

    KafkaRequestHandler kafkaRequestHandler;
    SocketAddress serviceAddress;

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

        kafkaRequestHandler = new KafkaRequestHandler(kafkaService);
        serviceAddress = new InetSocketAddress(kafkaService.getBindAddress(), kafkaBrokerPort);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private KafkaHeaderAndRequest buildRequest(AbstractRequest.Builder builder) {
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

    private CompletableFuture<ResponseAndRequest> checkInvalidPartition(String topic,
                                                                        int invalidPartitionId) {
        TopicPartition invalidTopicPartition = new TopicPartition(topic, invalidPartitionId);
        PartitionData partitionOffsetCommitData = new OffsetCommitRequest.PartitionData(15L, "");
        Map<TopicPartition, PartitionData> offsetData = Maps.newHashMap();
        offsetData.put(invalidTopicPartition, partitionOffsetCommitData);
        KafkaHeaderAndRequest request = buildRequest(new OffsetCommitRequest.Builder("groupId", offsetData));
        return kafkaRequestHandler.handleOffsetCommitRequest(request);
    }

    @Test(timeOut = 20000)
    public void testOffsetCommitWithInvalidPartition() throws Exception {
        String topicName = "kopOffsetCommitWithInvalidPartition";

        // invalid partition id -1;
        CompletableFuture<ResponseAndRequest> invalidResponse1 = checkInvalidPartition(topicName, -1);
        ResponseAndRequest response1 = invalidResponse1.get();
        assertEquals(response1.getRequest().getHeader().apiKey(), ApiKeys.OFFSET_COMMIT);
        TopicPartition topicPartition1 = new TopicPartition(topicName, -1);
        assertEquals(((OffsetCommitResponse)response1.getResponse()).responseData().get(topicPartition1),
            Errors.UNKNOWN_TOPIC_OR_PARTITION);

        // invalid partition id 1.
        CompletableFuture<ResponseAndRequest> invalidResponse2 = checkInvalidPartition(topicName, 1);
        TopicPartition topicPartition2 = new TopicPartition(topicName, 1);
        ResponseAndRequest response2 = invalidResponse2.get();
        assertEquals(response2.getRequest().getHeader().apiKey(), ApiKeys.OFFSET_COMMIT);
        assertEquals(((OffsetCommitResponse)response2.getResponse()).responseData().get(topicPartition2),
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

    @Test(timeOut = 20000)
    public void testReadUncommittedConsumerListOffsetEarliestOffsetEqualsHighWatermark() throws Exception {
        String topicName = "testReadUncommittedConsumerListOffsetEarliestOffsetEqualsHighWatermark";
        long limitOffset = 15L;
        long nowMs = System.currentTimeMillis();

        TopicPartition tp = new TopicPartition(topicName, 0);
        Map<TopicPartition, Long> targetTimes = Maps.newHashMap();
        targetTimes.put(tp, ListOffsetRequest.EARLIEST_TIMESTAMP);
        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
            .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
            .setTargetTimes(targetTimes);

        KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<ResponseAndRequest> responseFuture = kafkaRequestHandler
            .handleOffsetCommitRequest(request);

        ResponseAndRequest response = responseFuture.get();
        ListOffsetResponse listOffsetResponse = (ListOffsetResponse)response.getResponse();
        assertEquals(response.getRequest().getHeader().apiKey(), ApiKeys.OFFSET_COMMIT);


    }
}
