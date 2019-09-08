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

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.kop.KafkaCommandDecoder.ResponseAndRequest;
import io.streamnative.kop.utils.MessageIdUtils;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Validate KafkaApisTest.
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
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockCtx).channel();
        kafkaRequestHandler.ctx = mockCtx;

        serviceAddress = new InetSocketAddress(kafkaService.getBindAddress(), kafkaBrokerPort);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
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

    CompletableFuture<ResponseAndRequest> checkInvalidPartition(String topic,
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
        assertEquals(((OffsetCommitResponse) response1.getResponse()).responseData().get(topicPartition1),
            Errors.UNKNOWN_TOPIC_OR_PARTITION);

        // invalid partition id 1.
        CompletableFuture<ResponseAndRequest> invalidResponse2 = checkInvalidPartition(topicName, 1);
        TopicPartition topicPartition2 = new TopicPartition(topicName, 1);
        ResponseAndRequest response2 = invalidResponse2.get();
        assertEquals(response2.getRequest().getHeader().apiKey(), ApiKeys.OFFSET_COMMIT);
        assertEquals(((OffsetCommitResponse) response2.getResponse()).responseData().get(topicPartition2),
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

        // use producer to create some message to get Limit Offset.
        String pulsarTopicName = "persistent://public/default/" + topicName;

        // create partitioned topic.
        kafkaService.getAdminClient().topics().createPartitionedTopic(topicName, 1);

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

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(pulsarTopicName)
            .subscriptionName(topicName + "_sub")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();
        Message<byte[]> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
        assertNotNull(msg);
        MessageIdImpl messageId = (MessageIdImpl) ((TopicMessageIdImpl) msg.getMessageId()).getInnerMessageId();
        // first entry should be the limit offset.
        long limitOffset = MessageIdUtils.getOffset(messageId.getLedgerId(), 0);
        log.info("After create {} messages, get messageId: {} expected earliest limit: {}",
            totalMsgs, messageId, limitOffset);

        // 2. real test, for ListOffset request verify Earliest get earliest
        Map<TopicPartition, Long> targetTimes = Maps.newHashMap();
        targetTimes.put(tp, ListOffsetRequest.EARLIEST_TIMESTAMP);

        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
            .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
            .setTargetTimes(targetTimes);

        KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<ResponseAndRequest> responseFuture = kafkaRequestHandler
            .handleListOffsetRequest(request);

        ResponseAndRequest response = responseFuture.get();
        ListOffsetResponse listOffsetResponse = (ListOffsetResponse) response.getResponse();
        assertEquals(response.getRequest().getHeader().apiKey(), ApiKeys.LIST_OFFSETS);
        assertEquals(listOffsetResponse.responseData().get(tp).error, Errors.NONE);
        assertEquals(listOffsetResponse.responseData().get(tp).offset, Long.valueOf(limitOffset));
        assertEquals(listOffsetResponse.responseData().get(tp).timestamp, Long.valueOf(NO_TIMESTAMP));
    }

    // these 2 test cases test Read Commit / UnCommit. they are the same for Pulsar,
    // so combine it in one test case.
    // Test ListOffset for latest get the earliest message in topic.
    // testReadUncommittedConsumerListOffsetLatest
    // testReadCommittedConsumerListOffsetLatest
    @Test(timeOut = 20000)
    public void testConsumerListOffsetLatest() throws Exception {
        String topicName = "testConsumerListOffsetLatest";
        TopicPartition tp = new TopicPartition(topicName, 0);

        // use producer to create some message to get Limit Offset.
        String pulsarTopicName = "persistent://public/default/" + topicName;

        // create partitioned topic.
        kafkaService.getAdminClient().topics().createPartitionedTopic(topicName, 1);

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

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(pulsarTopicName)
            .subscriptionName(topicName + "_sub")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();
        Message<byte[]> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
        assertNotNull(msg);
        MessageIdImpl messageId = (MessageIdImpl) ((TopicMessageIdImpl) msg.getMessageId()).getInnerMessageId();
        // LAC entry should be the limit offset.
        long limitOffset = MessageIdUtils.getOffset(messageId.getLedgerId(), totalMsgs - 1);
        log.info("After create {} messages, get messageId: {} expected latest limit: {}",
            totalMsgs, messageId, limitOffset);

        // 2. real test, for ListOffset request verify Earliest get earliest
        Map<TopicPartition, Long> targetTimes = Maps.newHashMap();
        targetTimes.put(tp, ListOffsetRequest.LATEST_TIMESTAMP);

        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
            .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
            .setTargetTimes(targetTimes);

        KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<ResponseAndRequest> responseFuture = kafkaRequestHandler
            .handleListOffsetRequest(request);

        ResponseAndRequest response = responseFuture.get();
        ListOffsetResponse listOffsetResponse = (ListOffsetResponse) response.getResponse();
        assertEquals(response.getRequest().getHeader().apiKey(), ApiKeys.LIST_OFFSETS);
        assertEquals(listOffsetResponse.responseData().get(tp).error, Errors.NONE);
        assertEquals(listOffsetResponse.responseData().get(tp).offset, Long.valueOf(limitOffset));
        assertEquals(listOffsetResponse.responseData().get(tp).timestamp, Long.valueOf(NO_TIMESTAMP));
    }
}
