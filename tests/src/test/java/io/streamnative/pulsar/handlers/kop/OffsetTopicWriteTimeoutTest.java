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

import static io.streamnative.pulsar.handlers.kop.KafkaCommonTestUtils.buildRequest;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class OffsetTopicWriteTimeoutTest extends KopProtocolHandlerTestBase {

    private KafkaRequestHandler handler;
    private InetSocketAddress serviceAddress;


    @BeforeClass(timeOut = 30000)
    @Override
    protected void setup() throws Exception {
        // Any request that writes to the offset topic will time out with such a low timeout
        conf.setOffsetCommitTimeoutMs(1);
        super.internalSetup();
        handler = newRequestHandler();
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockCtx).channel();
        handler.channelActive(mockCtx);
        serviceAddress = new InetSocketAddress(pulsar.getBindAddress(), kafkaBrokerPort);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(newKafkaConsumerProperties());
        final var rebalanced = new AtomicBoolean(false);
        consumer.subscribe(Collections.singleton("my-topic"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                // No ops
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                rebalanced.set(true);
            }
        });
        for (int i = 0; !rebalanced.get() && i < 100; i++) {
            consumer.poll(Duration.ofMillis(50));
        }
        Assert.assertTrue(rebalanced.get());
        consumer.close();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 30000)
    public void testSyncGroup() throws Exception {
        final var protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol().setName("range").setMetadata("".getBytes()));
        final var joinGroupRequest = buildRequest(new JoinGroupRequest.Builder(
                new JoinGroupRequestData().setGroupId(DEFAULT_GROUP_ID).setMemberId("")
                        .setSessionTimeoutMs(conf.getGroupMinSessionTimeoutMs())
                        .setProtocolType("consumer").setProtocols(protocols)
        ), serviceAddress);
        final var joinGroupFuture = new CompletableFuture<AbstractResponse>();
        handler.handleJoinGroupRequest(joinGroupRequest, joinGroupFuture);
        final var joinGroupResponse = (JoinGroupResponse) joinGroupFuture.get();
        Assert.assertEquals(joinGroupResponse.error(), Errors.NONE);

        final var syncGroupRequest = buildRequest(new SyncGroupRequest.Builder(
                new SyncGroupRequestData().setGroupId(DEFAULT_GROUP_ID)
                        .setMemberId(joinGroupResponse.data().memberId())
                        .setGenerationId(joinGroupResponse.data().generationId())), serviceAddress);
        var syncGroupFuture = new CompletableFuture<AbstractResponse>();

        handler.handleSyncGroupRequest(syncGroupRequest, syncGroupFuture);
        final var syncGroupResponse = (SyncGroupResponse) syncGroupFuture.get();
        Assert.assertEquals(syncGroupResponse.errorCounts().keySet(),
                Collections.singleton(Errors.REBALANCE_IN_PROGRESS));
    }

    @Test(timeOut = 30000)
    public void testOffsetCommit() throws Exception {
        final var offsetCommit = new OffsetCommitRequest.Builder(new OffsetCommitRequestData()
                .setGroupId(DEFAULT_GROUP_ID)
                .setTopics(Collections.singletonList(KafkaCommonTestUtils.newOffsetCommitRequestPartitionData(
                        new TopicPartition("my-topic", 0),
                        0,
                        ""
                ))));
        final var request = buildRequest(offsetCommit, serviceAddress);
        final var future = new CompletableFuture<AbstractResponse>();
        handler.handleOffsetCommitRequest(request, future);
        final var response = (OffsetCommitResponse) future.get();
        Assert.assertEquals(response.errorCounts().keySet(), Collections.singleton(Errors.REQUEST_TIMED_OUT));
    }
}
