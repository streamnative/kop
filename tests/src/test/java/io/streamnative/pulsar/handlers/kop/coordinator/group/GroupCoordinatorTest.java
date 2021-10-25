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
package io.streamnative.pulsar.handlers.kop.coordinator.group;

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata.GroupOverview;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata.GroupSummary;
import io.streamnative.pulsar.handlers.kop.coordinator.group.MemberMetadata.MemberSummary;
import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationPurgatory;
import io.streamnative.pulsar.handlers.kop.utils.timer.MockTimer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.schema.KeyValue;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link GroupCoordinator}.
 */
public class GroupCoordinatorTest extends KopProtocolHandlerTestBase {

    private static final int ConsumerMinSessionTimeout = 10;
    private static final int ConsumerMaxSessionTimeout = 10000;
    private static final int DefaultRebalanceTimeout = 500;
    private static final int DefaultSessionTimeout = 500;
    private static final int GroupInitialRebalanceDelay = 50;
    private static final String groupId = "groupId";
    private static final String protocolType = "consumer";
    private static final String memberId = "memberId";
    private static final byte[] metadata = new byte[0];

    String topicName;
    MockTimer timer = null;
    GroupCoordinator groupCoordinator = null;

    ProducerBuilder<ByteBuffer> producerBuilder;
    ReaderBuilder<ByteBuffer> readerBuilder;

    Consumer<ByteBuffer> consumer;
    OrderedScheduler scheduler;
    GroupMetadataManager groupMetadataManager;
    private int groupPartitionId = -1;
    private String otherGroupId;
    private int otherGroupPartitionId;
    private Map<String, byte[]> protocols;

    @Override
    protected void resetConfig() {
        super.resetConfig();
    }

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        protocols = newProtocols();

        scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test-scheduler")
            .numThreads(1)
            .build();

        GroupConfig groupConfig = new GroupConfig(
            ConsumerMinSessionTimeout,
            ConsumerMaxSessionTimeout,
            GroupInitialRebalanceDelay
        );

        topicName = "test-coordinator-" + System.currentTimeMillis();
        OffsetConfig offsetConfig = OffsetConfig.builder().offsetsTopicName(topicName).build();

        timer = new MockTimer();

        producerBuilder = pulsarClient.newProducer(Schema.BYTEBUFFER);

        consumer = pulsarClient.newConsumer(Schema.BYTEBUFFER)
            .topic(topicName)
            .subscriptionName("test-sub")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();

        readerBuilder = pulsarClient.newReader(Schema.BYTEBUFFER)
            .startMessageId(MessageId.earliest);

        groupPartitionId = 0;
        otherGroupPartitionId = 1;
        otherGroupId = "otherGroupId";
        offsetConfig.offsetsTopicNumPartitions(4);
        groupMetadataManager = spy(new GroupMetadataManager(
            conf.getKafkaMetadataTenant(),
            offsetConfig,
            producerBuilder,
            readerBuilder,
            scheduler,
            timer.time(),
            id -> {
                if (groupId.equals(id) || id.isEmpty()) {
                    return groupPartitionId;
                } else {
                    return otherGroupPartitionId;
                }
            }
        ));

        assertNotEquals(groupPartitionId, otherGroupPartitionId);

        DelayedOperationPurgatory<DelayedHeartbeat> heartbeatPurgatory =
            DelayedOperationPurgatory.<DelayedHeartbeat>builder()
                .purgatoryName("Heartbeat")
                .timeoutTimer(timer)
                .reaperEnabled(false)
                .build();
        DelayedOperationPurgatory<DelayedJoin> joinPurgatory =
            DelayedOperationPurgatory.<DelayedJoin>builder()
                .purgatoryName("Rebalance")
                .timeoutTimer(timer)
                .reaperEnabled(false)
                .build();

        groupCoordinator = new GroupCoordinator(
            tenant,
            groupConfig,
            groupMetadataManager,
            heartbeatPurgatory,
            joinPurgatory,
            timer.time()
        );

        // start the group coordinator
        groupCoordinator.startup(false);

        // add the partition into the owned partition list
        groupPartitionId = groupMetadataManager.partitionFor(groupId);
        groupMetadataManager.addPartitionOwnership(groupPartitionId);
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        groupCoordinator.shutdown();
        groupMetadataManager.shutdown();
        consumer.close();
        scheduler.shutdown();
        super.internalCleanup();
    }

    private Map<String, byte[]> newProtocols() {
        Map<String, byte[]> protocols = new HashMap<>();
        protocols.put("range", metadata);
        return protocols;
    }

    @Test
    public void testRequestHandlingWhileLoadingInProgress() throws Exception {
        groupMetadataManager.addLoadingPartition(groupPartitionId);
        assertTrue(groupMetadataManager.isGroupLocal(groupId));

        // JoinGroup
        JoinGroupResult joinGroupResponse = groupCoordinator.handleJoinGroup(
            groupId, memberId, "clientId", "clientHost", 60000, 10000, "consumer",
            protocols
        ).get();
        assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, joinGroupResponse.getError());

        // SyncGroup
        CompletableFuture<Errors> syncGroupResponse = new CompletableFuture<>();
        groupCoordinator.handleSyncGroup(
            groupId, 1, memberId, protocols,
            (ignored, error) -> {
                syncGroupResponse.complete(error);
            });
        assertEquals(Errors.REBALANCE_IN_PROGRESS, syncGroupResponse.get());

        // OffsetCommit
        TopicPartition topicPartition = new TopicPartition("foo", 0);
        Map<TopicPartition, Errors> offsetCommitErrors = groupCoordinator.handleCommitOffsets(
            groupId, memberId, 1,
            ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(topicPartition, OffsetAndMetadata.apply(15L))
                .build()
        ).get();
        assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, offsetCommitErrors.get(topicPartition));

        // Heartbeat
        Errors heartbeatError = groupCoordinator.handleHeartbeat(
            groupId, memberId, 1
        ).get();
        assertEquals(Errors.NONE, heartbeatError);

        // DescribeGroups
        KeyValue<Errors, GroupSummary> describeGroupResult = groupCoordinator.handleDescribeGroup(
            groupId
        );
        assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, describeGroupResult.getKey());

        // ListGroups
        KeyValue<Errors, List<GroupOverview>> listGroupsResult = groupCoordinator.handleListGroups();
        assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, listGroupsResult.getKey());

        // DeleteGroups
        Map<String, Errors> deleteGroupsErrors = groupCoordinator.handleDeleteGroups(
            Sets.newHashSet(groupId));
        assertEquals(Errors.COORDINATOR_LOAD_IN_PROGRESS, deleteGroupsErrors.get(groupId));

        // After loading, we should be able to access the group
        groupMetadataManager.removeLoadingPartition(groupPartitionId);
        groupMetadataManager.scheduleLoadGroupAndOffsets(groupPartitionId, group -> {}).get();
        assertEquals(Errors.NONE, groupCoordinator.handleDescribeGroup(groupId).getKey());
    }

    @Test
    public void testJoinGroupUnknowMemberId() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            otherGroupId, memberId, protocolType, protocols
        );
        assertEquals(Errors.NOT_COORDINATOR, joinGroupResult.getError());
    }

    @Test
    public void testJoinGroupSessionTimeoutTooSmall() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols,
            ConsumerMinSessionTimeout - 1
        );
        assertEquals(Errors.INVALID_SESSION_TIMEOUT, joinGroupResult.getError());
    }

    @Test
    public void testJoinGroupSessionTimeoutTooLarge() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols,
            ConsumerMaxSessionTimeout + 1
        );
        assertEquals(Errors.INVALID_SESSION_TIMEOUT, joinGroupResult.getError());
    }

    @Test
    public void testInvalidGroupId() throws Exception {
        String groupId = "";
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        assertEquals(Errors.INVALID_GROUP_ID, joinGroupResult.getError());
    }

    @Test
    public void testValidJoinGroup() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        assertEquals(Errors.NONE, joinGroupResult.getError());
    }

    @Test
    public void testJoinGroupIncosistentProtocolType() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        String otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        assertEquals(Errors.NONE, joinGroupResult.getError());

        JoinGroupResult otherJoinGroupResult = joinGroup(
            groupId, otherMemberId, "connect", protocols
        );
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, otherJoinGroupResult.getError());
    }

    @Test
    public void testJoinGroupWithEmptyProtocolType() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, "", protocols
        );
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, joinGroupResult.getError());
    }

    @Test
    public void testJoinGroupWithEmptyGroupProtocol() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, "", Collections.emptyMap()
        );
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, joinGroupResult.getError());
    }

    @Test
    public void testJoinGroupInconsistentGroupProtocol() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        String otherMemberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        assertEquals(Errors.NONE, joinGroupResult.getError());

        Map<String, byte[]> protocols = new HashMap<>();
        protocols.put("roundrobin", new byte[0]);
        JoinGroupResult otherJoinGroupResult = joinGroup(
            groupId, otherMemberId, protocolType, protocols
        );
        assertEquals(Errors.INCONSISTENT_GROUP_PROTOCOL, otherJoinGroupResult.getError());
    }

    @Test
    public void testJoinGroupUnknownConsumerExistingGroup() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        String otherMemberId = "memberId";
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        assertEquals(Errors.NONE, joinGroupResult.getError());
        JoinGroupResult otherJoinGroupRequest = joinGroup(
            groupId, otherMemberId, protocolType, protocols
        );
        assertEquals(Errors.UNKNOWN_MEMBER_ID, otherJoinGroupRequest.getError());
    }

    @Test
    public void testHeartbeatWrongCoordinator() throws Exception {
        Errors error = groupCoordinator.handleHeartbeat(otherGroupId, memberId, -1).get();
        assertEquals(Errors.NOT_COORDINATOR, error);
    }

    @Test
    public void testHeartbeatUnknowConsumerExistingGroup() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        String otherMemberId = "memberId";

        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        String assignedMemberId = joinGroupResult.getMemberId();
        Errors joinGroupErrors = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupErrors);

        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, joinGroupResult.getGenerationId(), assignedMemberId,
            ImmutableMap.<String, byte[]>builder()
                .put(assignedMemberId, new byte[0])
                .build()
        ).get();
        assertEquals(Errors.NONE, syncGroupResult.getKey());

        Errors heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, otherMemberId, 1
        ).get();
        assertEquals(Errors.UNKNOWN_MEMBER_ID, heartbeatResult);
    }

    @Test
    public void testHeartbeatRebalanceInProgress() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        String assignedMemberId = joinGroupResult.getMemberId();
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupError);

        Errors heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, assignedMemberId, 2
        ).get();
        assertEquals(Errors.REBALANCE_IN_PROGRESS, heartbeatResult);
    }

    @Test
    public void testHeartbeatIllegalGeneration() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        String assignedMemberId = joinGroupResult.getMemberId();
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupError);

        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, joinGroupResult.getGenerationId(), assignedMemberId,
            ImmutableMap.<String, byte[]>builder()
                .put(assignedMemberId, new byte[0])
                .build()
        ).get();
        assertEquals(Errors.NONE, syncGroupResult.getKey());

        Errors heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, assignedMemberId, 2
        ).get();
        assertEquals(Errors.ILLEGAL_GENERATION, heartbeatResult);
    }

    @Test
    public void testValidHeartbeat() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        String assignedConsumerId = joinGroupResult.getMemberId();
        int generationId = joinGroupResult.getGenerationId();
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupError);

        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, generationId, assignedConsumerId,
            ImmutableMap.<String, byte[]>builder()
                .put(assignedConsumerId, new byte[0])
                .build()
        ).get();
        assertEquals(Errors.NONE, syncGroupResult.getKey());

        Errors heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, assignedConsumerId, 1
        ).get();
        assertEquals(Errors.NONE, heartbeatResult);
    }

    @Test(enabled = false)
    // todo: https://github.com/streamnative/kop/issues/108
    public void testSessionTimeout() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        String assignedConsumerId = joinGroupResult.getMemberId();
        int generationId = joinGroupResult.getGenerationId();
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupError);

        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, generationId, assignedConsumerId,
            ImmutableMap.<String, byte[]>builder()
                .put(assignedConsumerId, new byte[0])
                .build()
        ).get();
        assertEquals(Errors.NONE, syncGroupResult.getKey());

        timer.advanceClock(DefaultSessionTimeout + 100);

        Errors heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, assignedConsumerId, 1
        ).get();
        assertEquals(Errors.UNKNOWN_MEMBER_ID, heartbeatResult);
    }

    @Test
    public void testHeartbeatMaintainsSession() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        int sessionTimeout = 1000;

        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols,
            sessionTimeout, sessionTimeout
        );
        String assignedConsumerId = joinGroupResult.getMemberId();
        int generationId = joinGroupResult.getGenerationId();
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupError);

        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, generationId, assignedConsumerId,
            ImmutableMap.<String, byte[]>builder()
                .put(assignedConsumerId, new byte[0])
                .build()
        ).get();
        assertEquals(Errors.NONE, syncGroupResult.getKey());

        timer.advanceClock(sessionTimeout / 2);

        Errors heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, assignedConsumerId, 1
        ).get();
        assertEquals(Errors.NONE, heartbeatResult);

        timer.advanceClock(sessionTimeout / 2 + 100);

        heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, assignedConsumerId, 1
        ).get();
        assertEquals(Errors.NONE, heartbeatResult);
    }

    @Test
    public void testCommitMaintainsSession() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        int sessionTimeout = 1000;
        TopicPartition tp = new TopicPartition("topic", 0);
        OffsetAndMetadata offset = OffsetAndMetadata.apply(0);

        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols,
            sessionTimeout, sessionTimeout
        );
        String assignedConsumerId = joinGroupResult.getMemberId();
        int generationId = joinGroupResult.getGenerationId();
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupError);

        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, generationId, assignedConsumerId,
            ImmutableMap.<String, byte[]>builder()
                .put(assignedConsumerId, new byte[0])
                .build()
        ).get();
        assertEquals(Errors.NONE, syncGroupResult.getKey());

        timer.advanceClock(sessionTimeout / 2);

        Map<TopicPartition, Errors> commitOffsetResult = groupCoordinator.handleCommitOffsets(
            groupId, assignedConsumerId, 1,
            ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(tp, offset)
                .build()
        ).get();
        assertEquals(Errors.NONE, commitOffsetResult.get(tp));

        timer.advanceClock(sessionTimeout / 2 + 100);

        Errors heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, assignedConsumerId, 1
        ).get();
        assertEquals(Errors.NONE, heartbeatResult);
    }

    @Test
    public void testSessionTimeoutDuringRebalance() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        int rebalanceTimeout = 2000;
        int sessionTimeout = 1000;

        JoinGroupResult firstJoinResult = joinGroup(
            groupId, memberId, protocolType, protocols,
            rebalanceTimeout, sessionTimeout
        );
        String firstMemberId = firstJoinResult.getMemberId();
        int firstGenerationId = firstJoinResult.getGenerationId();
        assertEquals(firstMemberId, firstJoinResult.getLeaderId());
        assertEquals(Errors.NONE, firstJoinResult.getError());

        KeyValue<Errors, byte[]> firstSyncResult = groupCoordinator.handleSyncGroup(
            groupId, firstGenerationId, firstMemberId,
            ImmutableMap.<String, byte[]>builder()
                .put(firstMemberId, new byte[0])
                .build()
        ).get();
        assertEquals(Errors.NONE, firstSyncResult.getKey());

        CompletableFuture<JoinGroupResult> otherJoinFuture = groupCoordinator.handleJoinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, "clientId", "clientHost",
            DefaultRebalanceTimeout, DefaultSessionTimeout,
            protocolType, protocols
        );

        timer.advanceClock(sessionTimeout / 2);

        Errors heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, firstMemberId, firstGenerationId
        ).get();
        assertEquals(Errors.REBALANCE_IN_PROGRESS, heartbeatResult);

        timer.advanceClock(rebalanceTimeout + 100);

        heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, firstMemberId, firstGenerationId
        ).get();
        assertEquals(Errors.UNKNOWN_MEMBER_ID, heartbeatResult);

        JoinGroupResult otherJoinResult = otherJoinFuture.get();
        assertEquals(Errors.NONE, otherJoinResult.getError());
    }

    @Test
    public void testRebalanceCompleteBeforeMemberJoins() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        int rebalanceTimeout = 1200;
        int sessionTimeout = 1000;

        JoinGroupResult firstJoinResult = joinGroup(
            groupId, memberId, protocolType, protocols,
            rebalanceTimeout, sessionTimeout
        );
        String firstMemberId = firstJoinResult.getMemberId();
        int firstGenerationId = firstJoinResult.getGenerationId();
        assertEquals(firstMemberId, firstJoinResult.getLeaderId());
        assertEquals(Errors.NONE, firstJoinResult.getError());

        KeyValue<Errors, byte[]> firstSyncResult = groupCoordinator.handleSyncGroup(
            groupId, firstGenerationId, firstMemberId,
            ImmutableMap.<String, byte[]>builder()
                .put(firstMemberId, new byte[0])
                .build()
        ).get();
        assertEquals(Errors.NONE, firstSyncResult.getKey());

        CompletableFuture<JoinGroupResult> otherJoinFuture = groupCoordinator.handleJoinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, "clientId", "clientHost",
            DefaultRebalanceTimeout, DefaultSessionTimeout,
            protocolType, protocols
        );

        // send a couple heartbeats to keep the member alive while the rebalance finishes
        timer.advanceClock(sessionTimeout / 2);

        Errors heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, firstMemberId, firstGenerationId
        ).get();
        assertEquals(Errors.REBALANCE_IN_PROGRESS, heartbeatResult);

        timer.advanceClock(sessionTimeout / 2);

        heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, firstMemberId, firstGenerationId
        ).get();
        assertEquals(Errors.REBALANCE_IN_PROGRESS, heartbeatResult);

        // now timeout the rebalance
        timer.advanceClock(rebalanceTimeout);
        JoinGroupResult otherJoinResult = otherJoinFuture.get();
        String otherMemberId = otherJoinResult.getMemberId();
        int otherGenerationId = otherJoinResult.getGenerationId();
        KeyValue<Errors, byte[]> syncResult = groupCoordinator.handleSyncGroup(
            groupId, otherGenerationId, otherMemberId,
            ImmutableMap.<String, byte[]>builder()
                .put(otherMemberId, new byte[0])
                .build()
        ).get();
        assertEquals(Errors.NONE, syncResult.getKey());

        // the unjoined member should be kicked out from the group
        assertEquals(Errors.NONE, otherJoinResult.getError());
        heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, firstMemberId, firstGenerationId
        ).get();
        assertEquals(Errors.UNKNOWN_MEMBER_ID, heartbeatResult);

        // the joined member should get heart beat response with no error.
        // Let the new member keep heartbeating for a while to verify that
        // no new rebalance is triggered unexpectedly
        for (int i = 0; i < 20; i++) {
            timer.advanceClock(sessionTimeout / 2);
            heartbeatResult = groupCoordinator.handleHeartbeat(
                groupId, otherMemberId, otherGenerationId
            ).get();
            assertEquals(Errors.NONE, heartbeatResult);
        }
    }

    @Test
    public void testSyncGroupEmptyAssignment() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        String assignedMemberId = joinGroupResult.getMemberId();
        int generationId = joinGroupResult.getGenerationId();
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupError);

        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, generationId, assignedMemberId,
            ImmutableMap.<String, byte[]>builder()
                .build()
        ).get();
        assertEquals(Errors.NONE, syncGroupResult.getKey());
        assertTrue(syncGroupResult.getValue().length == 0);

        Errors heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, assignedMemberId, 1
        ).get();
        assertEquals(Errors.NONE, heartbeatResult);
    }

    @Test
    public void testSyncGroupOtherGroupId() throws Exception {
        int generation = 1;
        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            otherGroupId, generation, memberId,
            ImmutableMap.<String, byte[]>builder()
                .build()
        ).get();
        assertEquals(Errors.NOT_COORDINATOR, syncGroupResult.getKey());
    }

    @Test
    public void testSyncGroupFromUnknownGroup() throws Exception {
        int generation = 1;
        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, generation, memberId,
            ImmutableMap.<String, byte[]>builder()
                .build()
        ).get();
        assertEquals(Errors.UNKNOWN_MEMBER_ID, syncGroupResult.getKey());
    }

    @Test
    public void testSyncGroupFromUnknownMember() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        String assignedConsumerId = joinGroupResult.getMemberId();
        int generationId = joinGroupResult.getGenerationId();
        assertEquals(Errors.NONE, joinGroupResult.getError());

        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, generationId, assignedConsumerId,
            ImmutableMap.<String, byte[]>builder()
                .put(assignedConsumerId, new byte[0])
                .build()
        ).get();
        assertEquals(Errors.NONE, syncGroupResult.getKey());

        String unknownMemberId = "blah";
        KeyValue<Errors, byte[]> unknownMemberSyncResult = groupCoordinator.handleSyncGroup(
            groupId, generationId, unknownMemberId,
            ImmutableMap.<String, byte[]>builder()
                .build()
        ).get();
        assertEquals(Errors.UNKNOWN_MEMBER_ID, unknownMemberSyncResult.getKey());
    }

    @Test
    public void testSyncGroupFromIllegalGeneration() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        String assignedConsumerId = joinGroupResult.getMemberId();
        int generationId = joinGroupResult.getGenerationId();
        assertEquals(Errors.NONE, joinGroupResult.getError());

        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, generationId + 1, assignedConsumerId,
            ImmutableMap.<String, byte[]>builder()
                .put(assignedConsumerId, new byte[0])
                .build()
        ).get();
        assertEquals(Errors.ILLEGAL_GENERATION, syncGroupResult.getKey());
    }

    @Test
    public void testJoinGroupFromUnchangedFollowerDoesNotRebalance() throws Exception {
        // to get a group of two members:
        // 1. join and sync with a single member (because we can't immediately join with two members)
        // 2. join and sync with the first member and a new member

        JoinGroupResult firstJoinResult = joinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, newProtocols()
        );
        String firstMemberId = firstJoinResult.getMemberId();
        int firstGenerationId = firstJoinResult.getGenerationId();
        assertEquals(Errors.NONE, firstJoinResult.getError());

        Map<String, byte[]> assignments = new HashMap<>();
        assignments.put(firstMemberId, new byte[0]);
        KeyValue<Errors, byte[]> firstSyncResult = groupCoordinator.handleSyncGroup(
            groupId, firstGenerationId, firstMemberId, assignments
        ).get();
        assertEquals(Errors.NONE, firstSyncResult.getKey());

        CompletableFuture<JoinGroupResult> otherJoinFuture = groupCoordinator.handleJoinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
            "clientId", "clientHost",
            DefaultRebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols()
        );
        CompletableFuture<JoinGroupResult> joinFuture = groupCoordinator.handleJoinGroup(
            groupId, firstMemberId,
            "clientId", "clientHost",
            DefaultRebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols()
        );

        JoinGroupResult joinResult = joinFuture.get();
        JoinGroupResult otherJoinResult = otherJoinFuture.get();

        assertEquals(Errors.NONE, joinResult.getError());
        assertEquals(Errors.NONE, otherJoinResult.getError());
        assertEquals(joinResult.getGenerationId(), otherJoinResult.getGenerationId());

        assertEquals(firstMemberId, joinResult.getLeaderId());
        assertEquals(firstMemberId, otherJoinResult.getLeaderId());

        long nextGenerationId = joinResult.getGenerationId();
        JoinGroupResult followerJoinResult = groupCoordinator.handleJoinGroup(
            groupId, otherJoinResult.getMemberId(),
            "clientId", "clientHost",
            DefaultRebalanceTimeout, DefaultSessionTimeout,
            protocolType, protocols
        ).get();
        assertEquals(Errors.NONE, followerJoinResult.getError());
        assertEquals(nextGenerationId, followerJoinResult.getGenerationId());
    }

    @Test
    public void testJoinGroupFromUnchangedLeaderShouldRebalance() throws Exception {
        JoinGroupResult firstJoinResult = joinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, newProtocols()
        );
        String firstMemberId = firstJoinResult.getMemberId();
        int firstGenerationId = firstJoinResult.getGenerationId();
        assertEquals(Errors.NONE, firstJoinResult.getError());

        Map<String, byte[]> assignments = new HashMap<>();
        assignments.put(firstMemberId, new byte[0]);
        KeyValue<Errors, byte[]> firstSyncResult = groupCoordinator.handleSyncGroup(
            groupId, firstGenerationId, firstMemberId, assignments
        ).get();
        assertEquals(Errors.NONE, firstSyncResult.getKey());

        JoinGroupResult secondJoinResult = groupCoordinator.handleJoinGroup(
            groupId, firstMemberId,
            "clientId", "clientHost",
            DefaultRebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols()
        ).get();

        assertEquals(Errors.NONE, secondJoinResult.getError());
        assertNotEquals(firstGenerationId, secondJoinResult.getGenerationId());
    }

    @Test
    public void testLeaderFailureInSyncGroup() throws Exception {
        // to get a group of two members:
        // 1. join and sync with a single member (because we can't immediately join with two members)
        // 2. join and sync with the first member and a new member

        JoinGroupResult firstJoinResult = joinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, newProtocols()
        );
        String firstMemberId = firstJoinResult.getMemberId();
        int firstGenerationId = firstJoinResult.getGenerationId();
        assertEquals(Errors.NONE, firstJoinResult.getError());

        Map<String, byte[]> assignments = new HashMap<>();
        assignments.put(firstMemberId, new byte[0]);
        KeyValue<Errors, byte[]> firstSyncResult = groupCoordinator.handleSyncGroup(
            groupId, firstGenerationId, firstMemberId, assignments
        ).get();
        assertEquals(Errors.NONE, firstSyncResult.getKey());

        CompletableFuture<JoinGroupResult> otherJoinFuture = groupCoordinator.handleJoinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
            "clientId", "clientHost",
            DefaultRebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols()
        );
        CompletableFuture<JoinGroupResult> joinFuture = groupCoordinator.handleJoinGroup(
            groupId, firstMemberId,
            "clientId", "clientHost",
            DefaultRebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols()
        );

        JoinGroupResult joinResult = joinFuture.get();
        JoinGroupResult otherJoinResult = otherJoinFuture.get();

        assertEquals(Errors.NONE, joinResult.getError());
        assertEquals(Errors.NONE, otherJoinResult.getError());
        assertEquals(joinResult.getGenerationId(), otherJoinResult.getGenerationId());

        assertEquals(firstMemberId, joinResult.getLeaderId());
        assertEquals(firstMemberId, otherJoinResult.getLeaderId());

        int nextGenerationId = joinResult.getGenerationId();
        assignments = new HashMap<>();
        assignments.put(firstMemberId, new byte[0]);
        CompletableFuture<KeyValue<Errors, byte[]>> followerSyncFuture = groupCoordinator.handleSyncGroup(
            groupId, nextGenerationId, otherJoinResult.getMemberId(), assignments
        );

        timer.advanceClock(DefaultSessionTimeout + 100);

        KeyValue<Errors, byte[]> followerSyncResult = followerSyncFuture.get();
        assertEquals(Errors.REBALANCE_IN_PROGRESS, followerSyncResult.getKey());
    }

    @Test
    public void testSyncGroupFollowerAfterLeader() throws Exception {
        // to get a group of two members:
        // 1. join and sync with a single member (because we can't immediately join with two members)
        // 2. join and sync with the first member and a new member

        JoinGroupResult firstJoinResult = joinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, newProtocols()
        );
        String firstMemberId = firstJoinResult.getMemberId();
        int firstGenerationId = firstJoinResult.getGenerationId();
        assertEquals(Errors.NONE, firstJoinResult.getError());

        Map<String, byte[]> assignments = new HashMap<>();
        assignments.put(firstMemberId, new byte[0]);
        KeyValue<Errors, byte[]> firstSyncResult = groupCoordinator.handleSyncGroup(
            groupId, firstGenerationId, firstMemberId, assignments
        ).get();
        assertEquals(Errors.NONE, firstSyncResult.getKey());

        CompletableFuture<JoinGroupResult> otherJoinFuture = groupCoordinator.handleJoinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
            "clientId", "clientHost",
            DefaultRebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols()
        );
        CompletableFuture<JoinGroupResult> joinFuture = groupCoordinator.handleJoinGroup(
            groupId, firstMemberId,
            "clientId", "clientHost",
            DefaultRebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols()
        );

        JoinGroupResult joinResult = joinFuture.get();
        JoinGroupResult otherJoinResult = otherJoinFuture.get();

        assertEquals(Errors.NONE, joinResult.getError());
        assertEquals(Errors.NONE, otherJoinResult.getError());
        assertEquals(joinResult.getGenerationId(), otherJoinResult.getGenerationId());

        assertEquals(firstMemberId, joinResult.getLeaderId());
        assertEquals(firstMemberId, otherJoinResult.getLeaderId());

        int nextGenerationId = joinResult.getGenerationId();
        String leaderId = firstMemberId;
        byte[] leaderAssignment = new byte[0];
        String followerId = otherJoinResult.getMemberId();
        byte[] followerAssignment = new byte[1];

        assignments = new HashMap<>();
        assignments.put(leaderId, leaderAssignment);
        assignments.put(followerId, followerAssignment);
        KeyValue<Errors, byte[]> leaderSyncResult = groupCoordinator.handleSyncGroup(
            groupId, nextGenerationId, leaderId, assignments
        ).get();
        assertEquals(Errors.NONE, leaderSyncResult.getKey());
        assertEquals(leaderAssignment, leaderSyncResult.getValue());

        assignments = new HashMap<>();
        KeyValue<Errors, byte[]> followerSyncResult = groupCoordinator.handleSyncGroup(
            groupId, nextGenerationId, followerId, assignments
        ).get();
        assertEquals(Errors.NONE, followerSyncResult.getKey());
        assertEquals(followerAssignment, followerSyncResult.getValue());
    }

    @Test
    public void testSyncGroupLeaderAfterFollower() throws Exception {
        // to get a group of two members:
        // 1. join and sync with a single member (because we can't immediately join with two members)
        // 2. join and sync with the first member and a new member

        JoinGroupResult firstJoinResult = joinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID, protocolType, newProtocols()
        );
        String firstMemberId = firstJoinResult.getMemberId();
        int firstGenerationId = firstJoinResult.getGenerationId();
        assertEquals(Errors.NONE, firstJoinResult.getError());

        Map<String, byte[]> assignments = new HashMap<>();
        assignments.put(firstMemberId, new byte[0]);
        KeyValue<Errors, byte[]> firstSyncResult = groupCoordinator.handleSyncGroup(
            groupId, firstGenerationId, firstMemberId, assignments
        ).get();
        assertEquals(Errors.NONE, firstSyncResult.getKey());

        CompletableFuture<JoinGroupResult> otherJoinFuture = groupCoordinator.handleJoinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
            "clientId", "clientHost",
            DefaultRebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols()
        );
        CompletableFuture<JoinGroupResult> joinFuture = groupCoordinator.handleJoinGroup(
            groupId, firstMemberId,
            "clientId", "clientHost",
            DefaultRebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols()
        );

        JoinGroupResult joinResult = joinFuture.get();
        JoinGroupResult otherJoinResult = otherJoinFuture.get();

        assertEquals(Errors.NONE, joinResult.getError());
        assertEquals(Errors.NONE, otherJoinResult.getError());
        assertEquals(joinResult.getGenerationId(), otherJoinResult.getGenerationId());

        assertEquals(firstMemberId, joinResult.getLeaderId());
        assertEquals(firstMemberId, otherJoinResult.getLeaderId());

        int nextGenerationId = joinResult.getGenerationId();
        String leaderId = firstMemberId;
        byte[] leaderAssignment = new byte[0];
        String followerId = otherJoinResult.getMemberId();
        byte[] followerAssignment = new byte[1];

        assignments = new HashMap<>();
        CompletableFuture<KeyValue<Errors, byte[]>> followerSyncFuture = groupCoordinator.handleSyncGroup(
            groupId, nextGenerationId, followerId, assignments
        );

        assignments = new HashMap<>();
        assignments.put(leaderId, leaderAssignment);
        assignments.put(followerId, followerAssignment);
        KeyValue<Errors, byte[]> leaderSyncResult = groupCoordinator.handleSyncGroup(
            groupId, nextGenerationId, leaderId, assignments
        ).get();
        assertEquals(Errors.NONE, leaderSyncResult.getKey());
        assertEquals(leaderAssignment, leaderSyncResult.getValue());

        KeyValue<Errors, byte[]> followerSyncResult = followerSyncFuture.get();
        assertEquals(Errors.NONE, followerSyncResult.getKey());
        assertEquals(followerAssignment, followerSyncResult.getValue());
    }

    @Test
    public void testCommitOffsetFromUnknownGroup() throws Exception {
        int generationId = 1;
        TopicPartition tp = new TopicPartition("topic", 0);
        OffsetAndMetadata offset = OffsetAndMetadata.apply(0);

        Map<TopicPartition, Errors> commitOffsetResult = groupCoordinator.handleCommitOffsets(
            groupId, memberId, generationId, ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(tp, offset)
                .build()
        ).get();
        assertEquals(Errors.ILLEGAL_GENERATION, commitOffsetResult.get(tp));
    }

    @Test
    public void testCommitOffsetWithDefaultGeneration() throws Exception {
        TopicPartition tp = new TopicPartition("topic", 0);
        OffsetAndMetadata offset = OffsetAndMetadata.apply(0);

        Map<TopicPartition, Errors> commitOffsetResult = groupCoordinator.handleCommitOffsets(
            groupId, memberId, OffsetCommitRequest.DEFAULT_GENERATION_ID,
            ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(tp, offset)
                .build()
        ).get();
        assertEquals(Errors.NONE, commitOffsetResult.get(tp));
    }

    @Test
    public void testCommitOffsetAfterGroupIsEmpty() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, newProtocols()
        );
        String assignedMemberId = joinGroupResult.getMemberId();
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupError);

        // and leaves
        Errors leaveGroupResult = groupCoordinator.handleLeaveGroup(groupId, assignedMemberId).get();
        assertEquals(Errors.NONE, leaveGroupResult);

        TopicPartition tp = new TopicPartition("topic", 0);
        OffsetAndMetadata offset = OffsetAndMetadata.apply(0);

        Map<TopicPartition, Errors> commitOffsetResult = groupCoordinator.handleCommitOffsets(
            groupId, OffsetCommitRequest.DEFAULT_MEMBER_ID, OffsetCommitRequest.DEFAULT_GENERATION_ID,
            ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(tp, offset)
                .build()
        ).get();
        assertEquals(Errors.NONE, commitOffsetResult.get(tp));

        KeyValue<Errors, Map<TopicPartition, PartitionData>> offsetFetchResult =
            groupCoordinator.handleFetchOffsets(groupId, Optional.of(Lists.newArrayList(tp)));

        assertEquals(Errors.NONE, offsetFetchResult.getKey());
        assertEquals(0L, offsetFetchResult.getValue().get(tp).offset);
    }

    @Test
    public void testFetchOffsets() throws Exception {
        TopicPartition tp = new TopicPartition("topic", 0);
        OffsetAndMetadata offset = OffsetAndMetadata.apply(0);

        Map<TopicPartition, Errors> commitOffsetResult = groupCoordinator.handleCommitOffsets(
            groupId, OffsetCommitRequest.DEFAULT_MEMBER_ID, OffsetCommitRequest.DEFAULT_GENERATION_ID,
            ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(tp, offset)
                .build()
        ).get();
        assertEquals(Errors.NONE, commitOffsetResult.get(tp));

        KeyValue<Errors, Map<TopicPartition, PartitionData>> fetchOffsetsResult =
            groupCoordinator.handleFetchOffsets(groupId, Optional.of(Lists.newArrayList(tp)));
        assertEquals(Errors.NONE, fetchOffsetsResult.getKey());
        assertEquals(0, fetchOffsetsResult.getValue().get(tp).offset);
    }

    @Test
    public void testCommitAndFetchOffsetsWithEmptyGroup() throws Exception {
        // For backwards compatibility, the coordinator supports committing/fetching offsets with an empty groupId.
        // To allow inspection and removal of the empty group, we must also support DescribeGroups and DeleteGroups

        TopicPartition tp = new TopicPartition("topic", 0);
        OffsetAndMetadata offset = OffsetAndMetadata.apply(0);
        String groupId = "";

        Map<TopicPartition, Errors> commitOffsetResult = groupCoordinator.handleCommitOffsets(
            groupId, OffsetCommitRequest.DEFAULT_MEMBER_ID, OffsetCommitRequest.DEFAULT_GENERATION_ID,
            ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(tp, offset)
                .build()
        ).get();
        assertEquals(Errors.NONE, commitOffsetResult.get(tp));

        KeyValue<Errors, Map<TopicPartition, PartitionData>> fetchOffsetsResult =
            groupCoordinator.handleFetchOffsets(groupId, Optional.of(Lists.newArrayList(tp)));
        assertEquals(Errors.NONE, fetchOffsetsResult.getKey());
        assertEquals(0, fetchOffsetsResult.getValue().get(tp).offset);

        KeyValue<Errors, GroupSummary> describeGroupResult = groupCoordinator.handleDescribeGroup(groupId);
        assertEquals(Errors.NONE, describeGroupResult.getKey());
        assertEquals(GroupState.Empty.toString(), describeGroupResult.getValue().state());

        TopicPartition groupTopicPartition = new TopicPartition(
            Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId
        );

        Map<String, Errors> deleteErrors = groupCoordinator.handleDeleteGroups(Sets.newHashSet(groupId));
        assertEquals(Errors.NONE, deleteErrors.get(groupId));

        KeyValue<Errors, Map<TopicPartition, PartitionData>> fetchOffsetsResult2 =
            groupCoordinator.handleFetchOffsets(groupId, Optional.of(Lists.newArrayList(tp)));
        assertEquals(Errors.NONE, fetchOffsetsResult2.getKey());
        assertEquals(OffsetFetchResponse.INVALID_OFFSET, fetchOffsetsResult2.getValue().get(tp).offset);
    }

    @Test
    public void testBasicFetchTxnOffsets() throws Exception {
        TopicPartition tp = new TopicPartition("topic", 0);
        OffsetAndMetadata offset = OffsetAndMetadata.apply(0);
        long producerId = 1000L;
        short producerEpoch = 2;

        Map<TopicPartition, Errors> commitOffsetResult = groupCoordinator.handleTxnCommitOffsets(
            groupId, producerId, producerEpoch,
            ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(tp, offset)
                .build()
        ).get();
        assertEquals(Errors.NONE, commitOffsetResult.get(tp));

        KeyValue<Errors, Map<TopicPartition, PartitionData>> fetchOffsetsResult = groupCoordinator.handleFetchOffsets(
            groupId, Optional.of(Lists.newArrayList(tp))
        );

        // Validate that the offset isn't materialized yet.
        assertEquals(Errors.NONE, fetchOffsetsResult.getKey());
        assertEquals(OffsetFetchResponse.INVALID_OFFSET, fetchOffsetsResult.getValue().get(tp).offset);

        TopicPartition offsetsTopic = new TopicPartition(
            Topic.GROUP_METADATA_TOPIC_NAME,
            groupPartitionId
        );

        // send commit marker
        groupCoordinator.scheduleHandleTxnCompletion(
            producerId,
            Lists.newArrayList(offsetsTopic).stream(),
            TransactionResult.COMMIT
        ).get();

        // validate that committed offset is materialized
        KeyValue<Errors, Map<TopicPartition, PartitionData>> offsetFetchResult = groupCoordinator.handleFetchOffsets(
            groupId, Optional.of(Lists.newArrayList(tp))
        );
        assertEquals(Errors.NONE, offsetFetchResult.getKey());
        assertEquals(0, offsetFetchResult.getValue().get(tp).offset);
    }

    @Test
    public void testFetchTxnOffsetsWithAbort() throws Exception {
        TopicPartition tp = new TopicPartition("topic", 0);
        OffsetAndMetadata offset = OffsetAndMetadata.apply(0);
        long producerId = 1000L;
        short producerEpoch = 2;

        Map<TopicPartition, Errors> commitOffsetResult = groupCoordinator.handleTxnCommitOffsets(
            groupId, producerId, producerEpoch,
            ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(tp, offset)
                .build()
        ).get();
        assertEquals(Errors.NONE, commitOffsetResult.get(tp));

        KeyValue<Errors, Map<TopicPartition, PartitionData>> fetchOffsetsResult = groupCoordinator.handleFetchOffsets(
            groupId, Optional.of(Lists.newArrayList(tp))
        );

        // Validate that the offset isn't materialized yet.
        assertEquals(Errors.NONE, fetchOffsetsResult.getKey());
        assertEquals(OffsetFetchResponse.INVALID_OFFSET, fetchOffsetsResult.getValue().get(tp).offset);

        TopicPartition offsetsTopic = new TopicPartition(
            Topic.GROUP_METADATA_TOPIC_NAME,
            groupPartitionId
        );

        // send commit marker
        groupCoordinator.scheduleHandleTxnCompletion(
            producerId,
            Lists.newArrayList(offsetsTopic).stream(),
            TransactionResult.ABORT
        ).get();

        KeyValue<Errors, Map<TopicPartition, PartitionData>> offsetFetchResult = groupCoordinator.handleFetchOffsets(
            groupId, Optional.of(Lists.newArrayList(tp))
        );
        assertEquals(Errors.NONE, offsetFetchResult.getKey());
        assertEquals(OffsetFetchResponse.INVALID_OFFSET, offsetFetchResult.getValue().get(tp).offset);
    }

    @Test
    public void testFetchTxnOffsetsIgnoreSpuriousCommit() throws Exception {
        TopicPartition tp = new TopicPartition("topic", 0);
        OffsetAndMetadata offset = OffsetAndMetadata.apply(0);
        long producerId = 1000L;
        short producerEpoch = 2;

        Map<TopicPartition, Errors> commitOffsetResult = groupCoordinator.handleTxnCommitOffsets(
            groupId, producerId, producerEpoch,
            ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(tp, offset)
                .build()
        ).get();
        assertEquals(Errors.NONE, commitOffsetResult.get(tp));

        KeyValue<Errors, Map<TopicPartition, PartitionData>> fetchOffsetsResult = groupCoordinator.handleFetchOffsets(
            groupId, Optional.of(Lists.newArrayList(tp))
        );

        // Validate that the offset isn't materialized yet.
        assertEquals(Errors.NONE, fetchOffsetsResult.getKey());
        assertEquals(OffsetFetchResponse.INVALID_OFFSET, fetchOffsetsResult.getValue().get(tp).offset);

        TopicPartition offsetsTopic = new TopicPartition(
            Topic.GROUP_METADATA_TOPIC_NAME,
            groupPartitionId
        );

        // send commit marker
        groupCoordinator.scheduleHandleTxnCompletion(
            producerId,
            Lists.newArrayList(offsetsTopic).stream(),
            TransactionResult.ABORT
        ).get();

        KeyValue<Errors, Map<TopicPartition, PartitionData>> offsetFetchResult = groupCoordinator.handleFetchOffsets(
            groupId, Optional.of(Lists.newArrayList(tp))
        );
        assertEquals(Errors.NONE, offsetFetchResult.getKey());
        assertEquals(OffsetFetchResponse.INVALID_OFFSET, offsetFetchResult.getValue().get(tp).offset);

        // ignore spurious commit
        groupCoordinator.scheduleHandleTxnCompletion(
            producerId,
            Lists.newArrayList(offsetsTopic).stream(),
            TransactionResult.COMMIT
        ).get();

        KeyValue<Errors, Map<TopicPartition, PartitionData>> offsetFetchResult2 = groupCoordinator.handleFetchOffsets(
            groupId, Optional.of(Lists.newArrayList(tp))
        );
        assertEquals(Errors.NONE, offsetFetchResult2.getKey());
        assertEquals(OffsetFetchResponse.INVALID_OFFSET, offsetFetchResult2.getValue().get(tp).offset);
    }

    @Test
    public void testFetchTxnOffsetsOneProducerMultipleGroups() throws Exception {
        // One producer, two groups located on separate offsets topic partitions.
        // Both group have pending offset commits.
        // Marker for only one partition is received. That commit should be materialized while the other should not.
        List<TopicPartition> partitions = Lists.newArrayList(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic2", 0)
        );
        List<OffsetAndMetadata> offsets = Lists.newArrayList(
            OffsetAndMetadata.apply(10),
            OffsetAndMetadata.apply(15)
        );

        long producerId = 1000L;
        short producerEpoch = 3;

        List<String> groupIds = Lists.newArrayList(groupId, otherGroupId);
        List<TopicPartition> offsetTopicPartitions = Lists.newArrayList(
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(groupId)),
            new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupMetadataManager.partitionFor(otherGroupId))
        );

        groupMetadataManager.addPartitionOwnership(offsetTopicPartitions.get(1).partition());
        List<Errors> errors = new ArrayList<>();
        List<Map<TopicPartition, PartitionData>> partitionData = new ArrayList<>();
        List<Map<TopicPartition, Errors>> commitOffsetResults = new ArrayList<>();

        // Ensure that the two groups map to different partitions.
        assertNotEquals(offsetTopicPartitions.get(0), offsetTopicPartitions.get(1));

        commitOffsetResults.add(
            groupCoordinator.handleTxnCommitOffsets(
                groupId, producerId, producerEpoch,
                ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                    .put(partitions.get(0), offsets.get(0)).build()).get());
        assertEquals(Errors.NONE, commitOffsetResults.get(0).get(partitions.get(0)));
        commitOffsetResults.add(
            groupCoordinator.handleTxnCommitOffsets(
                otherGroupId, producerId, producerEpoch,
                ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                    .put(partitions.get(1), offsets.get(1)).build()).get());
        assertEquals(Errors.NONE, commitOffsetResults.get(1).get(partitions.get(1)));

        // We got a commit for only one __consumer_offsets partition. We should only materialize it's group offsets.
        groupCoordinator.scheduleHandleTxnCompletion(
            producerId,
            Lists.newArrayList(offsetTopicPartitions.get(0)).stream(),
            TransactionResult.COMMIT
        ).get();

        KeyValue<Errors, Map<TopicPartition, PartitionData>> offsetFetchResult0 =
            groupCoordinator.handleFetchOffsets(groupIds.get(0), Optional.of(partitions));
        errors.add(offsetFetchResult0.getKey());
        partitionData.add(offsetFetchResult0.getValue());

        KeyValue<Errors, Map<TopicPartition, PartitionData>> offsetFetchResult1 =
            groupCoordinator.handleFetchOffsets(groupIds.get(1), Optional.of(partitions));
        errors.add(offsetFetchResult1.getKey());
        partitionData.add(offsetFetchResult1.getValue());

        assertEquals(2, errors.size());
        assertEquals(Errors.NONE, errors.get(0));
        assertEquals(Errors.NONE, errors.get(1));

        assertEquals(
            offsets.get(0).offset(),
            partitionData.get(0).get(partitions.get(0)).offset);
        assertEquals(
            OffsetFetchResponse.INVALID_OFFSET,
            partitionData.get(0).get(partitions.get(1)).offset);
        assertEquals(
            OffsetFetchResponse.INVALID_OFFSET,
            partitionData.get(1).get(partitions.get(0)).offset);
        assertEquals(
            OffsetFetchResponse.INVALID_OFFSET,
            partitionData.get(1).get(partitions.get(1)).offset);

        // Now we receive the other marker
        groupCoordinator.scheduleHandleTxnCompletion(
            producerId,
            Lists.newArrayList(offsetTopicPartitions.get(1)).stream(),
            TransactionResult.COMMIT
        ).get();
        errors.clear();
        partitionData.clear();

        offsetFetchResult0 =
            groupCoordinator.handleFetchOffsets(groupIds.get(0), Optional.of(partitions));
        errors.add(offsetFetchResult0.getKey());
        partitionData.add(offsetFetchResult0.getValue());

        offsetFetchResult1 =
            groupCoordinator.handleFetchOffsets(groupIds.get(1), Optional.of(partitions));
        errors.add(offsetFetchResult1.getKey());
        partitionData.add(offsetFetchResult1.getValue());

        // Two offsets should have been materialized
        assertEquals(
            offsets.get(0).offset(),
            partitionData.get(0).get(partitions.get(0)).offset);
        assertEquals(
            OffsetFetchResponse.INVALID_OFFSET,
            partitionData.get(0).get(partitions.get(1)).offset);
        assertEquals(
            OffsetFetchResponse.INVALID_OFFSET,
            partitionData.get(1).get(partitions.get(0)).offset);
        assertEquals(
            offsets.get(1).offset(),
            partitionData.get(1).get(partitions.get(1)).offset);
    }

    @Test
    public void testFetchTxnOffsetsMultipleProducersOneGroup() throws Exception {
        // One group, two producers
        // Different producers will commit offsets for different partitions.
        // Each partition's offsets should be materialized when the corresponding producer's marker is received.
        List<TopicPartition> partitions = Lists.newArrayList(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic2", 0)
        );
        List<OffsetAndMetadata> offsets = Lists.newArrayList(
            OffsetAndMetadata.apply(10),
            OffsetAndMetadata.apply(15)
        );

        List<Long> producerIds = Lists.newArrayList(1000L, 1005L);
        List<Short> producerEpochs = Lists.newArrayList((short) 3, (short) 4);

        TopicPartition offsetTopicPartition = new TopicPartition(
            Topic.GROUP_METADATA_TOPIC_NAME,
            groupMetadataManager.partitionFor(groupId)
        );

        List<Errors> errors = new ArrayList<>();
        List<Map<TopicPartition, PartitionData>> partitionData = new ArrayList<>();
        List<Map<TopicPartition, Errors>> commitOffsetResults = new ArrayList<>();

        // producer0 commits the offsets for partition0
        commitOffsetResults.add(
            groupCoordinator.handleTxnCommitOffsets(
                groupId, producerIds.get(0), producerEpochs.get(0),
                ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                    .put(partitions.get(0), offsets.get(0)).build()).get());
        assertEquals(Errors.NONE, commitOffsetResults.get(0).get(partitions.get(0)));

        // producer1 commits the offsets for partition1
        commitOffsetResults.add(
            groupCoordinator.handleTxnCommitOffsets(
                groupId, producerIds.get(1), producerEpochs.get(1),
                ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                    .put(partitions.get(1), offsets.get(1)).build()).get());
        assertEquals(Errors.NONE, commitOffsetResults.get(1).get(partitions.get(1)));

        // producer0 commits its transaction.
        groupCoordinator.scheduleHandleTxnCompletion(
            producerIds.get(0),
            Lists.newArrayList(offsetTopicPartition).stream(),
            TransactionResult.COMMIT
        ).get();

        KeyValue<Errors, Map<TopicPartition, PartitionData>> offsetFetchResult0 =
            groupCoordinator.handleFetchOffsets(groupId, Optional.of(partitions));
        errors.add(offsetFetchResult0.getKey());
        partitionData.add(offsetFetchResult0.getValue());

        assertEquals(Errors.NONE, errors.get(0));
        // we should only see the offset commit for producer0
        assertEquals(
            offsets.get(0).offset(),
            partitionData.get(0).get(partitions.get(0)).offset);
        assertEquals(
            OffsetFetchResponse.INVALID_OFFSET,
            partitionData.get(0).get(partitions.get(1)).offset);

        // producer 1 now commits its transaction
        groupCoordinator.scheduleHandleTxnCompletion(
            producerIds.get(1),
            Lists.newArrayList(offsetTopicPartition).stream(),
            TransactionResult.COMMIT
        ).get();

        KeyValue<Errors, Map<TopicPartition, PartitionData>> offsetFetchResult1 =
            groupCoordinator.handleFetchOffsets(groupId, Optional.of(partitions));
        errors.add(offsetFetchResult1.getKey());
        partitionData.add(offsetFetchResult1.getValue());

        assertEquals(Errors.NONE, errors.get(1));

        assertEquals(
            offsets.get(0).offset(),
            partitionData.get(1).get(partitions.get(0)).offset);
        assertEquals(
            offsets.get(1).offset(),
            partitionData.get(1).get(partitions.get(1)).offset);
    }

    @Test
    public void testFetchOffsetForUnknownPartition() {
        TopicPartition tp = new TopicPartition("topic", 0);
        KeyValue<Errors, Map<TopicPartition, PartitionData>> fetchOffsetsResult =
            groupCoordinator.handleFetchOffsets(
                groupId, Optional.of(Lists.newArrayList(tp)));
        assertEquals(Errors.NONE, fetchOffsetsResult.getKey());
        assertEquals(OffsetFetchResponse.INVALID_OFFSET, fetchOffsetsResult.getValue().get(tp).offset);
    }

    @Test
    public void testFetchOffsetNotCoordinatorForGroup() {
        TopicPartition tp = new TopicPartition("topic", 0);
        KeyValue<Errors, Map<TopicPartition, PartitionData>> fetchOffsetsResult =
            groupCoordinator.handleFetchOffsets(
                otherGroupId, Optional.of(Lists.newArrayList(tp)));
        assertEquals(Errors.NOT_COORDINATOR, fetchOffsetsResult.getKey());
        assertTrue(fetchOffsetsResult.getValue().isEmpty());
    }

    @Test
    public void testFetchAllOffsets() throws Exception {
        TopicPartition tp1 = new TopicPartition("topic", 0);
        TopicPartition tp2 = new TopicPartition("topic", 1);
        TopicPartition tp3 = new TopicPartition("other-topic", 0);
        OffsetAndMetadata offset1 = OffsetAndMetadata.apply(15);
        OffsetAndMetadata offset2 = OffsetAndMetadata.apply(16);
        OffsetAndMetadata offset3 = OffsetAndMetadata.apply(17);

        KeyValue<Errors, Map<TopicPartition, PartitionData>> fetchOffsetsResult =
            groupCoordinator.handleFetchOffsets(
                groupId,
                Optional.empty());

        assertEquals(Errors.NONE, fetchOffsetsResult.getKey());
        assertTrue(fetchOffsetsResult.getValue().isEmpty());

        Map<TopicPartition, Errors> commitOffsetResult = groupCoordinator.handleCommitOffsets(
            groupId, OffsetCommitRequest.DEFAULT_MEMBER_ID, OffsetCommitRequest.DEFAULT_GENERATION_ID,
            ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(tp1, offset1)
                .put(tp2, offset2)
                .put(tp3, offset3)
                .build()
        ).get();

        assertEquals(Errors.NONE, commitOffsetResult.get(tp1));
        assertEquals(Errors.NONE, commitOffsetResult.get(tp2));
        assertEquals(Errors.NONE, commitOffsetResult.get(tp3));

        fetchOffsetsResult = groupCoordinator.handleFetchOffsets(
            groupId,
            Optional.empty()
        );
        assertEquals(Errors.NONE, fetchOffsetsResult.getKey());
        assertEquals(3, fetchOffsetsResult.getValue().size());
        fetchOffsetsResult.getValue().forEach((tp, pd) -> {
            assertEquals(Errors.NONE, pd.error);
        });
        assertEquals(offset1.offset(), fetchOffsetsResult.getValue().get(tp1).offset);
        assertEquals(offset2.offset(), fetchOffsetsResult.getValue().get(tp2).offset);
        assertEquals(offset3.offset(), fetchOffsetsResult.getValue().get(tp3).offset);
    }

    @Test
    public void testCommitOffsetInCompletingRebalance() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        TopicPartition tp = new TopicPartition("topic", 0);
        OffsetAndMetadata offset = OffsetAndMetadata.apply(0);

        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        String assignedMemberId = joinGroupResult.getMemberId();
        int generationId = joinGroupResult.getGenerationId();
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupError);

        Map<TopicPartition, Errors> commitOffsetResult = groupCoordinator.handleCommitOffsets(
            groupId, assignedMemberId, generationId,
            ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(tp, offset)
                .build()
        ).get();
        assertEquals(Errors.REBALANCE_IN_PROGRESS, commitOffsetResult.get(tp));
    }

    @Test
    public void testHeartbeatDuringRebalanceCausesRebalanceInProgress() throws Exception {
        // First start up a group (with a slightly larger timeout to give us time to heartbeat
        // when the rebalance starts)
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;

        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, newProtocols()
        );
        String assignedMemberId = joinGroupResult.getMemberId();
        int initialGenerationId = joinGroupResult.getGenerationId();
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupError);

        // Then join with a new consumer to trigger a rebalance
        groupCoordinator.handleJoinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
            "clientId", "clientHost",
            DefaultRebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols());

        // We should be in the middle of a rebalance, so the heartbeat should return rebalance in progress
        Errors heartbeatResult = groupCoordinator.handleHeartbeat(
            groupId, assignedMemberId, initialGenerationId
        ).get();
        assertEquals(Errors.REBALANCE_IN_PROGRESS, heartbeatResult);
    }

    @Test
    public void testGenerationIdIncrementsOnRebalance() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, newProtocols()
        );
        String assignedMemberId = joinGroupResult.getMemberId();
        int initialGenerationId = joinGroupResult.getGenerationId();
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(1, initialGenerationId);
        assertEquals(Errors.NONE, joinGroupError);

        Map<String, byte[]> assignments = new HashMap<>();
        assignments.put(assignedMemberId, new byte[0]);
        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, initialGenerationId, assignedMemberId, assignments).get();
        assertEquals(Errors.NONE, syncGroupResult.getKey());

        JoinGroupResult otherJoinGroupResult = joinGroup(groupId, assignedMemberId, protocolType, newProtocols());

        int nextGenerationId = otherJoinGroupResult.getGenerationId();
        assertEquals(2, nextGenerationId);
        assertEquals(Errors.NONE, otherJoinGroupResult.getError());
    }

    @Test
    public void testLeaveGroupWrongCoordinator() throws Exception {
        Errors leaveGroupResult = groupCoordinator.handleLeaveGroup(
            otherGroupId, JoinGroupRequest.UNKNOWN_MEMBER_ID
        ).get();
        assertEquals(Errors.NOT_COORDINATOR, leaveGroupResult);
    }

    @Test
    public void testLeaveGroupUnknownGroup() throws Exception {
        Errors leaveGroupResult = groupCoordinator.handleLeaveGroup(
            groupId, memberId
        ).get();
        assertEquals(Errors.UNKNOWN_MEMBER_ID, leaveGroupResult);
    }

    @Test
    public void testLeaveGroupUnknownConsumerExistingGroup() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        String otherMemberId = "consumerId";

        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        assertEquals(Errors.NONE, joinGroupResult.getError());

        Errors leaveGroupResult = groupCoordinator.handleLeaveGroup(groupId, otherMemberId).get();
        assertEquals(Errors.UNKNOWN_MEMBER_ID, leaveGroupResult);
    }

    @Test
    public void testValidLeaveGroup() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;

        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, protocols
        );
        String assignedMemberId = joinGroupResult.getMemberId();
        assertEquals(Errors.NONE, joinGroupResult.getError());

        Errors leaveGroupResult = groupCoordinator.handleLeaveGroup(groupId, assignedMemberId).get();
        assertEquals(Errors.NONE, leaveGroupResult);
    }

    @Test
    public void testListGroupsIncludesStableGroups() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, newProtocols()
        );
        String assignedMemberId = joinGroupResult.getMemberId();
        int generationId = joinGroupResult.getGenerationId();
        assertEquals(Errors.NONE, joinGroupResult.getError());

        Map<String, byte[]> assignments = new HashMap<>();
        assignments.put(assignedMemberId, new byte[0]);
        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, generationId, assignedMemberId,
            assignments
        ).get();
        assertEquals(Errors.NONE, syncGroupResult.getKey());

        KeyValue<Errors, List<GroupOverview>> groups = groupCoordinator.handleListGroups();
        assertEquals(Errors.NONE, groups.getKey());
        assertEquals(1, groups.getValue().size());
        assertEquals(
            new GroupOverview("groupId", "consumer"),
            groups.getValue().get(0)
        );
    }

    @Test
    public void testListGroupsIncludesRebalancingGroups() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, newProtocols()
        );
        assertEquals(Errors.NONE, joinGroupResult.getError());

        KeyValue<Errors, List<GroupOverview>> groups = groupCoordinator.handleListGroups();
        assertEquals(Errors.NONE, groups.getKey());
        assertEquals(1, groups.getValue().size());
        assertEquals(
            new GroupOverview("groupId", "consumer"),
            groups.getValue().get(0)
        );
    }

    @Test
    public void testDescribeGroupWrongCoordinator() {
        KeyValue<Errors, GroupSummary> describeGroupResult = groupCoordinator.handleDescribeGroup(otherGroupId);
        assertEquals(Errors.NOT_COORDINATOR, describeGroupResult.getKey());
    }

    @Test
    public void testDescribeGroupInactiveGroup() {
        KeyValue<Errors, GroupSummary> describeGroupResult = groupCoordinator.handleDescribeGroup(groupId);
        assertEquals(Errors.NONE, describeGroupResult.getKey());
        assertEquals(GroupCoordinator.DeadGroup, describeGroupResult.getValue());
    }

    @Test
    public void testDescribeGroupStable() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, newProtocols()
        );
        String assignedMemberId = joinGroupResult.getMemberId();
        int generationId = joinGroupResult.getGenerationId();
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupError);

        Map<String, byte[]> assignments = new HashMap<>();
        assignments.put(assignedMemberId, new byte[0]);
        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, generationId, assignedMemberId, assignments
        ).get();
        assertEquals(Errors.NONE, syncGroupResult.getKey());

        KeyValue<Errors, GroupSummary> describeGroupResult = groupCoordinator.handleDescribeGroup(
            groupId
        );
        assertEquals(Errors.NONE, describeGroupResult.getKey());
        assertEquals(protocolType, describeGroupResult.getValue().protocolType());
        assertEquals("range", describeGroupResult.getValue().protocol());
        assertEquals(
            Lists.newArrayList(assignedMemberId),
            describeGroupResult.getValue().members().stream()
                .map(MemberSummary::memberId)
                .collect(Collectors.toList())
        );
    }

    @Test
    public void testDescribeGroupRebalancing() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, newProtocols()
        );
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupError);

        KeyValue<Errors, GroupSummary> describeGroupResult = groupCoordinator.handleDescribeGroup(
            groupId
        );
        assertEquals(Errors.NONE, describeGroupResult.getKey());
        assertEquals(protocolType, describeGroupResult.getValue().protocolType());
        assertEquals(GroupCoordinator.NoProtocol, describeGroupResult.getValue().protocol());
        assertEquals(GroupState.CompletingRebalance.toString(), describeGroupResult.getValue().state());
        assertEquals(
            Lists.newArrayList(joinGroupResult.getMemberId()),
            describeGroupResult.getValue().members().stream()
                .map(MemberSummary::memberId)
                .collect(Collectors.toList())
        );
        describeGroupResult.getValue()
            .members()
            .forEach(member -> {
                assertEquals(0, member.metadata().length);
            });
        describeGroupResult.getValue()
            .members()
            .forEach(member -> {
                assertEquals(0, member.assignment().length);
            });
    }

    @Test
    public void testDeleteNonEmptyGroup() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        joinGroup(groupId, memberId, protocolType, newProtocols());
        Map<String, Errors> deleteResult = groupCoordinator.handleDeleteGroups(Sets.newHashSet(groupId));
        assertEquals(1, deleteResult.size());
        assertTrue(deleteResult.containsKey(groupId));
        assertEquals(Errors.NON_EMPTY_GROUP, deleteResult.get(groupId));
    }

    @Test
    public void testDeleteGroupWithInvalidGroupId() throws Exception {
        String invalidGroupId = null;
        Map<String, Errors> deleteResult = groupCoordinator.handleDeleteGroups(
            Sets.newHashSet(invalidGroupId)
        );
        assertEquals(1, deleteResult.size());
        assertTrue(deleteResult.containsKey(invalidGroupId));
        assertEquals(Errors.INVALID_GROUP_ID, deleteResult.get(invalidGroupId));
    }

    @Test
    public void testDeleteGroupWithWrongCoordinator() throws Exception {
        Map<String, Errors> deleteResult = groupCoordinator.handleDeleteGroups(
            Sets.newHashSet(otherGroupId)
        );
        assertEquals(1, deleteResult.size());
        assertTrue(deleteResult.containsKey(otherGroupId));
        assertEquals(Errors.NOT_COORDINATOR, deleteResult.get(otherGroupId));
    }

    @Test
    public void testDeleteEmptyGroup() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;
        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, newProtocols()
        );

        Errors leaveGroupResult = groupCoordinator.handleLeaveGroup(groupId, joinGroupResult.getMemberId()).get();
        assertEquals(Errors.NONE, leaveGroupResult);

        Map<String, Errors> deleteResult = groupCoordinator.handleDeleteGroups(Sets.newHashSet(groupId));
        assertEquals(1, deleteResult.size());
        assertTrue(deleteResult.containsKey(groupId));
        assertEquals(Errors.NONE, deleteResult.get(groupId));
    }

    @Test
    public void testDeleteEmptyGroupWithStoredOffsets() throws Exception {
        String memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID;

        JoinGroupResult joinGroupResult = joinGroup(
            groupId, memberId, protocolType, newProtocols()
        );
        String assignedMemberId = joinGroupResult.getMemberId();
        Errors joinGroupError = joinGroupResult.getError();
        assertEquals(Errors.NONE, joinGroupError);

        Map<String, byte[]> assignments = new HashMap<>();
        assignments.put(assignedMemberId, new byte[0]);
        KeyValue<Errors, byte[]> syncGroupResult = groupCoordinator.handleSyncGroup(
            groupId, joinGroupResult.getGenerationId(),
            assignedMemberId, assignments
        ).get();
        assertEquals(Errors.NONE, syncGroupResult.getKey());

        TopicPartition tp = new TopicPartition("topic", 0);
        OffsetAndMetadata offset = OffsetAndMetadata.apply(0);
        Map<TopicPartition, Errors> commitOffsetResult = groupCoordinator.handleCommitOffsets(
            groupId, assignedMemberId, joinGroupResult.getGenerationId(),
            ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(tp, offset)
                .build()
        ).get();
        assertEquals(Errors.NONE, commitOffsetResult.get(tp));

        KeyValue<Errors, GroupSummary> describeGroupResult = groupCoordinator.handleDescribeGroup(groupId);
        assertEquals(GroupState.Stable.toString(), describeGroupResult.getValue().state());
        assertEquals(assignedMemberId, describeGroupResult.getValue().members().get(0).memberId());

        Errors leaveGroupResult = groupCoordinator.handleLeaveGroup(groupId, assignedMemberId).get();
        assertEquals(Errors.NONE, leaveGroupResult);

        Map<String, Errors> deleteResult = groupCoordinator.handleDeleteGroups(Sets.newHashSet(groupId));
        assertEquals(1, deleteResult.size());
        assertTrue(deleteResult.containsKey(groupId));
        assertEquals(Errors.NONE, deleteResult.get(groupId));

        describeGroupResult = groupCoordinator.handleDescribeGroup(groupId);

        assertEquals(GroupState.Dead.toString(), describeGroupResult.getValue().state());
    }

    @Test
    public void shouldResetRebalanceDelayWhenNewMemberJoinsGroupInInitialRebalance() throws Exception {
        int rebalanceTimeout = GroupInitialRebalanceDelay * 3;
        CompletableFuture<JoinGroupResult> firstMemberJoinFuture = groupCoordinator.handleJoinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
            "clientId", "clientHost",
            rebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols());
        timer.advanceClock(GroupInitialRebalanceDelay - 1);
        CompletableFuture<JoinGroupResult> secondMemberJoinFuture = groupCoordinator.handleJoinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
            "clientId", "clientHost",
            rebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols());
        timer.advanceClock(2);

        // advance past initial rebalance delay and make sure that tasks
        // haven't been completed
        timer.advanceClock(GroupInitialRebalanceDelay / 2 + 1);
        verifyDelayedTaskNotCompleted(firstMemberJoinFuture);
        verifyDelayedTaskNotCompleted(secondMemberJoinFuture);

        // advance clock beyond updated delay and make sure the
        // tasks have completed
        timer.advanceClock(GroupInitialRebalanceDelay / 2);
        assertEquals(Errors.NONE, firstMemberJoinFuture.get().getError());
        assertEquals(Errors.NONE, secondMemberJoinFuture.get().getError());
    }

    @Test
    public void shouldDelayRebalanceUptoRebalanceTimeout() throws Exception {
        int rebalanceTimeout = GroupInitialRebalanceDelay * 2;
        CompletableFuture<JoinGroupResult> firstMemberJoinFuture = groupCoordinator.handleJoinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
            "clientId", "clientHost",
            rebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols());
        CompletableFuture<JoinGroupResult> secondMemberJoinFuture = groupCoordinator.handleJoinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
            "clientId", "clientHost",
            rebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols());
        timer.advanceClock(GroupInitialRebalanceDelay + 1);
        CompletableFuture<JoinGroupResult> thirdMemberJoinFuture = groupCoordinator.handleJoinGroup(
            groupId, JoinGroupRequest.UNKNOWN_MEMBER_ID,
            "clientId", "clientHost",
            rebalanceTimeout, DefaultSessionTimeout,
            protocolType, newProtocols());
        timer.advanceClock(GroupInitialRebalanceDelay);

        verifyDelayedTaskNotCompleted(firstMemberJoinFuture);
        verifyDelayedTaskNotCompleted(secondMemberJoinFuture);
        verifyDelayedTaskNotCompleted(thirdMemberJoinFuture);

        // advance clock beyond rebalanceTimeout
        timer.advanceClock(1);
        assertEquals(Errors.NONE, firstMemberJoinFuture.get().getError());
        assertEquals(Errors.NONE, secondMemberJoinFuture.get().getError());
        assertEquals(Errors.NONE, thirdMemberJoinFuture.get().getError());
    }

    private void verifyDelayedTaskNotCompleted(CompletableFuture<JoinGroupResult> joinFuture) throws Exception {
        try {
            joinFuture.get(1, TimeUnit.MILLISECONDS);
            fail("Should have timed out as rebalance delay not expired");
        } catch (TimeoutException te) {
            // ok
        }
    }

    private JoinGroupResult joinGroup(
        String groupId,
        String memberId,
        String protocolType,
        Map<String, byte[]> protocols
    ) throws Exception {
        return joinGroup(
            groupId,
            memberId,
            protocolType,
            protocols,
            DefaultRebalanceTimeout,
            DefaultSessionTimeout
        );
    }

    private JoinGroupResult joinGroup(
        String groupId,
        String memberId,
        String protocolType,
        Map<String, byte[]> protocols,
        int sessionTimeout
    ) throws Exception {
        return joinGroup(
            groupId,
            memberId,
            protocolType,
            protocols,
            DefaultRebalanceTimeout,
            sessionTimeout
        );
    }

    private JoinGroupResult joinGroup(
        String groupId,
        String memberId,
        String protocolType,
        Map<String, byte[]> protocols,
        int rebalanceTimeout,
        int sessionTimeout
    ) throws Exception {
        CompletableFuture<JoinGroupResult> responseFuture = groupCoordinator.handleJoinGroup(
            groupId, memberId,
            "clientId", "clientHost",
            rebalanceTimeout,
            sessionTimeout,
            protocolType,
            protocols
        );
        timer.advanceClock(GroupInitialRebalanceDelay + 1);
        return responseFuture.get();
    }

}
