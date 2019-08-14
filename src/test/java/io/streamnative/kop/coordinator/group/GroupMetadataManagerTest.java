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
package io.streamnative.kop.coordinator.group;

import static io.streamnative.kop.coordinator.group.GroupMetadataConstants.groupMetadataKey;
import static io.streamnative.kop.coordinator.group.GroupMetadataConstants.groupMetadataValue;
import static io.streamnative.kop.coordinator.group.GroupMetadataConstants.offsetCommitKey;
import static io.streamnative.kop.coordinator.group.GroupMetadataConstants.offsetCommitValue;
import static io.streamnative.kop.coordinator.group.GroupState.Empty;
import static io.streamnative.kop.coordinator.group.GroupState.PreparingRebalance;
import static io.streamnative.kop.coordinator.group.GroupState.Stable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.streamnative.kop.MockKafkaServiceBaseTest;
import io.streamnative.kop.coordinator.group.GroupMetadata.CommitRecordMetadataAndOffset;
import io.streamnative.kop.coordinator.group.GroupMetadataManager.BaseKey;
import io.streamnative.kop.coordinator.group.GroupMetadataManager.GroupMetadataKey;
import io.streamnative.kop.coordinator.group.GroupMetadataManager.GroupTopicPartition;
import io.streamnative.kop.coordinator.group.GroupMetadataManager.OffsetKey;
import io.streamnative.kop.offset.OffsetAndMetadata;
import io.streamnative.kop.utils.MockTime;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link GroupMetadataManager}.
 */
@Slf4j
public class GroupMetadataManagerTest extends MockKafkaServiceBaseTest {

    private static final String groupId = "foo";
    private static final int groupPartitionId = 0;
    private static final TopicPartition groupTopicPartition =
        new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId);
    private static final String protocolType = "protocolType";
    private static final int rebalanceTimeout = 60000;
    private static final int sessionTimeout = 10000;

    MockTime time = null;
    GroupMetadataManager groupMetadataManager = null;
    Producer<ByteBuffer> producer = null;
    Reader<ByteBuffer> consumer = null;
    OffsetConfig offsetConfig = OffsetConfig.builder().build();
    OrderedScheduler scheduler;

    @Override
    protected void resetConfig() {
        super.resetConfig();
        // since this test mock all Group Coordinator, we disable the one in Kafka broker.
        this.conf.setEnableGroupCoordinator(false);
    }

    @Before
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test-scheduler")
            .numThreads(1)
            .build();

        admin.clusters().createCluster("test",
            new ClusterData("http://127.0.0.1:" + brokerWebservicePort));

        admin.tenants().createTenant("public",
            new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
        admin.namespaces().setRetention("public/default",
            new RetentionPolicies(20, 100));

        time = new MockTime();
        groupMetadataManager = new GroupMetadataManager(
            1,
            offsetConfig,
            producer,
            consumer,
            scheduler,
            time
        );
    }

    @After
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    private List<SimpleRecord> createCommittedOffsetRecords(Map<TopicPartition, Long> committedOffsets,
                                                            String groupId) {
        return committedOffsets.entrySet().stream().map(e -> {
            OffsetAndMetadata offsetAndMetadata = OffsetAndMetadata.apply(e.getValue());
            byte[] offsetCommitKey = offsetCommitKey(groupId, e.getKey());
            byte[] offsetCommitValue = offsetCommitValue(offsetAndMetadata);
            return new SimpleRecord(offsetCommitKey, offsetCommitValue);
        }).collect(Collectors.toList());
    }

    private SimpleRecord buildStableGroupRecordWithMember(int generation,
                                                          String protocolType,
                                                          String protocol,
                                                          String memberId) {
        return buildStableGroupRecordWithMember(
            generation,
            protocolType,
            protocol,
            memberId,
            0
        );
    }

    private SimpleRecord buildStableGroupRecordWithMember(int generation,
                                                          String protocolType,
                                                          String protocol,
                                                          String memberId,
                                                          int assignmentSize) {
        Map<String, byte[]> memberProtocols = new HashMap<>();
        memberProtocols.put(protocol, new byte[0]);
        MemberMetadata member = new MemberMetadata(
            memberId,
            groupId,
            "clientId",
            "clientHost",
            30000,
            10000,
            protocolType,
            memberProtocols);

        GroupMetadata group = GroupMetadata.loadGroup(
            groupId,
            Stable,
            generation,
            protocolType,
            protocol,
            memberId,
            Lists.newArrayList(member)
        );
        byte[] groupMetadataKey = groupMetadataKey(groupId);
        Map<String, byte[]> assignments = new HashMap<>();
        assignments.put(memberId, new byte[0]);
        byte[] groupMetadataValue = groupMetadataValue(group, assignments);
        return new SimpleRecord(groupMetadataKey, groupMetadataValue);
    }

    private SimpleRecord buildEmptyGroupRecord(int generation,
                                               String protocolType) {
        GroupMetadata group = GroupMetadata.loadGroup(
            groupId,
            Empty,
            generation,
            protocolType,
            null,
            null,
            Collections.emptyList()
        );
        byte[] groupMetadataKey = groupMetadataKey(groupId);
        byte[] groupMetadataValue = groupMetadataValue(
            group, Collections.emptyMap());
        return new SimpleRecord(groupMetadataKey, groupMetadataValue);
    }

    private ByteBuffer newMemoryRecordsBuffer(List<SimpleRecord> records) {
        return newMemoryRecordsBuffer(
            records,
            -1L,
            (short) -1,
            false
        );
    }

    private ByteBuffer newMemoryRecordsBuffer(List<SimpleRecord> records,
                                              long producerId,
                                              short producerEpoch,
                                              boolean isTxnOffsetCommit) {
        TimestampType timestampType = TimestampType.CREATE_TIME;
        long timestamp = time.milliseconds();

        ByteBuffer buffer = ByteBuffer.allocate(
            AbstractRecords.estimateSizeInBytes(
                RecordBatch.CURRENT_MAGIC_VALUE, offsetConfig.offsetsTopicCompressionType(), records
            )
        );

        MemoryRecordsBuilder builder = MemoryRecords.builder(
            buffer, RecordBatch.CURRENT_MAGIC_VALUE, offsetConfig.offsetsTopicCompressionType(),
            timestampType, 0L, timestamp,
            producerId,
            producerEpoch,
            0,
            isTxnOffsetCommit,
            RecordBatch.NO_PARTITION_LEADER_EPOCH
        );
        records.forEach(builder::append);
        return builder.build().buffer();
    }

    private int appendConsumerOffsetCommit(ByteBuffer buffer,
                                           long baseOffset,
                                           Map<TopicPartition, Long> offsets) {
        MemoryRecordsBuilder builder =
            MemoryRecords.builder(buffer, CompressionType.NONE, TimestampType.LOG_APPEND_TIME, baseOffset);
        List<SimpleRecord> commitRecords = createCommittedOffsetRecords(offsets, groupId);
        commitRecords.forEach(builder::append);
        builder.build();
        return offsets.size();
    }

    private int appendTransactionalOffsetCommits(ByteBuffer buffer,
                                                 long producerId,
                                                 short producerEpoch,
                                                 long baseOffset,
                                                 Map<TopicPartition, Long> offsets) {
        MemoryRecordsBuilder builder =
            MemoryRecords.builder(buffer, CompressionType.NONE, baseOffset, producerId, producerEpoch, 0, true);
        List<SimpleRecord> commitRecords = createCommittedOffsetRecords(offsets, groupId);
        commitRecords.forEach(builder::append);
        builder.build();
        return offsets.size();
    }

    private int completeTransactionalOffsetCommit(ByteBuffer buffer,
                                                  long producerId,
                                                  short producerEpoch,
                                                  long baseOffset,
                                                  boolean isCommit) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(
            buffer, RecordBatch.MAGIC_VALUE_V2, CompressionType.NONE,
            TimestampType.LOG_APPEND_TIME, baseOffset, time.milliseconds(),
            producerId, producerEpoch, 0, true, true,
            RecordBatch.NO_PARTITION_LEADER_EPOCH);
        ControlRecordType controlRecordType;
        if (isCommit) {
            controlRecordType = ControlRecordType.COMMIT;
        } else {
            controlRecordType = ControlRecordType.ABORT;
        }
        builder.appendEndTxnMarker(time.milliseconds(), new EndTransactionMarker(controlRecordType, 0));
        builder.build();
        return 1;
    }

    @Test
    public void testLoadOffsetsWithoutGroup() throws Exception {
        TopicPartition groupMetadataTopicPartition = groupTopicPartition;

        Map<TopicPartition, Long> committedOffsets = new HashMap<>();
        committedOffsets.put(
            new TopicPartition("foo", 0), 23L);
        committedOffsets.put(
            new TopicPartition("foo", 1), 455L);
        committedOffsets.put(
            new TopicPartition("bar", 0), 8992L);

        List<SimpleRecord> offsetCommitRecords = createCommittedOffsetRecords(
            committedOffsets,
            groupId
        );
        ByteBuffer buffer = newMemoryRecordsBuffer(offsetCommitRecords);
        byte[] key = groupMetadataKey(groupId);

        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(time.milliseconds())
                    .send();

                CompletableFuture<GroupMetadata> onLoadedFuture = new CompletableFuture<>();
                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> onLoadedFuture.complete(groupMetadata)
                ).get();
                GroupMetadata group = onLoadedFuture.get();
                GroupMetadata groupInCache = groupMetadataManager.getGroup(groupId).orElseGet(() -> {
                    fail("Group was not loaded into the cache");
                    return null;
                });
                assertSame(group, groupInCache);
                assertEquals(groupId, group.groupId());
                assertEquals(Empty, group.currentState());
                assertEquals(committedOffsets.size(), group.allOffsets().size());
                committedOffsets.forEach((tp, offset) ->
                    assertEquals(Optional.of(offset), group.offset(tp).map(OffsetAndMetadata::offset)));
            });

    }

    @Test
    public void testLoadEmptyGroupWithOffsets() throws Exception {
        TopicPartition groupMetadataTopicPartition = groupTopicPartition;
        int generation = 15;
        String protocolType = "consumer";

        Map<TopicPartition, Long> committedOffsets = new HashMap<>();
        committedOffsets.put(
            new TopicPartition("foo", 0), 23L);
        committedOffsets.put(
            new TopicPartition("foo", 1), 455L);
        committedOffsets.put(
            new TopicPartition("bar", 0), 8992L);

        List<SimpleRecord> offsetCommitRecords = createCommittedOffsetRecords(
            committedOffsets,
            groupId
        );
        offsetCommitRecords.add(
            buildEmptyGroupRecord(generation, protocolType));

        ByteBuffer buffer = newMemoryRecordsBuffer(offsetCommitRecords);
        byte[] key = groupMetadataKey(groupId);

        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(time.milliseconds())
                    .send();

                CompletableFuture<GroupMetadata> onLoadedFuture = new CompletableFuture<>();
                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> onLoadedFuture.complete(groupMetadata)
                ).get();
                GroupMetadata group = onLoadedFuture.get();
                GroupMetadata groupInCache = groupMetadataManager.getGroup(groupId).orElseGet(() -> {
                    fail("Group was not loaded into the cache");
                    return null;
                });
                assertSame(group, groupInCache);

                assertEquals(groupId, group.groupId());
                assertEquals(Empty, group.currentState());
                assertEquals(generation, group.generationId());
                assertEquals(Optional.of(protocolType), group.protocolType());
                assertEquals(committedOffsets.size(), group.allOffsets().size());
                assertNull(group.leaderOrNull());
                assertNull(group.protocolOrNull());
                committedOffsets.forEach((tp, offset) ->
                    assertEquals(Optional.of(offset), group.offset(tp).map(OffsetAndMetadata::offset)));
            });
    }

    @Test
    public void testLoadTransactionalOffsetsWithoutGroup() throws Exception {
        TopicPartition groupMetadataTopicPartition = groupTopicPartition;
        long producerId = 1000L;
        short producerEpoch = 2;

        Map<TopicPartition, Long> committedOffsets = new HashMap<>();
        committedOffsets.put(
            new TopicPartition("foo", 0), 23L);
        committedOffsets.put(
            new TopicPartition("foo", 1), 455L);
        committedOffsets.put(
            new TopicPartition("bar", 0), 8992L);

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int nextOffset = 0;
        nextOffset += appendTransactionalOffsetCommits(
            buffer, producerId, producerEpoch, nextOffset, committedOffsets
        );
        completeTransactionalOffsetCommit(
            buffer, producerId, producerEpoch, nextOffset, true
        );
        buffer.flip();

        byte[] key = groupMetadataKey(groupId);
        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(time.milliseconds())
                    .send();

                CompletableFuture<GroupMetadata> onLoadedFuture = new CompletableFuture<>();
                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> onLoadedFuture.complete(groupMetadata)
                ).get();
                GroupMetadata group = onLoadedFuture.get();
                GroupMetadata groupInCache = groupMetadataManager.getGroup(groupId).orElseGet(() -> {
                    fail("Group was not loaded into the cache");
                    return null;
                });
                assertSame(group, groupInCache);

                assertEquals(groupId, group.groupId());
                assertEquals(Empty, group.currentState());
                assertEquals(committedOffsets.size(), group.allOffsets().size());
                committedOffsets.forEach((tp, offset) ->
                    assertEquals(Optional.of(offset), group.offset(tp).map(OffsetAndMetadata::offset)));
            });
    }

    @Test
    public void testDoNotLoadAbortedTransactionalOffsetCommits() throws Exception {
        TopicPartition groupMetadataTopicPartition = groupTopicPartition;
        long producerId = 1000L;
        short producerEpoch = 2;

        Map<TopicPartition, Long> abortedOffsets = new HashMap<>();
        abortedOffsets.put(
            new TopicPartition("foo", 0), 23L);
        abortedOffsets.put(
            new TopicPartition("foo", 1), 455L);
        abortedOffsets.put(
            new TopicPartition("bar", 0), 8992L);

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int nextOffset = 0;
        nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, abortedOffsets);
        completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, false);
        buffer.flip();

        byte[] key = groupMetadataKey(groupId);

        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(time.milliseconds())
                    .send();

                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> {}
                ).get();
                Optional<GroupMetadata> groupInCache = groupMetadataManager.getGroup(groupId);
                assertFalse(groupInCache.isPresent());
            });
    }

    @Test
    public void testGroupLoadedWithPendingCommits() throws Exception {
        TopicPartition groupMetadataTopicPartition = groupTopicPartition;
        long producerId = 1000L;
        short producerEpoch = 2;

        Map<TopicPartition, Long> pendingOffsets = new HashMap<>();
        pendingOffsets.put(
            new TopicPartition("foo", 0), 23L);
        pendingOffsets.put(
            new TopicPartition("foo", 1), 455L);
        pendingOffsets.put(
            new TopicPartition("bar", 0), 8992L);

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int nextOffset = 0;
        appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, pendingOffsets);
        buffer.flip();

        byte[] key = groupMetadataKey(groupId);

        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(time.milliseconds())
                    .send();

                CompletableFuture<GroupMetadata> onLoadedFuture = new CompletableFuture<>();
                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> onLoadedFuture.complete(groupMetadata)
                ).get();
                GroupMetadata groupInCache = groupMetadataManager.getGroup(groupId).orElseGet(() -> {
                    fail("Group was not loaded into the cache");
                    return null;
                });
                GroupMetadata group = onLoadedFuture.get();
                assertSame(group, groupInCache);
                assertEquals(groupId, group.groupId());
                assertEquals(Empty, group.currentState());
                // Ensure that no offsets are materialized, but that we have offsets pending.
                assertEquals(0, group.allOffsets().size());
                assertTrue(group.hasOffsets());
                assertTrue(group.hasPendingOffsetCommitsFromProducer(producerId));
            });
    }

    @Test
    public void testLoadWithCommitedAndAbortedTransactionOffsetCommits() throws Exception {
        TopicPartition groupMetadataTopicPartition = groupTopicPartition;
        long producerId = 1000L;
        short producerEpoch = 2;

        Map<TopicPartition, Long> committedOffsets = new HashMap<>();
        committedOffsets.put(
            new TopicPartition("foo", 0), 23L);
        committedOffsets.put(
            new TopicPartition("foo", 1), 455L);
        committedOffsets.put(
            new TopicPartition("bar", 0), 8992L);

        Map<TopicPartition, Long> abortedOffsets = new HashMap<>();
        abortedOffsets.put(
            new TopicPartition("foo", 2), 231L);
        abortedOffsets.put(
            new TopicPartition("foo", 3), 4551L);
        abortedOffsets.put(
            new TopicPartition("bar", 1), 89921L);

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int nextOffset = 0;
        nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, abortedOffsets);
        nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, false);
        nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, committedOffsets);
        completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, true);
        buffer.flip();

        byte[] key = groupMetadataKey(groupId);

        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(time.milliseconds())
                    .send();

                CompletableFuture<GroupMetadata> onLoadedFuture = new CompletableFuture<>();
                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> onLoadedFuture.complete(groupMetadata)
                ).get();
                GroupMetadata groupInCache = groupMetadataManager.getGroup(groupId).orElseGet(() -> {
                    fail("Group was not loaded into the cache");
                    return null;
                });
                GroupMetadata group = onLoadedFuture.get();
                assertSame(group, groupInCache);
                assertEquals(groupId, group.groupId());
                assertEquals(Empty, group.currentState());
                // Ensure that only the committed offsets are materialized, and that there are no pending
                // commits for the producer. This allows us to be certain that the aborted offset commits
                //
                // are truly discarded.
                assertEquals(committedOffsets.size(), group.allOffsets().size());
                committedOffsets.forEach((tp, offset) ->
                    assertEquals(Optional.of(offset), group.offset(tp).map(OffsetAndMetadata::offset)));
                assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId));
            });
    }

    @Test
    public void testLoadWithCommitedAndAbortedAndPendingTransactionOffsetCommits() throws Exception {
        TopicPartition groupMetadataTopicPartition = groupTopicPartition;
        long producerId = 1000L;
        short producerEpoch = 2;

        Map<TopicPartition, Long> committedOffsets = new HashMap<>();
        committedOffsets.put(
            new TopicPartition("foo", 0), 23L);
        committedOffsets.put(
            new TopicPartition("foo", 1), 455L);
        committedOffsets.put(
            new TopicPartition("bar", 0), 8992L);

        Map<TopicPartition, Long> abortedOffsets = new HashMap<>();
        abortedOffsets.put(
            new TopicPartition("foo", 2), 231L);
        abortedOffsets.put(
            new TopicPartition("foo", 3), 4551L);
        abortedOffsets.put(
            new TopicPartition("bar", 1), 89921L);

        Map<TopicPartition, Long> pendingOffsets = new HashMap<>();
        pendingOffsets.put(
            new TopicPartition("foo", 3), 2312L);
        pendingOffsets.put(
            new TopicPartition("foo", 4), 45512L);
        pendingOffsets.put(
            new TopicPartition("bar", 2), 899212L);

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int nextOffset = 0;
        nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, committedOffsets);
        nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, true);
        nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, abortedOffsets);
        nextOffset += completeTransactionalOffsetCommit(buffer, producerId, producerEpoch, nextOffset, false);
        nextOffset += appendTransactionalOffsetCommits(buffer, producerId, producerEpoch, nextOffset, pendingOffsets);
        buffer.flip();

        byte[] key = groupMetadataKey(groupId);

        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(time.milliseconds())
                    .send();

                CompletableFuture<GroupMetadata> onLoadedFuture = new CompletableFuture<>();
                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> onLoadedFuture.complete(groupMetadata)
                ).get();
                GroupMetadata groupInCache = groupMetadataManager.getGroup(groupId).orElseGet(() -> {
                    fail("Group was not loaded into the cache");
                    return null;
                });
                GroupMetadata group = onLoadedFuture.get();
                assertSame(group, groupInCache);
                assertEquals(groupId, group.groupId());
                assertEquals(Empty, group.currentState());

                // Ensure that only the committed offsets are materialized, and that there are no pending commits
                // for the producer. This allows us to be certain that the aborted offset commits are truly discarded.
                assertEquals(committedOffsets.size(), group.allOffsets().size());
                committedOffsets.forEach((tp, offset) ->
                    assertEquals(Optional.of(offset), group.offset(tp).map(OffsetAndMetadata::offset)));

                // We should have pending commits.
                assertTrue(group.hasPendingOffsetCommitsFromProducer(producerId));

                 // The loaded pending commits should materialize after a commit marker comes in.
                groupMetadataManager.handleTxnCompletion(
                    producerId,
                    Sets.newHashSet(groupMetadataTopicPartition.partition()),
                    true);
                assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId));
                pendingOffsets.forEach((tp, offset) ->
                    assertEquals(Optional.of(offset), group.offset(tp).map(OffsetAndMetadata::offset)));
            });
    }

    @Test
    public void testLoadTransactionalOffsetCommitsFromMultipleProducers() throws Exception {
        TopicPartition groupMetadataTopicPartition = groupTopicPartition;
        long firstProducerId = 1000L;
        short firstProducerEpoch = 2;
        long secondProducerId = 1001L;
        short secondProducerEpoch = 3;

        Map<TopicPartition, Long> committedOffsetsFirstProducer = new HashMap<>();
        committedOffsetsFirstProducer.put(
            new TopicPartition("foo", 0), 23L);
        committedOffsetsFirstProducer.put(
            new TopicPartition("foo", 1), 455L);
        committedOffsetsFirstProducer.put(
            new TopicPartition("bar", 0), 8992L);

        Map<TopicPartition, Long> committedOffsetsSecondProducer = new HashMap<>();
        committedOffsetsSecondProducer.put(
            new TopicPartition("foo", 2), 231L);
        committedOffsetsSecondProducer.put(
            new TopicPartition("foo", 3), 4551L);
        committedOffsetsSecondProducer.put(
            new TopicPartition("bar", 1), 89921L);

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int nextOffset = 0;
        int firstProduceRecordOffset = nextOffset;
        nextOffset += appendTransactionalOffsetCommits(
            buffer, firstProducerId, firstProducerEpoch, nextOffset, committedOffsetsFirstProducer
        );
        nextOffset += completeTransactionalOffsetCommit(
            buffer, firstProducerId, firstProducerEpoch, nextOffset, true
        );
        int secondProduceRecordOffset = nextOffset;
        nextOffset += appendTransactionalOffsetCommits(
            buffer, secondProducerId, secondProducerEpoch, nextOffset, committedOffsetsSecondProducer
        );
        nextOffset += completeTransactionalOffsetCommit(
            buffer, secondProducerId, secondProducerEpoch, nextOffset, true
        );
        buffer.flip();

        byte[] key = groupMetadataKey(groupId);
        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(time.milliseconds())
                    .send();

                CompletableFuture<GroupMetadata> onLoadedFuture = new CompletableFuture<>();
                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> onLoadedFuture.complete(groupMetadata)
                ).get();
                GroupMetadata group = onLoadedFuture.get();
                GroupMetadata groupInCache = groupMetadataManager.getGroup(groupId).orElseGet(() -> {
                    fail("Group was not loaded into the cache");
                    return null;
                });
                assertSame(group, groupInCache);

                assertEquals(groupId, group.groupId());
                assertEquals(Empty, group.currentState());

                // Ensure that only the committed offsets are materialized, and that there are no pending commits
                // for the producer. This allows us to be certain that the aborted offset commits are truly discarded.
                assertEquals(committedOffsetsFirstProducer.size() + committedOffsetsSecondProducer.size(),
                    group.allOffsets().size());
                committedOffsetsFirstProducer.forEach((tp, offset) -> {
                    assertEquals(Optional.of(offset), group.offset(tp).map(OffsetAndMetadata::offset));
                    assertEquals(
                        Optional.of((long) firstProduceRecordOffset),
                        group.offsetWithRecordMetadata(tp).flatMap(CommitRecordMetadataAndOffset::appendedBatchOffset));
                });
                committedOffsetsSecondProducer.forEach((tp, offset) -> {
                    assertEquals(Optional.of(offset), group.offset(tp).map(OffsetAndMetadata::offset));
                    assertEquals(
                        Optional.of((long) secondProduceRecordOffset),
                        group.offsetWithRecordMetadata(tp).flatMap(CommitRecordMetadataAndOffset::appendedBatchOffset));
                });
            });
    }

    @Test
    public void testGroupLoadWithConsumerAndTransactionalOffsetCommitsTransactionWins() throws Exception {
        TopicPartition groupMetadataTopicPartition = groupTopicPartition;
        long producerId = 1000L;
        short producerEpoch = 2;

        Map<TopicPartition, Long> transactionalOffsetCommits = new HashMap<>();
        transactionalOffsetCommits.put(
            new TopicPartition("foo", 0), 23L);

        Map<TopicPartition, Long> consumerOffsetCommits = new HashMap<>();
        consumerOffsetCommits.put(
            new TopicPartition("foo", 0), 24L);

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int nextOffset = 0;
        nextOffset += appendConsumerOffsetCommit(
            buffer, nextOffset, consumerOffsetCommits
        );
        nextOffset += appendTransactionalOffsetCommits(
            buffer, producerId, producerEpoch, nextOffset, transactionalOffsetCommits
        );
        nextOffset += completeTransactionalOffsetCommit(
            buffer, producerId, producerEpoch, nextOffset, true
        );
        buffer.flip();

        byte[] key = groupMetadataKey(groupId);
        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(time.milliseconds())
                    .send();

                CompletableFuture<GroupMetadata> onLoadedFuture = new CompletableFuture<>();
                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> onLoadedFuture.complete(groupMetadata)
                ).get();
                GroupMetadata group = onLoadedFuture.get();
                GroupMetadata groupInCache = groupMetadataManager.getGroup(groupId).orElseGet(() -> {
                    fail("Group was not loaded into the cache");
                    return null;
                });
                assertSame(group, groupInCache);

                assertEquals(groupId, group.groupId());
                assertEquals(Empty, group.currentState());

                // The group should be loaded with pending offsets.
                assertEquals(1, group.allOffsets().size());
                assertTrue(group.hasOffsets());
                assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId));
                assertEquals(consumerOffsetCommits.size(), group.allOffsets().size());
                transactionalOffsetCommits.forEach((tp, offset) -> {
                    assertEquals(Optional.of(offset), group.offset(tp).map(OffsetAndMetadata::offset));
                });
            });
    }

    @Test
    public void testGroupNotExits() {
        // group is not owned
        assertFalse(groupMetadataManager.groupNotExists(groupId));

        groupMetadataManager.addPartitionOwnership(groupPartitionId);
        // group is owned but does not exist yet
        assertTrue(groupMetadataManager.groupNotExists(groupId));

        GroupMetadata group = new GroupMetadata(groupId, Empty);
        groupMetadataManager.addGroup(group);

        // group is owned but not Dead
        assertFalse(groupMetadataManager.groupNotExists(groupId));

        group.transitionTo(GroupState.Dead);
        // group is owned and Dead
        assertTrue(groupMetadataManager.groupNotExists(groupId));
    }

    @Test
    public void testLoadOffsetsWithTombstones() throws Exception {
        TopicPartition groupMetadataTopicPartition = groupTopicPartition;
        TopicPartition tombstonePartition = new TopicPartition("foo", 1);

        Map<TopicPartition, Long> committedOffsets = new HashMap<>();
        committedOffsets.put(
            new TopicPartition("foo", 0), 23L);
        committedOffsets.put(
            tombstonePartition, 455L);
        committedOffsets.put(
            new TopicPartition("bar", 0), 8992L);

        List<SimpleRecord> offsetCommitRecords = createCommittedOffsetRecords(committedOffsets, groupId);
        SimpleRecord tombstone = new SimpleRecord(
            offsetCommitKey(groupId, tombstonePartition),
            null
        );
        offsetCommitRecords.add(tombstone);

        ByteBuffer buffer = newMemoryRecordsBuffer(offsetCommitRecords);

        byte[] key = groupMetadataKey(groupId);
        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(time.milliseconds())
                    .send();

                CompletableFuture<GroupMetadata> onLoadedFuture = new CompletableFuture<>();
                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> onLoadedFuture.complete(groupMetadata)
                ).get();
                GroupMetadata group = onLoadedFuture.get();
                GroupMetadata groupInCache = groupMetadataManager.getGroup(groupId).orElseGet(() -> {
                    fail("Group was not loaded into the cache");
                    return null;
                });
                assertSame(group, groupInCache);

                assertEquals(groupId, group.groupId());
                assertEquals(Empty, group.currentState());

                // The group should be loaded with pending offsets.
                assertEquals(committedOffsets.size() - 1, group.allOffsets().size());
                committedOffsets.forEach((tp, offset) -> {
                    if (tp == tombstonePartition) {
                        assertEquals(Optional.empty(), group.offset(tp));
                    } else {
                        assertEquals(Optional.of(offset), group.offset(tp).map(OffsetAndMetadata::offset));
                    }
                });
            });
    }

    @Test
    public void testLoadOffsetsAndGroup() throws Exception {
        TopicPartition groupMetadataTopicPartition = groupTopicPartition;
        int generation = 935;
        String protocolType = "consumer";
        String protocol = "range";

        Map<TopicPartition, Long> committedOffsets = new HashMap<>();
        committedOffsets.put(
            new TopicPartition("foo", 0), 23L);
        committedOffsets.put(
            new TopicPartition("foo", 1), 455L);
        committedOffsets.put(
            new TopicPartition("bar", 0), 8992L);

        List<SimpleRecord> offsetCommitRecords = createCommittedOffsetRecords(committedOffsets, groupId);
        String memberId = "98098230493";
        SimpleRecord groupMetadataRecord = buildStableGroupRecordWithMember(
            generation,
            protocolType,
            protocol,
            memberId
        );
        offsetCommitRecords.add(groupMetadataRecord);

        ByteBuffer buffer = newMemoryRecordsBuffer(offsetCommitRecords);

        byte[] key = groupMetadataKey(groupId);
        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(time.milliseconds())
                    .send();

                CompletableFuture<GroupMetadata> onLoadedFuture = new CompletableFuture<>();
                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> onLoadedFuture.complete(groupMetadata)
                ).get();
                GroupMetadata group = onLoadedFuture.get();
                GroupMetadata groupInCache = groupMetadataManager.getGroup(groupId).orElseGet(() -> {
                    fail("Group was not loaded into the cache");
                    return null;
                });
                assertSame(group, groupInCache);

                assertEquals(groupId, group.groupId());
                assertEquals(Stable, group.currentState());
                assertEquals(memberId, group.leaderOrNull());
                assertEquals(generation, group.generationId());
                assertEquals(Optional.of(protocolType), group.protocolType());
                assertEquals(
                    Lists.newArrayList(memberId),
                    group.allMembers().stream().collect(Collectors.toList()));
                assertEquals(
                    committedOffsets.size(),
                    group.allOffsets().size()
                );
                committedOffsets.forEach((tp, offset) -> {
                    assertEquals(Optional.of(offset), group.offset(tp).map(OffsetAndMetadata::offset));
                });
            });
    }

    @Test
    public void testLoadGroupWithTombstone() throws Exception {
        TopicPartition groupMetadataTopicPartition = groupTopicPartition;
        int generation = 935;
        String memberId = "98098230493";
        String protocolType = "consumer";
        String protocol = "range";

        SimpleRecord groupMetadataRecord = buildStableGroupRecordWithMember(
            generation,
            protocolType,
            protocol,
            memberId
        );
        SimpleRecord groupMetadataTombstone = new SimpleRecord(
            groupMetadataKey(groupId),
            null
        );
        ByteBuffer buffer = newMemoryRecordsBuffer(Lists.newArrayList(
            groupMetadataRecord,
            groupMetadataTombstone
        ));

        byte[] key = groupMetadataKey(groupId);
        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(time.milliseconds())
                    .send();

                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> {}
                ).get();
                assertFalse(groupMetadataManager.getGroup(groupId).isPresent());
            });
    }

    @Test
    public void testOffsetWriteAfterGroupRemoved() throws Exception {
        // this test case checks the following scenario:
        // 1. the group exists at some point in time, but is later removed (because all members left)
        // 2. a "simple" consumer (i.e. not a consumer group) then uses the same groupId to commit some offsets

        TopicPartition groupMetadataTopicPartition = groupTopicPartition;
        int generation = 293;
        String memberId = "98098230493";
        String protocolType = "consumer";
        String protocol = "range";

        Map<TopicPartition, Long> committedOffsets = new HashMap<>();
        committedOffsets.put(
            new TopicPartition("foo", 0), 23L);
        committedOffsets.put(
            new TopicPartition("foo", 1), 455L);
        committedOffsets.put(
            new TopicPartition("bar", 0), 8992L);

        List<SimpleRecord> offsetCommitRecords = createCommittedOffsetRecords(committedOffsets, groupId);
        SimpleRecord groupMetadataRecord = buildStableGroupRecordWithMember(
            generation,
            protocolType,
            protocol,
            memberId
        );
        SimpleRecord groupMetadataTombstone = new SimpleRecord(
            groupMetadataKey(groupId),
            null
        );

        List<SimpleRecord> newOffsetCommitRecords = new ArrayList<>();
        newOffsetCommitRecords.add(groupMetadataRecord);
        newOffsetCommitRecords.add(groupMetadataTombstone);
        newOffsetCommitRecords.addAll(offsetCommitRecords);

        ByteBuffer buffer = newMemoryRecordsBuffer(newOffsetCommitRecords);

        byte[] key = groupMetadataKey(groupId);
        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(time.milliseconds())
                    .send();

                CompletableFuture<GroupMetadata> onLoadedFuture = new CompletableFuture<>();
                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> onLoadedFuture.complete(groupMetadata)
                ).get();
                GroupMetadata group = onLoadedFuture.get();
                GroupMetadata groupInCache = groupMetadataManager.getGroup(groupId).orElseGet(() -> {
                    fail("Group was not loaded into the cache");
                    return null;
                });
                assertSame(group, groupInCache);

                assertEquals(groupId, group.groupId());
                assertEquals(Empty, group.currentState());
                assertEquals(committedOffsets.size(), group.allOffsets().size());
                committedOffsets.forEach((tp, offset) -> {
                    assertEquals(Optional.of(offset), group.offset(tp).map(OffsetAndMetadata::offset));
                });
            });
    }

    @Test
    public void testLoadGroupAndOffsetsFromDifferentSegments() throws Exception {
        TopicPartition groupMetadataTopicPartition = groupTopicPartition;
        int generation = 293;
        String protocolType = "consumer";
        String protocol = "range";
        TopicPartition tp0 = new TopicPartition("foo", 0);
        TopicPartition tp1 = new TopicPartition("foo", 1);
        TopicPartition tp2 = new TopicPartition("bar", 0);
        TopicPartition tp3 = new TopicPartition("xxx", 0);

        String segment1MemberId = "a";
        Map<TopicPartition, Long> segment1Offsets = new HashMap<>();
        segment1Offsets.put(tp0, 23L);
        segment1Offsets.put(tp1, 455L);
        segment1Offsets.put(tp3, 42L);
        List<SimpleRecord> segment1Records = createCommittedOffsetRecords(segment1Offsets, groupId);
        SimpleRecord segment1Group = buildStableGroupRecordWithMember(
            generation,
            protocolType,
            protocol,
            segment1MemberId
        );
        segment1Records.add(segment1Group);
        ByteBuffer segment1Buffer = newMemoryRecordsBuffer(segment1Records);

        String segment2MemberId = "a";
        Map<TopicPartition, Long> segment2Offsets = new HashMap<>();
        segment2Offsets.put(tp0, 33L);
        segment2Offsets.put(tp2, 8992L);
        segment2Offsets.put(tp3, 10L);
        List<SimpleRecord> segment2Records = createCommittedOffsetRecords(segment2Offsets, groupId);
        SimpleRecord segment2Group = buildStableGroupRecordWithMember(
            generation,
            protocolType,
            protocol,
            segment2MemberId
        );
        segment2Records.add(segment2Group);
        ByteBuffer segment2Buffer = newMemoryRecordsBuffer(segment2Records);

        byte[] key = groupMetadataKey(groupId);
        runGroupMetadataManagerProducerTester("test-load-offsets-without-group",
            (groupMetadataManager, producer) -> {
                producer.newMessage()
                    .keyBytes(key)
                    .value(segment1Buffer)
                    .eventTime(time.milliseconds())
                    .send();

                producer.newMessage()
                    .keyBytes(key)
                    .value(segment2Buffer)
                    .eventTime(time.milliseconds())
                    .send();

                CompletableFuture<GroupMetadata> onLoadedFuture = new CompletableFuture<>();
                groupMetadataManager.scheduleLoadGroupAndOffsets(
                    groupMetadataTopicPartition.partition(),
                    groupMetadata -> onLoadedFuture.complete(groupMetadata)
                ).get();
                GroupMetadata group = onLoadedFuture.get();
                GroupMetadata groupInCache = groupMetadataManager.getGroup(groupId).orElseGet(() -> {
                    fail("Group was not loaded into the cache");
                    return null;
                });
                assertSame(group, groupInCache);

                assertEquals(groupId, group.groupId());
                assertEquals(Stable, group.currentState());

                assertEquals("segment2 group record member should be elected",
                    segment2MemberId, group.leaderOrNull());
                assertEquals("segment2 group record member should be only member",
                    Lists.newArrayList(segment2MemberId),
                    group.allMembers().stream().collect(Collectors.toList()));

                // offsets of segment1 should be overridden by segment2 offsets of the same topic partitions
                Map<TopicPartition, Long> committedOffsets = new HashMap<>();
                committedOffsets.putAll(segment1Offsets);
                committedOffsets.putAll(segment2Offsets);
                assertEquals(committedOffsets.size(), group.allOffsets().size());
                committedOffsets.forEach((tp, offset) -> {
                    assertEquals(Optional.of(offset), group.offset(tp).map(OffsetAndMetadata::offset));
                });
            });
    }

    @Test
    public void testAddGroup() {
        GroupMetadata group = new GroupMetadata("foo", Empty);
        assertEquals(group, groupMetadataManager.addGroup(group));
        assertEquals(group, groupMetadataManager.addGroup(
            new GroupMetadata("foo", Empty)
        ));
    }

    @Test
    public void testStoreEmptyGroup() throws Exception {
        final String topicName = "test-store-empty-group";

        runGroupMetadataManagerConsumerTester(topicName, (groupMetadataManager, consumer) -> {
            int generation = 27;
            String protocolType = "consumer";
            GroupMetadata group = GroupMetadata.loadGroup(
                groupId,
                Empty,
                generation,
                protocolType,
                null,
                null,
                Collections.emptyList()
            );
            groupMetadataManager.addGroup(group);

            Errors errors = groupMetadataManager.storeGroup(group, Collections.emptyMap()).get();
            assertEquals(Errors.NONE, errors);

            Message<ByteBuffer> message = consumer.receive();
            assertTrue(message.getEventTime() > 0L);
            assertTrue(message.hasKey());
            byte[] key = message.getKeyBytes();
            BaseKey groupKey = GroupMetadataConstants.readMessageKey(ByteBuffer.wrap(key));
            assertTrue(groupKey instanceof GroupMetadataKey);
            GroupMetadataKey groupMetadataKey = (GroupMetadataKey) groupKey;
            assertEquals(groupId, groupMetadataKey.key());

            ByteBuffer value = message.getValue();
            MemoryRecords memRecords = MemoryRecords.readableRecords(value);
            AtomicBoolean verified = new AtomicBoolean(false);
            memRecords.batches().forEach(batch -> {
                for (Record record : batch) {
                    assertFalse(verified.get());
                    BaseKey bk = GroupMetadataConstants.readMessageKey(record.key());
                    assertTrue(bk instanceof GroupMetadataKey);
                    GroupMetadataKey gmk = (GroupMetadataKey) bk;
                    assertEquals(groupId, gmk.key());

                    GroupMetadata gm = GroupMetadataConstants.readGroupMessageValue(
                        groupId, record.value()
                    );
                    assertTrue(gm.is(Empty));
                    assertEquals(generation, gm.generationId());
                    assertEquals(Optional.of(protocolType), gm.protocolType());
                    verified.set(true);
                }
            });
            assertTrue(verified.get());
        });
    }

    @Test
    public void testStoreEmptySimpleGroup() throws Exception {
        final String topicName = "test-store-empty-simple-group";

        runGroupMetadataManagerConsumerTester(topicName, (groupMetadataManager, consumer) -> {

            GroupMetadata group = new GroupMetadata(groupId, Empty);
            groupMetadataManager.addGroup(group);

            Errors errors = groupMetadataManager.storeGroup(group, Collections.emptyMap()).get();
            assertEquals(Errors.NONE, errors);

            Message<ByteBuffer> message = consumer.receive();
            assertTrue(message.getEventTime() > 0L);
            assertTrue(message.hasKey());
            byte[] key = message.getKeyBytes();

            BaseKey groupKey = GroupMetadataConstants.readMessageKey(ByteBuffer.wrap(key));
            assertTrue(groupKey instanceof GroupMetadataKey);
            GroupMetadataKey groupMetadataKey = (GroupMetadataKey) groupKey;
            assertEquals(groupId, groupMetadataKey.key());

            ByteBuffer value = message.getValue();
            MemoryRecords memRecords = MemoryRecords.readableRecords(value);
            AtomicBoolean verified = new AtomicBoolean(false);
            memRecords.batches().forEach(batch -> {
                for (Record record : batch) {
                    assertFalse(verified.get());
                    BaseKey bk = GroupMetadataConstants.readMessageKey(record.key());
                    assertTrue(bk instanceof GroupMetadataKey);
                    GroupMetadataKey gmk = (GroupMetadataKey) bk;
                    assertEquals(groupId, gmk.key());

                    GroupMetadata gm = GroupMetadataConstants.readGroupMessageValue(
                        groupId, record.value()
                    );
                    assertTrue(gm.is(Empty));
                    assertEquals(0, gm.generationId());
                    assertEquals(Optional.empty(), gm.protocolType());
                    verified.set(true);
                }
            });
            assertTrue(verified.get());
        });
    }

    @Test
    public void testStoreNoneEmptyGroup() throws Exception {
        final String topicName = "test-store-non-empty-group";

        runGroupMetadataManagerConsumerTester(topicName, (groupMetadataManager, consumer) -> {
            String memberId = "memberId";
            String clientId = "clientId";
            String clientHost = "localhost";

            GroupMetadata group = new GroupMetadata(groupId, Empty);
            groupMetadataManager.addGroup(group);

            Map<String, byte[]> protocols = new HashMap<>();
            protocols.put("protocol", new byte[0]);
            MemberMetadata member = new MemberMetadata(
                memberId,
                groupId,
                clientId,
                clientHost,
                rebalanceTimeout,
                sessionTimeout,
                protocolType,
                protocols
            );
            CompletableFuture<JoinGroupResult> joinFuture = new CompletableFuture<>();
            member.awaitingJoinCallback(joinFuture);
            group.add(member);
            group.transitionTo(GroupState.PreparingRebalance);
            group.initNextGeneration();

            Map<String, byte[]> assignments = new HashMap<>();
            assignments.put(memberId, new byte[0]);
            Errors errors = groupMetadataManager.storeGroup(group, assignments).get();
            assertEquals(Errors.NONE, errors);

            Message<ByteBuffer> message = consumer.receive();
            assertTrue(message.getEventTime() > 0L);
            assertTrue(message.hasKey());
            byte[] key = message.getKeyBytes();
            BaseKey groupKey = GroupMetadataConstants.readMessageKey(ByteBuffer.wrap(key));
            assertTrue(groupKey instanceof GroupMetadataKey);
            GroupMetadataKey groupMetadataKey = (GroupMetadataKey) groupKey;
            assertEquals(groupId, groupMetadataKey.key());

            ByteBuffer value = message.getValue();
            MemoryRecords memRecords = MemoryRecords.readableRecords(value);
            AtomicBoolean verified = new AtomicBoolean(false);
            memRecords.batches().forEach(batch -> {
                for (Record record : batch) {
                    assertFalse(verified.get());
                    BaseKey bk = GroupMetadataConstants.readMessageKey(record.key());
                    assertTrue(bk instanceof GroupMetadataKey);
                    GroupMetadataKey gmk = (GroupMetadataKey) bk;
                    assertEquals(groupId, gmk.key());

                    GroupMetadata gm = GroupMetadataConstants.readGroupMessageValue(
                        groupId, record.value()
                    );
                    assertEquals(Stable, gm.currentState());
                    assertEquals(1, gm.generationId());
                    assertEquals(Optional.of(protocolType), gm.protocolType());
                    assertEquals("protocol", gm.protocolOrNull());
                    assertTrue(gm.has(memberId));
                    verified.set(true);
                }
            });
            assertTrue(verified.get());
        });
    }

    @Test
    public void testCommitOffset() throws Exception {
        runGroupMetadataManagerConsumerTester("test-commit-offset", (groupMetadataManager, consumer) -> {
            String memberId = "";
            TopicPartition topicPartition = new TopicPartition("foo", 0);
            groupMetadataManager.addPartitionOwnership(groupPartitionId);
            long offset = 37L;

            GroupMetadata group = new GroupMetadata(groupId, Empty);
            groupMetadataManager.addGroup(group);

            Map<TopicPartition, OffsetAndMetadata> offsets = ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(topicPartition, OffsetAndMetadata.apply(offset))
                .build();

            Map<TopicPartition, Errors> commitErrors = groupMetadataManager.storeOffsets(
                group, memberId, offsets
            ).get();

            assertTrue(group.hasOffsets());
            assertFalse(commitErrors.isEmpty());
            Errors maybeError = commitErrors.get(topicPartition);
            assertEquals(Errors.NONE, maybeError);
            assertTrue(group.hasOffsets());

            Map<TopicPartition, PartitionData> cachedOffsets = groupMetadataManager.getOffsets(
                groupId,
                Optional.of(Lists.newArrayList(topicPartition))
            );
            PartitionData maybePartitionResponse = cachedOffsets.get(topicPartition);
            assertNotNull(maybePartitionResponse);

            assertEquals(Errors.NONE, maybePartitionResponse.error);
            assertEquals(offset, maybePartitionResponse.offset);

            Message<ByteBuffer> message = consumer.receive();
            assertTrue(message.getEventTime() > 0L);
            assertTrue(message.hasKey());
            byte[] key = message.getKeyBytes();
            BaseKey groupKey = GroupMetadataConstants.readMessageKey(ByteBuffer.wrap(key));
            assertTrue(groupKey instanceof OffsetKey);

            ByteBuffer value = message.getValue();
            MemoryRecords memRecords = MemoryRecords.readableRecords(value);
            AtomicBoolean verified = new AtomicBoolean(false);
            memRecords.batches().forEach(batch -> {
                for (Record record : batch) {
                    assertFalse(verified.get());
                    BaseKey bk = GroupMetadataConstants.readMessageKey(record.key());
                    assertTrue(bk instanceof OffsetKey);
                    OffsetKey ok = (OffsetKey) bk;
                    GroupTopicPartition gtp = ok.key();
                    assertEquals(groupId, gtp.group());
                    assertEquals(topicPartition, gtp.topicPartition());

                    OffsetAndMetadata gm = GroupMetadataConstants.readOffsetMessageValue(
                        record.value()
                    );
                    assertEquals(offset, gm.offset());
                    verified.set(true);
                }
            });
            assertTrue(verified.get());
        });
    }

    @Test
    public void testTransactionalCommitOffsetCommitted() throws Exception {
        runGroupMetadataManagerConsumerTester("test-commit-offset", (groupMetadataManager, consumer) -> {
            String memberId = "";
            TopicPartition topicPartition = new TopicPartition("foo", 0);
            long offset = 37L;
            long producerId = 232L;
            short producerEpoch = 0;

            groupMetadataManager.addPartitionOwnership(groupPartitionId);

            GroupMetadata group = new GroupMetadata(groupId, Empty);
            groupMetadataManager.addGroup(group);

            Map<TopicPartition, OffsetAndMetadata> offsets = ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(topicPartition, OffsetAndMetadata.apply(offset))
                .build();

            CompletableFuture<MessageId> writeOffsetMessageFuture = new CompletableFuture<>();
            AtomicReference<CompletableFuture<MessageId>> realWriteFutureRef = new AtomicReference<>();
            doAnswer(invocationOnMock -> {
                CompletableFuture<MessageId> realWriteFuture =
                    (CompletableFuture<MessageId>) invocationOnMock.callRealMethod();
                realWriteFutureRef.set(realWriteFuture);
                return writeOffsetMessageFuture;
            }).when(groupMetadataManager).storeOffsetMessage(
                any(byte[].class), any(ByteBuffer.class), anyLong()
            );

            CompletableFuture<Map<TopicPartition, Errors>> storeFuture = groupMetadataManager.storeOffsets(
                group, memberId, offsets, producerId, producerEpoch
            );

            assertTrue(group.hasOffsets());
            assertTrue(group.allOffsets().isEmpty());

            // complete the write message
            writeOffsetMessageFuture.complete(realWriteFutureRef.get().get());
            Map<TopicPartition, Errors> commitErrors = storeFuture.get();

            assertFalse(commitErrors.isEmpty());
            Errors maybeError = commitErrors.get(topicPartition);
            assertEquals(Errors.NONE, maybeError);
            assertTrue(group.hasOffsets());
            assertTrue(group.allOffsets().isEmpty());

            group.completePendingTxnOffsetCommit(producerId, true);
            assertTrue(group.hasOffsets());
            assertFalse(group.allOffsets().isEmpty());

            assertEquals(
                Optional.of(OffsetAndMetadata.apply(offset)),
                group.offset(topicPartition)
            );
        });
    }

    @Test
    public void testTransactionalCommitOffsetAppendFailure() throws Exception {
        runGroupMetadataManagerConsumerTester("test-commit-offset", (groupMetadataManager, consumer) -> {
            String memberId = "";
            TopicPartition topicPartition = new TopicPartition("foo", 0);
            long offset = 37L;
            long producerId = 232L;
            short producerEpoch = 0;

            groupMetadataManager.addPartitionOwnership(groupPartitionId);

            GroupMetadata group = new GroupMetadata(groupId, Empty);
            groupMetadataManager.addGroup(group);

            Map<TopicPartition, OffsetAndMetadata> offsets = ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(topicPartition, OffsetAndMetadata.apply(offset))
                .build();

            CompletableFuture<MessageId> writeOffsetMessageFuture = new CompletableFuture<>();
            AtomicReference<CompletableFuture<MessageId>> realWriteFutureRef = new AtomicReference<>();
            doAnswer(invocationOnMock -> {
                CompletableFuture<MessageId> realWriteFuture =
                    (CompletableFuture<MessageId>) invocationOnMock.callRealMethod();
                realWriteFutureRef.set(realWriteFuture);
                return writeOffsetMessageFuture;
            }).when(groupMetadataManager).storeOffsetMessage(
                any(byte[].class), any(ByteBuffer.class), anyLong()
            );

            CompletableFuture<Map<TopicPartition, Errors>> storeFuture = groupMetadataManager.storeOffsets(
                group, memberId, offsets, producerId, producerEpoch
            );

            assertTrue(group.hasOffsets());
            assertTrue(group.allOffsets().isEmpty());

            // complete the write message
            writeOffsetMessageFuture.completeExceptionally(
                new Exception("Not enought replicas")
            );
            Map<TopicPartition, Errors> commitErrors = storeFuture.get();

            assertFalse(commitErrors.isEmpty());
            Errors maybeError = commitErrors.get(topicPartition);
            assertEquals(Errors.UNKNOWN_SERVER_ERROR, maybeError);
            assertFalse(group.hasOffsets());
            assertTrue(group.allOffsets().isEmpty());

            group.completePendingTxnOffsetCommit(producerId, false);
            assertFalse(group.hasOffsets());
            assertTrue(group.allOffsets().isEmpty());
        });
    }

    @Test
    public void testTransactionalCommitOffsetAborted() throws Exception {
        runGroupMetadataManagerConsumerTester("test-commit-offset", (groupMetadataManager, consumer) -> {
            String memberId = "";
            TopicPartition topicPartition = new TopicPartition("foo", 0);
            long offset = 37L;
            long producerId = 232L;
            short producerEpoch = 0;

            groupMetadataManager.addPartitionOwnership(groupPartitionId);

            GroupMetadata group = new GroupMetadata(groupId, Empty);
            groupMetadataManager.addGroup(group);

            Map<TopicPartition, OffsetAndMetadata> offsets = ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(topicPartition, OffsetAndMetadata.apply(offset))
                .build();

            CompletableFuture<MessageId> writeOffsetMessageFuture = new CompletableFuture<>();
            AtomicReference<CompletableFuture<MessageId>> realWriteFutureRef = new AtomicReference<>();
            doAnswer(invocationOnMock -> {
                CompletableFuture<MessageId> realWriteFuture =
                    (CompletableFuture<MessageId>) invocationOnMock.callRealMethod();
                realWriteFutureRef.set(realWriteFuture);
                return writeOffsetMessageFuture;
            }).when(groupMetadataManager).storeOffsetMessage(
                any(byte[].class), any(ByteBuffer.class), anyLong()
            );

            CompletableFuture<Map<TopicPartition, Errors>> storeFuture = groupMetadataManager.storeOffsets(
                group, memberId, offsets, producerId, producerEpoch
            );

            assertTrue(group.hasOffsets());
            assertTrue(group.allOffsets().isEmpty());

            // complete the write message
            writeOffsetMessageFuture.complete(realWriteFutureRef.get().get());
            Map<TopicPartition, Errors> commitErrors = storeFuture.get();

            assertFalse(commitErrors.isEmpty());
            Errors maybeError = commitErrors.get(topicPartition);
            assertEquals(Errors.NONE, maybeError);
            assertTrue(group.hasOffsets());
            assertTrue(group.allOffsets().isEmpty());

            group.completePendingTxnOffsetCommit(producerId, false);
            assertFalse(group.hasOffsets());
            assertTrue(group.allOffsets().isEmpty());
        });
    }

    @Test
    public void testExpiredOffset() throws Exception {
        runGroupMetadataManagerConsumerTester("test-commit-offset", (groupMetadataManager, consumer) -> {
            String memberId = "";
            TopicPartition topicPartition1 = new TopicPartition("foo", 0);
            TopicPartition topicPartition2 = new TopicPartition("foo", 1);
            groupMetadataManager.addPartitionOwnership(groupPartitionId);
            long offset = 37L;

            GroupMetadata group = new GroupMetadata(groupId, Empty);
            groupMetadataManager.addGroup(group);

            long startMs = time.milliseconds();
            Map<TopicPartition, OffsetAndMetadata> offsets = ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(topicPartition1, OffsetAndMetadata.apply(
                    offset, "", startMs, startMs + 1))
                .put(topicPartition2, OffsetAndMetadata.apply(
                    offset, "", startMs, startMs + 3))
                .build();

            Map<TopicPartition, Errors> commitErrors = groupMetadataManager.storeOffsets(
                group, memberId, offsets
            ).get();
            assertTrue(group.hasOffsets());

            assertFalse(commitErrors.isEmpty());
            Errors maybeError = commitErrors.get(topicPartition1);
            assertEquals(Errors.NONE, maybeError);

            // expire only one of the offsets
            time.sleep(2);

            groupMetadataManager.cleanupGroupMetadata();

            assertEquals(Optional.of(group), groupMetadataManager.getGroup(groupId));
            assertEquals(Optional.empty(), group.offset(topicPartition1));
            assertEquals(Optional.of(offset), group.offset(topicPartition2).map(OffsetAndMetadata::offset));

            Map<TopicPartition, PartitionData> cachedOffsets = groupMetadataManager.getOffsets(
                groupId,
                Optional.of(Lists.newArrayList(
                    topicPartition1,
                    topicPartition2
                ))
            );
            assertEquals(
                OffsetFetchResponse.INVALID_OFFSET,
                cachedOffsets.get(topicPartition1).offset);
            assertEquals(
                offset,
                cachedOffsets.get(topicPartition2).offset);
        });
    }

    @Test
    public void testGroupMetadataRemoval() throws Exception {
        runGroupMetadataManagerConsumerTester("test-commit-offset", (groupMetadataManager, consumer) -> {
            TopicPartition topicPartition1 = new TopicPartition("foo", 0);
            TopicPartition topicPartition2 = new TopicPartition("foo", 1);

            groupMetadataManager.addPartitionOwnership(groupPartitionId);

            GroupMetadata group = new GroupMetadata(groupId, Empty);
            groupMetadataManager.addGroup(group);
            group.generationId(5);

            groupMetadataManager.cleanupGroupMetadata().get();

            Message<ByteBuffer> message = consumer.receive();
            assertTrue(message.getEventTime() > 0L);
            assertTrue(message.hasKey());
            byte[] key = message.getKeyBytes();

            BaseKey groupKey = GroupMetadataConstants.readMessageKey(ByteBuffer.wrap(key));
            assertTrue(groupKey instanceof GroupMetadataKey);
            GroupMetadataKey groupMetadataKey = (GroupMetadataKey) groupKey;
            assertEquals(groupId, groupMetadataKey.key());

            ByteBuffer value = message.getValue();
            MemoryRecords memRecords = MemoryRecords.readableRecords(value);
            AtomicBoolean verified = new AtomicBoolean(false);
            memRecords.batches().forEach(batch -> {
                assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, batch.magic());
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
                for (Record record : batch) {
                    assertFalse(verified.get());
                    assertTrue(record.hasKey());
                    assertFalse(record.hasValue());
                    assertTrue(record.timestamp() > 0);
                    BaseKey bk = GroupMetadataConstants.readMessageKey(record.key());
                    assertTrue(bk instanceof GroupMetadataKey);
                    GroupMetadataKey gmk = (GroupMetadataKey) bk;
                    assertEquals(groupId, gmk.key());
                    verified.set(true);
                }
            });
            assertTrue(verified.get());
            assertEquals(Optional.empty(), groupMetadataManager.getGroup(groupId));
            Map<TopicPartition, PartitionData> cachedOffsets = groupMetadataManager.getOffsets(
                groupId,
                Optional.of(Lists.newArrayList(
                    topicPartition1,
                    topicPartition2
                ))
            );
            assertEquals(
                OffsetFetchResponse.INVALID_OFFSET,
                cachedOffsets.get(topicPartition1).offset);
            assertEquals(
                OffsetFetchResponse.INVALID_OFFSET,
                cachedOffsets.get(topicPartition2).offset);
        });
    }

    @Test
    public void testExpireGroupWithOffsetsOnly() throws Exception {
        runGroupMetadataManagerConsumerTester("test-commit-offset", (groupMetadataManager, consumer) -> {
            // verify that the group is removed properly, but no tombstone is written if
            // this is a group which is only using kafka for offset storage

            String memberId = "";
            TopicPartition topicPartition1 = new TopicPartition("foo", 0);
            TopicPartition topicPartition2 = new TopicPartition("foo", 1);
            long offset = 37;

            groupMetadataManager.addPartitionOwnership(groupPartitionId);

            GroupMetadata group = new GroupMetadata(groupId, Empty);
            groupMetadataManager.addGroup(group);

            long startMs = time.milliseconds();
            Map<TopicPartition, OffsetAndMetadata> offsets = ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(topicPartition1, OffsetAndMetadata.apply(offset, "", startMs, startMs + 1))
                .put(topicPartition2, OffsetAndMetadata.apply(offset, "", startMs, startMs + 3))
                .build();

            Map<TopicPartition, Errors> commitErrors =
                groupMetadataManager.storeOffsets(group, memberId, offsets).get();
            assertTrue(group.hasOffsets());

            assertFalse(commitErrors.isEmpty());
            assertEquals(
                Errors.NONE,
                commitErrors.get(topicPartition1)
            );

            // expire all of the offsets
            time.sleep(4);

            groupMetadataManager.cleanupGroupMetadata().get();

            // skip `storeOffsets` op
            consumer.receive();

            Message<ByteBuffer> message = consumer.receive();
            assertTrue(message.getEventTime() > 0L);
            assertTrue(message.hasKey());
            byte[] key = message.getKeyBytes();

            BaseKey groupKey = GroupMetadataConstants.readMessageKey(ByteBuffer.wrap(key));
            assertTrue(groupKey instanceof GroupMetadataKey);
            GroupMetadataKey gmk = (GroupMetadataKey) groupKey;
            assertEquals(groupId, gmk.key());

            ByteBuffer value = message.getValue();
            MemoryRecords memRecords = MemoryRecords.readableRecords(value);
            AtomicInteger verified = new AtomicInteger(2);
            memRecords.batches().forEach(batch -> {
                assertEquals(RecordBatch.CURRENT_MAGIC_VALUE, batch.magic());
                assertEquals(TimestampType.CREATE_TIME, batch.timestampType());
                for (Record record : batch) {
                    verified.decrementAndGet();
                    assertTrue(record.hasKey());
                    assertFalse(record.hasValue());
                    assertTrue(record.timestamp() > 0);
                    BaseKey bk = GroupMetadataConstants.readMessageKey(record.key());
                    assertTrue(bk instanceof OffsetKey);
                    OffsetKey ok = (OffsetKey) bk;
                    assertEquals(groupId, ok.key().group());
                    assertEquals("foo", ok.key().topicPartition().topic());
                }
            });
            assertEquals(0, verified.get());
            assertEquals(Optional.empty(), groupMetadataManager.getGroup(groupId));
            Map<TopicPartition, PartitionData> cachedOffsets = groupMetadataManager.getOffsets(
                groupId,
                Optional.of(Lists.newArrayList(
                    topicPartition1,
                    topicPartition2
                ))
            );
            assertEquals(
                OffsetFetchResponse.INVALID_OFFSET,
                cachedOffsets.get(topicPartition1).offset);
            assertEquals(
                OffsetFetchResponse.INVALID_OFFSET,
                cachedOffsets.get(topicPartition2).offset);
        });
    }

    @Test
    public void testExpireOffsetsWithActiveGroup() throws Exception {
        runGroupMetadataManagerConsumerTester("test-commit-offset", (groupMetadataManager, consumer) -> {
            String memberId = "memberId";
            String clientId = "clientId";
            String clientHost = "localhost";
            TopicPartition topicPartition1 = new TopicPartition("foo", 0);
            TopicPartition topicPartition2 = new TopicPartition("foo", 1);
            long offset = 37;

            groupMetadataManager.addPartitionOwnership(groupPartitionId);

            GroupMetadata group = new GroupMetadata(groupId, Empty);
            groupMetadataManager.addGroup(group);

            MemberMetadata member = new MemberMetadata(
                memberId, groupId, clientId, clientHost,
                rebalanceTimeout,
                sessionTimeout,
                protocolType,
                ImmutableMap.<String, byte[]>builder()
                    .put("protocol", new byte[0])
                    .build()
            );
            CompletableFuture<JoinGroupResult> memberJoinFuture = new CompletableFuture<>();
            member.awaitingJoinCallback(memberJoinFuture);
            group.add(member);
            group.transitionTo(PreparingRebalance);
            group.initNextGeneration();

            long startMs = time.milliseconds();
            Map<TopicPartition, OffsetAndMetadata> offsets = ImmutableMap.<TopicPartition, OffsetAndMetadata>builder()
                .put(topicPartition1, OffsetAndMetadata.apply(offset, "", startMs, startMs + 1))
                .put(topicPartition2, OffsetAndMetadata.apply(offset, "", startMs, startMs + 3))
                .build();

            Map<TopicPartition, Errors> commitErrors =
                groupMetadataManager.storeOffsets(group, memberId, offsets).get();
            assertTrue(group.hasOffsets());

            assertFalse(commitErrors.isEmpty());
            assertEquals(
                Errors.NONE,
                commitErrors.get(topicPartition1)
            );

            // expire all of the offsets
            time.sleep(4);

            groupMetadataManager.cleanupGroupMetadata().get();

            // group should still be there, but the offsets should be gone
            assertEquals(
                Optional.of(group),
                groupMetadataManager.getGroup(groupId)
            );
            assertEquals(
                Optional.empty(),
                group.offset(topicPartition1)
            );
            assertEquals(
                Optional.empty(),
                group.offset(topicPartition2)
            );

            Map<TopicPartition, PartitionData> cachedOffsets = groupMetadataManager.getOffsets(
                groupId,
                Optional.of(Lists.newArrayList(
                    topicPartition1,
                    topicPartition2
                ))
            );
            assertEquals(
                OffsetFetchResponse.INVALID_OFFSET,
                cachedOffsets.get(topicPartition1).offset);
            assertEquals(
                OffsetFetchResponse.INVALID_OFFSET,
                cachedOffsets.get(topicPartition2).offset);
        });
    }

    /**
     * A group metadata manager test runner.
     */
    @FunctionalInterface
    public interface GroupMetadataManagerProducerTester {

        void test(GroupMetadataManager groupMetadataManager,
                  Producer<ByteBuffer> consumer) throws Exception;

    }

    /**
     * A group metadata manager test runner.
     */
    @FunctionalInterface
    public interface GroupMetadataManagerConsumerTester {

        void test(GroupMetadataManager groupMetadataManager,
                  Consumer<ByteBuffer> consumer) throws Exception;

    }

    void runGroupMetadataManagerProducerTester(final String topicName,
                                               GroupMetadataManagerProducerTester tester) throws Exception {
        @Cleanup
        Producer<ByteBuffer> producer = pulsarClient.newProducer(Schema.BYTEBUFFER)
            .topic(topicName)
            .create();
        @Cleanup
        Consumer<ByteBuffer> consumer = pulsarClient.newConsumer(Schema.BYTEBUFFER)
            .topic(topicName)
            .subscriptionName("test-sub")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();
        @Cleanup
        Reader<ByteBuffer> reader = pulsarClient.newReader(Schema.BYTEBUFFER)
            .topic(topicName)
            .startMessageId(MessageId.earliest)
            .create();
        groupMetadataManager = spy(new GroupMetadataManager(
            1,
            offsetConfig,
            producer,
            reader,
            scheduler,
            time
        ));
        tester.test(groupMetadataManager, producer);
    }

    void runGroupMetadataManagerConsumerTester(final String topicName,
                                               GroupMetadataManagerConsumerTester tester) throws Exception {
        @Cleanup
        Producer<ByteBuffer> producer = pulsarClient.newProducer(Schema.BYTEBUFFER)
            .topic(topicName)
            .create();
        @Cleanup
        Consumer<ByteBuffer> consumer = pulsarClient.newConsumer(Schema.BYTEBUFFER)
            .topic(topicName)
            .subscriptionName("test-sub")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();
        @Cleanup
        Reader<ByteBuffer> reader = pulsarClient.newReader(Schema.BYTEBUFFER)
            .topic(topicName)
            .startMessageId(MessageId.earliest)
            .create();
        groupMetadataManager = spy(new GroupMetadataManager(
            1,
            offsetConfig,
            producer,
            reader,
            scheduler,
            time
        ));
        tester.test(groupMetadataManager, consumer);
    }

}
