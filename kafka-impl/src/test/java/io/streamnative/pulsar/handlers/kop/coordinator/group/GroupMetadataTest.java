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

import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupState.CompletingRebalance;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupState.Dead;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupState.Empty;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupState.PreparingRebalance;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupState.Stable;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata.CommitRecordMetadataAndOffset;
import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.val;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link GroupMetadata}.
 */
public class GroupMetadataTest {

    private static final String protocolType = "consumer";
    private static final String groupId = "test-group-id";
    private static final String clientId = "test-client-id";
    private static final String clientHost = "test-client-host";
    private static final String NAMESPACE_PREFIX = "public/default";
    private static final int rebalanceTimeoutMs = 60000;
    private static final int sessionTimeoutMs = 10000;

    private GroupMetadata group = null;

    @BeforeMethod
    public void setUp() {
        group = new GroupMetadata(groupId, Empty);
    }

    @Test
    public void testCanRebalanceWhenStable() {
        assertTrue(group.canReblance());
    }

    @Test
    public void testCanRebalanceWhenCompletingRebalance() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(CompletingRebalance);
        assertTrue(group.canReblance());
    }

    @Test
    public void testCannotRebalanceWhenPreparingRebalance() {
        group.transitionTo(PreparingRebalance);
        assertFalse(group.canReblance());
    }

    @Test
    public void testCannotRebalanceWhenDead() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Empty);
        group.transitionTo(Dead);
        assertFalse(group.canReblance());
    }

    @Test
    public void testStableToPreparingRebalanceTransition() {
        group.transitionTo(PreparingRebalance);
        assertState(group, PreparingRebalance);
    }

    @Test
    public void testStableToDeadTransition() {
        group.transitionTo(Dead);
        assertState(group, Dead);
    }

    @Test
    public void testAwaitingRebalanceToPreparingRebalanceTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(CompletingRebalance);
        group.transitionTo(PreparingRebalance);
        assertState(group, PreparingRebalance);
    }

    @Test
    public void testPreparingRebalanceToDeadTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Dead);
        assertState(group, Dead);
    }

    @Test
    public void testPreparingRebalanceToEmptyTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Empty);
        assertState(group, Empty);
    }

    @Test
    public void testEmptyToDeadTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Empty);
        group.transitionTo(Dead);
        assertState(group, Dead);
    }

    @Test
    public void testAwaitingRebalanceToStableTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(CompletingRebalance);
        group.transitionTo(Stable);
        assertState(group, Stable);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testEmptyToStableIllegalTransition() {
        group.transitionTo(Stable);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testStableToStableIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(CompletingRebalance);
        group.transitionTo(Stable);
        group.transitionTo(Stable);
        fail("should have failed due to illegal transition");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testEmptyToAwaitingRebalanceIllegalTransition() {
        group.transitionTo(CompletingRebalance);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testPreparingRebalanceToPreparingRebalanceIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(PreparingRebalance);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testPreparingRebalanceToStableIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Stable);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testAwaitingRebalanceToAwaitingRebalanceIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(CompletingRebalance);
        group.transitionTo(CompletingRebalance);
    }

    @Test
    public void testDeadToDeadIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Dead);
        group.transitionTo(Dead);
        assertState(group, Dead);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDeadToStableIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Dead);
        group.transitionTo(Stable);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDeadToPreparingRebalanceIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Dead);
        group.transitionTo(PreparingRebalance);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testDeadToAwaitingRebalanceIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Dead);
        group.transitionTo(CompletingRebalance);
    }

    @Test
    public void testSelectProtocol() {
        String memberId = "memberId";
        Map<String, byte[]> protocols = new LinkedHashMap<>();
        protocols.put("range", new byte[0]);
        protocols.put("roundrobin", new byte[0]);
        MemberMetadata member = new MemberMetadata(
            memberId,
            groupId,
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols);

        group.add(member);
        assertEquals(group.selectProtocol(), "range");

        String otherMemberId = "otherMemberId";
        protocols = new LinkedHashMap<>();
        protocols.put("roundrobin", new byte[0]);
        protocols.put("range", new byte[0]);
        MemberMetadata otherMember = new MemberMetadata(
            otherMemberId,
            groupId,
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols);

        group.add(otherMember);
        // now could be either range or robin since there is no majority preference
        assertTrue(protocols.keySet().contains(group.selectProtocol()));

        String lastMemberId = "lastMemberId";
        protocols = new LinkedHashMap<>();
        protocols.put("roundrobin", new byte[0]);
        protocols.put("range", new byte[0]);
        val lastMember = new MemberMetadata(
            lastMemberId,
            groupId,
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols);

        group.add(lastMember);
        // now we should prefer 'roundrobin'
        assertEquals("roundrobin", group.selectProtocol());
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testSelectProtocolRaisesIfNoMembers() {
        group.selectProtocol();
        fail("Should not reach here");
    }

    @Test
    public void testSelectProtocolChoosesCompatibleProtocol() {
        String memberId = "memberId";
        Map<String, byte[]> protocols = new LinkedHashMap<>();
        protocols.put("range", new byte[0]);
        protocols.put("roundrobin", new byte[0]);
        MemberMetadata member = new MemberMetadata(
            memberId,
            groupId,
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols);

        String otherMemberId = "otherMemberId";
        protocols = new LinkedHashMap<>();
        protocols.put("roundrobin", new byte[0]);
        protocols.put("blah", new byte[0]);
        MemberMetadata otherMember = new MemberMetadata(
            otherMemberId,
            groupId,
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols);

        group.add(member);
        group.add(otherMember);
        assertEquals("roundrobin", group.selectProtocol());
    }

    @Test
    public void testSupportsProtocols() {
        // by default, the group supports everything
        assertTrue(group.supportsProtocols(Sets.newHashSet("roundrobin", "range")));

        String memberId = "memberId";
        Map<String, byte[]> protocols = new LinkedHashMap<>();
        protocols.put("range", new byte[0]);
        protocols.put("roundrobin", new byte[0]);
        MemberMetadata member = new MemberMetadata(
            memberId,
            groupId,
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols);

        group.add(member);
        assertTrue(group.supportsProtocols(Sets.newHashSet("roundrobin", "foo")));
        assertTrue(group.supportsProtocols(Sets.newHashSet("range", "foo")));
        assertFalse(group.supportsProtocols(Sets.newHashSet("foo", "bar")));

        String otherMemberId = "otherMemberId";
        protocols = new LinkedHashMap<>();
        protocols.put("roundrobin", new byte[0]);
        protocols.put("blah", new byte[0]);
        MemberMetadata otherMember = new MemberMetadata(
            otherMemberId,
            groupId,
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols);

        group.add(otherMember);

        assertTrue(group.supportsProtocols(Sets.newHashSet("roundrobin", "foo")));
        assertFalse(group.supportsProtocols(Sets.newHashSet("range", "foo")));
    }

    @Test
    public void testInitNextGeneration() {
        val memberId = "memberId";
        Map<String, byte[]> protocols = new LinkedHashMap<>();
        protocols.put("roundrobin", new byte[0]);
        val member = new MemberMetadata(
            memberId,
            groupId,
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols);

        group.transitionTo(PreparingRebalance);
        member.awaitingJoinCallback(new CompletableFuture<>());
        group.add(member);

        assertEquals(0, group.generationId());
        assertNull(group.protocolOrNull());

        group.initNextGeneration();

        assertEquals(1, group.generationId());
        assertEquals("roundrobin", group.protocolOrNull());
    }

    @Test
    public void testInitNextGenerationEmptyGroup() {
        assertEquals(Empty, group.currentState());
        assertEquals(0, group.generationId());
        assertNull(group.protocolOrNull());

        group.transitionTo(PreparingRebalance);
        group.initNextGeneration();

        assertEquals(1, group.generationId());
        assertNull(group.protocolOrNull());
    }

    @Test
    public void testOffsetCommit() {
        TopicPartition partition = new TopicPartition("foo", 0);
        OffsetAndMetadata offset = OffsetAndMetadata.apply(37);
        long commitRecordOffset = 3;

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(partition, offset);
        group.prepareOffsetCommit(offsets);
        assertTrue(group.hasOffsets());
        assertEquals(Optional.empty(), group.offset(partition, NAMESPACE_PREFIX));

        group.onOffsetCommitAppend(
            partition,
            new CommitRecordMetadataAndOffset(Optional.of(commitRecordOffset), offset));
        assertTrue(group.hasOffsets());
        assertEquals(Optional.of(offset), group.offset(partition, NAMESPACE_PREFIX));
    }

    @Test
    public void testOffsetCommitFailure() {
        TopicPartition partition = new TopicPartition("foo", 0);
        OffsetAndMetadata offset = OffsetAndMetadata.apply(37);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(partition, offset);
        group.prepareOffsetCommit(offsets);
        assertTrue(group.hasOffsets());
        assertEquals(Optional.empty(), group.offset(partition, NAMESPACE_PREFIX));

        group.failPendingOffsetWrite(partition, offset);
        assertFalse(group.hasOffsets());
        assertEquals(Optional.empty(), group.offset(partition, NAMESPACE_PREFIX));
    }

    @Test
    public void testOffsetCommitFailureWithAnotherPending() {
        TopicPartition partition = new TopicPartition("foo", 0);
        OffsetAndMetadata firstOffset = OffsetAndMetadata.apply(37);
        OffsetAndMetadata secondOffset = OffsetAndMetadata.apply(57);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(partition, firstOffset);
        group.prepareOffsetCommit(offsets);
        assertTrue(group.hasOffsets());
        assertEquals(Optional.empty(), group.offset(partition, NAMESPACE_PREFIX));

        offsets = new HashMap<>();
        offsets.put(partition, secondOffset);
        group.prepareOffsetCommit(offsets);
        assertTrue(group.hasOffsets());

        group.failPendingOffsetWrite(partition, firstOffset);
        assertTrue(group.hasOffsets());
        assertEquals(Optional.empty(), group.offset(partition, NAMESPACE_PREFIX));

        group.onOffsetCommitAppend(partition, new CommitRecordMetadataAndOffset(Optional.of(3L), secondOffset));
        assertTrue(group.hasOffsets());
        assertEquals(Optional.of(secondOffset), group.offset(partition, NAMESPACE_PREFIX));
    }

    @Test
    public void testOffsetCommitWithAnotherPending() {
        TopicPartition partition = new TopicPartition("foo", 0);
        OffsetAndMetadata firstOffset = OffsetAndMetadata.apply(37);
        OffsetAndMetadata secondOffset = OffsetAndMetadata.apply(57);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(partition, firstOffset);
        group.prepareOffsetCommit(offsets);
        assertTrue(group.hasOffsets());
        assertEquals(Optional.empty(), group.offset(partition, NAMESPACE_PREFIX));

        offsets = new HashMap<>();
        offsets.put(partition, secondOffset);
        group.prepareOffsetCommit(offsets);
        assertTrue(group.hasOffsets());

        group.onOffsetCommitAppend(partition, new CommitRecordMetadataAndOffset(Optional.of(4L), firstOffset));
        assertTrue(group.hasOffsets());
        assertEquals(Optional.of(firstOffset), group.offset(partition, NAMESPACE_PREFIX));

        group.onOffsetCommitAppend(partition, new CommitRecordMetadataAndOffset(Optional.of(5L), secondOffset));
        assertTrue(group.hasOffsets());
        assertEquals(Optional.of(secondOffset), group.offset(partition, NAMESPACE_PREFIX));
    }

    @Test
    public void testConsumerBeatsTransactionalOffsetCommit() {
        TopicPartition partition = new TopicPartition("foo", 0);
        long producerId = 13232L;
        OffsetAndMetadata txnOffsetCommit = OffsetAndMetadata.apply(37);
        OffsetAndMetadata consumerOffsetCommit = OffsetAndMetadata.apply(57);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(partition, txnOffsetCommit);
        group.prepareTxnOffsetCommit(producerId, offsets);
        assertTrue(group.hasOffsets());
        assertEquals(Optional.empty(), group.offset(partition, NAMESPACE_PREFIX));

        offsets = new HashMap<>();
        offsets.put(partition, consumerOffsetCommit);
        group.prepareOffsetCommit(offsets);
        assertTrue(group.hasOffsets());

        group.onTxnOffsetCommitAppend(producerId, partition,
            new CommitRecordMetadataAndOffset(Optional.of(3L), txnOffsetCommit));
        group.onOffsetCommitAppend(partition,
            new CommitRecordMetadataAndOffset(Optional.of(4L), consumerOffsetCommit));
        assertTrue(group.hasOffsets());
        assertEquals(Optional.of(consumerOffsetCommit), group.offset(partition, NAMESPACE_PREFIX));

        group.completePendingTxnOffsetCommit(producerId, true);
        assertTrue(group.hasOffsets());
        // This is the crucial assertion which validates that we materialize offsets in offset order,
        // not transactional order.
        assertEquals(Optional.of(consumerOffsetCommit), group.offset(partition, NAMESPACE_PREFIX));
    }

    @Test
    public void testTransactionBeatsConsumerOffsetCommit() {
        TopicPartition partition = new TopicPartition("foo", 0);
        long producerId = 13232L;
        OffsetAndMetadata txnOffsetCommit = OffsetAndMetadata.apply(37);
        OffsetAndMetadata consumerOffsetCommit = OffsetAndMetadata.apply(57);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(partition, txnOffsetCommit);
        group.prepareTxnOffsetCommit(producerId, offsets);
        assertTrue(group.hasOffsets());
        assertEquals(Optional.empty(), group.offset(partition, NAMESPACE_PREFIX));

        offsets = new HashMap<>();
        offsets.put(partition, consumerOffsetCommit);
        group.prepareOffsetCommit(offsets);
        assertTrue(group.hasOffsets());

        group.onOffsetCommitAppend(
            partition, new CommitRecordMetadataAndOffset(Optional.of(3L), consumerOffsetCommit));
        group.onTxnOffsetCommitAppend(producerId, partition,
            new CommitRecordMetadataAndOffset(Optional.of(4L), txnOffsetCommit));
        assertTrue(group.hasOffsets());
        // The transactional offset commit hasn't been committed yet, so we should materialize
        // the consumer offset commit.
        assertEquals(Optional.of(consumerOffsetCommit), group.offset(partition, NAMESPACE_PREFIX));

        group.completePendingTxnOffsetCommit(producerId, true);
        assertTrue(group.hasOffsets());
        // The transactional offset commit has been materialized and the transactional commit record
        // is later in the log, so it should be materialized.
        assertEquals(Optional.of(txnOffsetCommit), group.offset(partition, NAMESPACE_PREFIX));
    }

    @Test
    public void testTransactionalCommitIsAbortedAndConsumerCommitWins() {
        TopicPartition partition = new TopicPartition("foo", 0);
        long producerId = 13232L;
        OffsetAndMetadata txnOffsetCommit = OffsetAndMetadata.apply(37);
        OffsetAndMetadata consumerOffsetCommit = OffsetAndMetadata.apply(57);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(partition, txnOffsetCommit);
        group.prepareTxnOffsetCommit(producerId, offsets);
        assertTrue(group.hasOffsets());
        assertEquals(Optional.empty(), group.offset(partition, NAMESPACE_PREFIX));

        offsets = new HashMap<>();
        offsets.put(partition, consumerOffsetCommit);
        group.prepareOffsetCommit(offsets);
        assertTrue(group.hasOffsets());

        group.onOffsetCommitAppend(partition,
            new CommitRecordMetadataAndOffset(Optional.of(3L), consumerOffsetCommit));
        group.onTxnOffsetCommitAppend(producerId, partition,
            new CommitRecordMetadataAndOffset(Optional.of(4L), txnOffsetCommit));
        assertTrue(group.hasOffsets());
        // The transactional offset commit hasn't been committed yet, so we should materialize the consumer
        // offset commit.
        assertEquals(Optional.of(consumerOffsetCommit), group.offset(partition, NAMESPACE_PREFIX));

        group.completePendingTxnOffsetCommit(producerId, false);
        assertTrue(group.hasOffsets());
        // The transactional offset commit should be discarded and the consumer offset commit should continue to be
        // materialized.
        assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId));
        assertEquals(Optional.of(consumerOffsetCommit), group.offset(partition, NAMESPACE_PREFIX));
    }

    @Test
    public void testFailedTxnOffsetCommitLeavesNoPendingState() {
        TopicPartition partition = new TopicPartition("foo", 0);
        long producerId = 13232L;
        OffsetAndMetadata txnOffsetCommit = OffsetAndMetadata.apply(37);

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(partition, txnOffsetCommit);
        group.prepareTxnOffsetCommit(producerId, offsets);
        assertTrue(group.hasPendingOffsetCommitsFromProducer(producerId));
        assertTrue(group.hasOffsets());
        assertEquals(Optional.empty(), group.offset(partition, NAMESPACE_PREFIX));
        group.failPendingTxnOffsetCommit(producerId, partition);
        assertFalse(group.hasOffsets());
        assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId));

        // The commit marker should now have no effect.
        group.completePendingTxnOffsetCommit(producerId, true);
        assertFalse(group.hasOffsets());
        assertFalse(group.hasPendingOffsetCommitsFromProducer(producerId));
    }

    private void assertState(GroupMetadata group, GroupState targetState) {
        Set<GroupState> states = Sets.newHashSet(
            Stable, PreparingRebalance, CompletingRebalance, Dead
        );
        Set<GroupState> otherStates = Sets.newHashSet(states);
        otherStates.remove(targetState);
        otherStates.forEach(otherState -> assertFalse(group.is(otherState)));
        assertTrue(group.is(targetState));
    }

}
