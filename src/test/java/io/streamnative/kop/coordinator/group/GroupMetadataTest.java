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

import static io.streamnative.kop.coordinator.group.GroupState.CompletingRebalance;
import static io.streamnative.kop.coordinator.group.GroupState.Dead;
import static io.streamnative.kop.coordinator.group.GroupState.Empty;
import static io.streamnative.kop.coordinator.group.GroupState.PreparingRebalance;
import static io.streamnative.kop.coordinator.group.GroupState.Stable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Sets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import lombok.val;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link GroupMetadata}.
 */
public class GroupMetadataTest {

    private static final String protocolType = "consumer";
    private static final String groupId = "test-group-id";
    private static final String clientId = "test-client-id";
    private static final String clientHost = "test-client-host";
    private static final int rebalanceTimeoutMs = 60000;
    private static final int sessionTimeoutMs = 10000;

    private GroupMetadata group = null;

    @Before
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

    @Test(expected = IllegalStateException.class)
    public void testEmptyToStableIllegalTransition() {
        group.transitionTo(Stable);
    }

    @Test(expected = IllegalStateException.class)
    public void testStableToStableIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(CompletingRebalance);
        group.transitionTo(Stable);
        group.transitionTo(Stable);
        fail("should have failed due to illegal transition");
    }

    @Test(expected = IllegalStateException.class)
    public void testEmptyToAwaitingRebalanceIllegalTransition() {
        group.transitionTo(CompletingRebalance);
    }

    @Test(expected = IllegalStateException.class)
    public void testPreparingRebalanceToPreparingRebalanceIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(PreparingRebalance);
    }

    @Test(expected = IllegalStateException.class)
    public void testPreparingRebalanceToStableIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Stable);
    }

    @Test(expected = IllegalStateException.class)
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

    @Test(expected = IllegalStateException.class)
    public void testDeadToStableIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Dead);
        group.transitionTo(Stable);
    }

    @Test(expected = IllegalStateException.class)
    public void testDeadToPreparingRebalanceIllegalTransition() {
        group.transitionTo(PreparingRebalance);
        group.transitionTo(Dead);
        group.transitionTo(PreparingRebalance);
    }

    @Test(expected = IllegalStateException.class)
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
        assertEquals("range", group.selectProtocol());

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

    @Test(expected = IllegalStateException.class)
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
        member.awaitingJoinCallback(e -> {});
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
