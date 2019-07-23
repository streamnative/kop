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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Supplier;
import com.google.common.collect.Sets;
import io.streamnative.kop.coordinator.group.MemberMetadata.MemberSummary;
import io.streamnative.kop.utils.CoreUtils;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Data;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

/**
 * Group contains the following metadata:
 *
 * <p>Membership metadata:
 *  1. Members registered in this group
 *  2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
 *  3. Protocol metadata associated with group members
 *
 * <p>State metadata:
 *  1. group state
 *  2. generation id
 *  3. leader id
 */
@NotThreadSafe
@Setter
@Accessors(fluent = true)
class GroupMetadata {

    private static final Map<GroupState, Set<GroupState>> validPreviousStates = new HashMap<>();

    static {
        validPreviousStates.put(
            GroupState.Dead,
            Sets.newHashSet(
                GroupState.Stable,
                GroupState.PreparingRebalance,
                GroupState.CompletingRebalance,
                GroupState.Empty,
                GroupState.Dead
            )
        );

        validPreviousStates.put(
            GroupState.CompletingRebalance,
            Sets.newHashSet(
                GroupState.PreparingRebalance
            )
        );

        validPreviousStates.put(
            GroupState.Stable,
            Sets.newHashSet(
                GroupState.CompletingRebalance
            )
        );

        validPreviousStates.put(
            GroupState.PreparingRebalance,
            Sets.newHashSet(
                GroupState.Stable,
                GroupState.CompletingRebalance,
                GroupState.Empty
            )
        );

        validPreviousStates.put(
            GroupState.Empty,
            Sets.newHashSet(
                GroupState.PreparingRebalance
            )
        );
    }

    public static GroupMetadata loadGroup(
        String groupId,
        GroupState initialState,
        int generationId,
        String protocolType,
        String protocol,
        String leaderId,
        Iterable<MemberMetadata> members
    ) {
        GroupMetadata metadata = new GroupMetadata(groupId, initialState)
            .generationId(generationId)
            .protocolType(
                StringUtils.isEmpty(protocolType) ? Optional.empty() : Optional.of(protocolType)
            )
            .protocol(Optional.ofNullable(protocol))
            .leaderId(Optional.ofNullable(leaderId));
        members.forEach(metadata::add);
        return metadata;
    }

    /**
     * Class used to represent group metadata for the ListGroups API.
     */
    @Data
    static class GroupOverview {
        private final String groupId;
        private final String protocolType;
    }

    /**
     * Class used to represent group metadata for the DescribeGroup API.
     */
    @Data
    static class GroupSummary {
        private final String state;
        private final String protocolType;
        private final String protocol;
        private final List<MemberSummary> members;
    }

    private final String groupId;
    private final ReentrantLock lock = new ReentrantLock();
    private GroupState state;

    private Optional<String> protocolType = Optional.empty();
    private long generationId = 0L;
    private Optional<String> leaderId = Optional.empty();
    private Optional<String> protocol = Optional.empty();

    // state management
    private final Map<String, MemberMetadata> members = new HashMap<>();

    GroupMetadata(String groupId, GroupState initialState) {
        this.groupId = groupId;
        this.state = initialState;
    }

    public <T> T inLock(Supplier<T> supplier) {
        return CoreUtils.inLock(lock, supplier);
    }

    public Optional<String> protocolType() {
        return protocolType;
    }

    public GroupState currentState() {
        return state;
    }

    public String groupId() {
        return groupId;
    }

    public long generationId() {
        return generationId;
    }

    public List<MemberMetadata> allMemberMetadata() {
        return members.values().stream().collect(Collectors.toList());
    }

    public boolean is(GroupState groupState) {
        return state == groupState;
    }

    public boolean not(GroupState groupState) {
        return state != groupState;
    }

    public boolean has(String memberId) {
        return members.containsKey(memberId);
    }

    public boolean isLeader(String memberId) {
        return Objects.equals(leaderId.orElse(null), memberId);
    }

    public String leaderOrNull() {
        return leaderId.orElse(null);
    }

    public String protocolOrNull() {
        return protocol.orElse(null);
    }

    public List<MemberMetadata> notYetRejoinedMembers() {
        return members.values()
            .stream()
            .filter(e -> e.awaitingJoinCallback() == null)
            .collect(Collectors.toList());
    }

    private Set<String> candidateProtocols() {
        return members.values().stream()
            .map(MemberMetadata::protocols)
            .reduce((p1, p2) -> {
                Set<String> newProtocols = new HashSet<>();
                newProtocols.addAll(Sets.intersection(p1, p2));
                return newProtocols;
            })
            .orElse(Collections.emptySet());
    }

    public boolean supportsProtocols(Set<String> memberProtocols) {
        return members.isEmpty()
            || !Sets.intersection(memberProtocols, candidateProtocols()).isEmpty();
    }

    public void initNextGeneration() {
        checkArgument(notYetRejoinedMembers().isEmpty());
        if (!members.isEmpty()) {
            generationId += 1;
            protocol = Optional.ofNullable(selectProtocol());
            transitionTo(GroupState.CompletingRebalance);
        } else {
            generationId += 1;
            protocol = Optional.empty();
            transitionTo(GroupState.Empty);
        }
    }

    public void add(MemberMetadata member) {
        if (members.isEmpty()) {
            this.protocolType = Optional.of(member.protocolType());
        }

        checkArgument(groupId == member.groupId());
        checkArgument(Objects.equals(protocolType.orElse(null), member.protocolType()));
        checkArgument(supportsProtocols(member.protocols()));

        if (!leaderId.isPresent()) {
            leaderId = Optional.of(member.memberId());
        }

        members.put(member.memberId(), member);
    }

    public void remove(String memberId) {
        members.remove(memberId);
        if (isLeader(memberId)) {
            if (members.isEmpty()) {
                leaderId = Optional.empty();
            } else {
                leaderId = members.keySet().stream().findFirst();
            }
        }
    }

    public boolean canReblance() {
        return validPreviousStates.get(GroupState.PreparingRebalance).contains(state);
    }

    public void transitionTo(GroupState groupState) {
        assertValidTransition(groupState);
        state = groupState;
    }

    private void assertValidTransition(GroupState targetState) {
        if (!validPreviousStates.get(targetState).contains(state)) {
            throw new IllegalStateException(("Group %s should be in the %s states before moving"
                + " to %s state. Instead it is in %s state"
            ).format(
                groupId,
                StringUtils.join(validPreviousStates.get(targetState), ","),
                targetState,
                state));
        }
    }

    public String selectProtocol() {
        checkState(
            !members.isEmpty(),
            "Cannot select protocol for empty group");

        Set<String> candidates = candidateProtocols();

        return members.values().stream()
            .map(m -> m.vote(candidates))
            .collect(Collectors.groupingBy(protocol -> protocol))
            .entrySet()
            .stream()
            .max(Comparator.comparingInt(o -> o.getValue().size()))
            .map(Entry::getKey)
            .orElse(null);
    }

    public GroupSummary summary() {
        if (is(GroupState.Stable)) {
            String protocol = protocolOrNull();
            checkState(
                protocol != null,
                "Invalid null group protocol for stable group");

            List<MemberSummary> summaries = members.values()
                .stream()
                .map(member -> member.summary(protocol))
                .collect(Collectors.toList());

            return new GroupSummary(
                state.toString(),
                protocolType.orElse(""),
                protocol,
                summaries
            );
        } else {
            List<MemberSummary> summaries = members.values()
                .stream()
                .map(member -> member.summaryNoMetadata())
                .collect(Collectors.toList());

            return new GroupSummary(
                state.toString(),
                protocolType.orElse(""),
                GroupCoordinator.NoProtocol,
                summaries
            );
        }
    }

    public GroupOverview overview() {
        return new GroupOverview(
            groupId,
            protocolType.orElse("")
        );
    }

    @Override
    public String toString() {
        ToStringHelper helper = MoreObjects.toStringHelper("GroupMetadata")
            .add("groupId", groupId)
            .add("generation", generationId)
            .add("protocolType", protocolType)
            .add("state", state)
            .add("members", members);
        return helper.toString();
    }

}
