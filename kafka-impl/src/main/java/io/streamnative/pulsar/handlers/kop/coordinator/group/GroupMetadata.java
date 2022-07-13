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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupState.Dead;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupState.PreparingRebalance;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.coordinator.group.MemberMetadata.MemberSummary;
import io.streamnative.pulsar.handlers.kop.exceptions.KoPTopicException;
import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.util.ArrayList;
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
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.common.schema.KeyValue;

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
@Slf4j
public class GroupMetadata {

    private static final Map<GroupState, Set<GroupState>> validPreviousStates = new HashMap<>();

    static {
        validPreviousStates.put(
            Dead,
            Sets.newHashSet(
                GroupState.Stable,
                PreparingRebalance,
                GroupState.CompletingRebalance,
                GroupState.Empty,
                Dead
            )
        );

        validPreviousStates.put(
            GroupState.CompletingRebalance,
            Sets.newHashSet(
                PreparingRebalance
            )
        );

        validPreviousStates.put(
            GroupState.Stable,
            Sets.newHashSet(
                GroupState.CompletingRebalance
            )
        );

        validPreviousStates.put(
            PreparingRebalance,
            Sets.newHashSet(
                GroupState.Stable,
                GroupState.CompletingRebalance,
                GroupState.Empty
            )
        );

        validPreviousStates.put(
            GroupState.Empty,
            Sets.newHashSet(
                PreparingRebalance
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
    public static class GroupOverview {
        private final String groupId;
        private final String protocolType;
    }

    /**
     * Class used to represent group metadata for the DescribeGroup API.
     */
    @Data
    public static class GroupSummary {
        private final String state;
        private final String protocolType;
        private final String protocol;
        private final List<MemberSummary> members;
    }

    /**
     * We cache offset commits along with their commit record offset. This enables us to ensure that the latest offset
     * commit is always materialized when we have a mix of transactional and regular offset commits. Without preserving
     * information of the commit record offset, compaction of the offsets topic it self may result in the wrong offset
     * commit being materialized.
     */
    @Data
    static class CommitRecordMetadataAndOffset {
        private final Optional<Long> appendedBatchOffset;
        private final OffsetAndMetadata offsetAndMetadata;

        public boolean olderThan(CommitRecordMetadataAndOffset that) {
            return appendedBatchOffset.get() < that.appendedBatchOffset.get();
        }
    }

    private final String groupId;
    @Getter
    private final ReentrantLock lock = new ReentrantLock();
    private GroupState state;

    private Optional<String> protocolType = Optional.empty();
    private int generationId = 0;
    private Optional<String> leaderId = Optional.empty();
    private Optional<String> protocol = Optional.empty();
    @Getter
    private boolean newMemberAdded = false;

    // state management
    private final Map<String, MemberMetadata> members =
        Collections.synchronizedMap(new HashMap<>());
    private final Map<TopicPartition, CommitRecordMetadataAndOffset> offsets =
        Collections.synchronizedMap(new HashMap<>());
    private final Map<TopicPartition, OffsetAndMetadata> pendingOffsetCommits =
        Collections.synchronizedMap(new HashMap<>());
    private final Map<Long, Map<TopicPartition, CommitRecordMetadataAndOffset>> pendingTransactionalOffsetCommits =
        Collections.synchronizedMap(new HashMap<>());
    private boolean receivedTransactionalOffsetCommits = false;
    private boolean receivedConsumerOffsetCommits = false;

    GroupMetadata(String groupId, GroupState initialState) {
        this.groupId = groupId;
        this.state = initialState;
    }

    public String generateMemberIdSuffix() {
        return UUID.randomUUID().toString();
    }

    public void newMemberAdded(boolean newMemberAdded) {
        this.newMemberAdded = newMemberAdded;
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

    public int generationId() {
        return generationId;
    }

    public Set<String> allMembers() {
        return members.keySet();
    }

    public List<MemberMetadata> allMemberMetadata() {
        return new ArrayList<>(members.values());
    }

    public int rebalanceTimeoutMs() {
        if (members.isEmpty()) {
            return 0;
        }
        return members.values().stream().mapToInt(MemberMetadata::rebalanceTimeoutMs).max().getAsInt();
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

    public MemberMetadata get(String memberId) {
        return members.get(memberId);
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
            .reduce((p1, p2) -> new HashSet<>(Sets.intersection(p1, p2)))
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

        checkArgument(groupId.equals(member.groupId()));
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

    public boolean canRebalance() {
        return validPreviousStates.get(PreparingRebalance).contains(state);
    }

    public void transitionTo(GroupState groupState) {
        assertValidTransition(groupState);
        state = groupState;
    }

    private void assertValidTransition(GroupState targetState) {
        if (!validPreviousStates.get(targetState).contains(state)) {
            throw new IllegalStateException(String.format(
                    "Group %s should be in the %s states before moving"
                            + " to %s state. Instead it is in %s state",
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

    public Map<String, byte[]> currentMemberMetadata() {
        if (is(Dead) || is(PreparingRebalance)) {
            throw new IllegalStateException("Cannot obtain member metadata for group in state " + state);
        }
        return members.entrySet().stream()
            .collect(Collectors.toMap(
                    Entry::getKey,
                e -> e.getValue().metadata(protocol.get())
            ));
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
                .map(MemberMetadata::summaryNoMetadata)
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

    public void initializeOffsets(Map<TopicPartition, CommitRecordMetadataAndOffset> offsets,
                                  Map<Long, Map<TopicPartition, CommitRecordMetadataAndOffset>> pendingTxnOffsets) {
        this.offsets.putAll(offsets);
        this.pendingTransactionalOffsetCommits.putAll(pendingTxnOffsets);
    }

    public void onOffsetCommitAppend(TopicPartition topicPartition,
                                     CommitRecordMetadataAndOffset offsetWithCommitRecordMetadata) {
        if (pendingOffsetCommits.containsKey(topicPartition)) {
            if (!offsetWithCommitRecordMetadata.appendedBatchOffset.isPresent()) {
                throw new IllegalStateException("Cannot complete offset commit write without providing the metadata"
                    + " of the record in the log.");
            }
            if (!offsets.containsKey(topicPartition)
                || offsets.get(topicPartition).olderThan(offsetWithCommitRecordMetadata)) {
                offsets.put(topicPartition, offsetWithCommitRecordMetadata);
            }
        }

        OffsetAndMetadata stagedOffset = pendingOffsetCommits.get(topicPartition);
        if (offsetWithCommitRecordMetadata.offsetAndMetadata.equals(stagedOffset)) {
            pendingOffsetCommits.remove(topicPartition);
        } else {
            // The pendingOffsetCommits for this partition could be empty if the topic was deleted, in which case
            // its entries would be removed from the cache by the `removeOffsets` method.
        }
    }

    public void failPendingOffsetWrite(TopicPartition topicPartition,
                                       OffsetAndMetadata offset) {
        OffsetAndMetadata pendingOffset = pendingOffsetCommits.get(topicPartition);
        if (offset.equals(pendingOffset)) {
            pendingOffsetCommits.remove(topicPartition);
        }
    }

    public void prepareOffsetCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        receivedConsumerOffsetCommits = true;
        pendingOffsetCommits.putAll(offsets);
    }

    public void prepareTxnOffsetCommit(long producerId,
                                       Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (log.isTraceEnabled()) {
            log.trace("TxnOffsetCommit for producer {} and group {} with offsets {} is pending",
                producerId, groupId, offsets);
        }
        receivedTransactionalOffsetCommits = true;
        Map<TopicPartition, CommitRecordMetadataAndOffset> producerOffsets =
            pendingTransactionalOffsetCommits.computeIfAbsent(producerId, pid -> new HashMap<>());
        offsets.forEach((tp, offsetsAndMetadata) -> producerOffsets.put(tp, new CommitRecordMetadataAndOffset(
            Optional.empty(),
            offsetsAndMetadata
        )));
    }

    public boolean hasReceivedConsistentOffsetCommits() {
        return !receivedConsumerOffsetCommits || !receivedTransactionalOffsetCommits;
    }

    /**
     * Remove a pending transactional offset commit if the actual offset commit record was not written to the log.
     * We will return an error and the client will retry the request, potentially to a different coordinator.
     */
    public void failPendingTxnOffsetCommit(long producerId,
                                           TopicPartition topicPartition) {
        Map<TopicPartition, CommitRecordMetadataAndOffset> pendingOffsets =
            pendingTransactionalOffsetCommits.get(producerId);
        if (null != pendingOffsets) {
            CommitRecordMetadataAndOffset pendingOffsetCommit = pendingOffsets.remove(topicPartition);
            if (log.isTraceEnabled()) {
                log.trace("TxnOffsetCommit for producer {} and group {} with offsets {} failed to be appended"
                        + " to the log",
                    producerId, groupId, pendingOffsetCommit);
            }
            if (pendingOffsets.isEmpty()) {
                pendingTransactionalOffsetCommits.remove(producerId);
            }
        } else {
            // We may hit this case if the partition in question has emigrated already.
        }
    }

    public void onTxnOffsetCommitAppend(long producerId,
                                        TopicPartition topicPartition,
                                        CommitRecordMetadataAndOffset commitRecordMetadataAndOffset) {
        Map<TopicPartition, CommitRecordMetadataAndOffset> pendingOffsets =
            pendingTransactionalOffsetCommits.get(producerId);
        if (null != pendingOffsets) {
            if (pendingOffsets.containsKey(topicPartition)
                    && pendingOffsets.get(topicPartition).offsetAndMetadata().equals(
                    commitRecordMetadataAndOffset.offsetAndMetadata)) {
                pendingOffsets.put(topicPartition, commitRecordMetadataAndOffset);
            }
        } else {
            // We may hit this case if the partition in question has emigrated.
        }
    }

    /**
     * Complete a pending transactional offset commit. This is called after a commit or abort marker is fully written
     * to the log.
     */
    public void completePendingTxnOffsetCommit(long producerId,
                                               boolean isCommit) {
        Map<TopicPartition, CommitRecordMetadataAndOffset> pendingOffsets =
            pendingTransactionalOffsetCommits.remove(producerId);
        if (isCommit) {
            if (null != pendingOffsets) {
                pendingOffsets.forEach((topicPartition, commitRecordMetadataAndOffset) -> {
                    if (!commitRecordMetadataAndOffset.appendedBatchOffset.isPresent()) {
                        throw new IllegalStateException(String.format("Trying to complete a transactional offset"
                                + " commit for producerId %s and groupId %s even though the offset commit record"
                                + " itself hasn't been appended to the log.", producerId, groupId));
                    }

                    CommitRecordMetadataAndOffset currentOffsetOpt = offsets.get(topicPartition);
                    if (currentOffsetOpt == null || currentOffsetOpt.olderThan(commitRecordMetadataAndOffset)) {
                        if (log.isTraceEnabled()) {
                            log.trace("TxnOffsetCommit for producer {} and group {} with offset {} "
                                            + "committed and loaded into the cache.",
                                    producerId, groupId, commitRecordMetadataAndOffset);
                        }
                        offsets.put(topicPartition, commitRecordMetadataAndOffset);
                    } else {
                        if (log.isTraceEnabled()) {
                            log.trace("TxnOffsetCommit for producer {} and group {} with offset {} "
                                            + "committed, but not loaded since its offset is older than current offset"
                                            + " {}.",
                                    producerId, groupId, commitRecordMetadataAndOffset, currentOffsetOpt);
                        }
                    }
                });
            }
        } else {
            if (log.isTraceEnabled()) {
                log.trace("TxnOffsetCommit for producer {} and group {} with offsets {} aborted",
                    producerId, groupId, pendingOffsets);
            }
        }
    }

    public Set<Long> activeProducers() {
        return pendingTransactionalOffsetCommits.keySet();
    }

    public boolean hasPendingOffsetCommitsFromProducer(long producerId) {
        return pendingTransactionalOffsetCommits.containsKey(producerId);
    }

    public Map<TopicPartition, OffsetAndMetadata> removeAllOffsets() {
        return removeOffsets(new HashSet<>(offsets.keySet()).stream());
    }

    public Map<TopicPartition, OffsetAndMetadata> removeOffsets(Stream<TopicPartition> topicPartitions) {
        return topicPartitions.map(topicPartition -> {
            pendingOffsetCommits.remove(topicPartition);
            pendingTransactionalOffsetCommits.forEach((pid, pendingOffsets) -> pendingOffsets.remove(topicPartition));
            CommitRecordMetadataAndOffset removedOffset = offsets.remove(topicPartition);
            // removedOffset.offsetAndMetadata() have an NPE
            if (removedOffset == null) {
                return new KeyValue<>(
                        topicPartition,
                        OffsetAndMetadata.apply(0)
                );
            }
            return new KeyValue<>(
                topicPartition,
                removedOffset.offsetAndMetadata()
            );
        }).collect(Collectors.toMap(
                KeyValue::getKey,
                KeyValue::getValue
        ));
    }

    public List<TopicPartition> collectPartitionsWithTopics(Set<String> topics) {
        ArrayList<TopicPartition> topicPartitions = Lists.newArrayList();

        topicPartitions.addAll(pendingOffsetCommits.keySet().stream().filter(
                topicPartition -> topics.contains(topicPartition.topic())
        ).collect(Collectors.toList()));

        pendingTransactionalOffsetCommits.values().stream().map(Map::keySet)
                .collect(Collectors.toList())
                .forEach(partitionSet -> topicPartitions.addAll(partitionSet.stream().filter(
                                topicPartition -> topics.contains(topicPartition.topic()))
                        .collect(Collectors.toList())));

        topicPartitions.addAll(offsets.keySet().stream().filter(
                topicPartition -> topics.contains(topicPartition.topic())
        ).collect(Collectors.toList()));
        return topicPartitions;
    }

    public Map<TopicPartition, OffsetAndMetadata> removeExpiredOffsets(long startMs) {
        Map<TopicPartition, OffsetAndMetadata> expiredOffsets = offsets.entrySet().stream()
            .filter(e ->
                e.getValue().offsetAndMetadata().expireTimestamp() < startMs
                    && !pendingOffsetCommits.containsKey(e.getKey()))
            .map(e -> new KeyValue<>(
                e.getKey(),
                e.getValue().offsetAndMetadata()
            ))
            .collect(Collectors.toMap(
                    KeyValue::getKey,
                    KeyValue::getValue
            ));

        expiredOffsets.keySet().forEach(offsets::remove);
        return expiredOffsets;
    }

    public Map<TopicPartition, OffsetAndMetadata> allOffsets() {
        return offsets.entrySet().stream().collect(Collectors.toMap(
                Entry::getKey,
            e -> e.getValue().offsetAndMetadata()
        ));
    }

    public Optional<OffsetAndMetadata> offset(TopicPartition topicPartition, String namespacePrefix) {
        return Optional
                .ofNullable(offsets.computeIfAbsent(
                        topicPartition,
                        tp -> {
                            // Some test cases may use the original topic name to read the offset
                            // directly from this method, so we need to ensure that all the topics
                            // has chances to be converted when it is missing
                            try {
                                return offsets.get(new TopicPartition(
                                        new KopTopic(tp.topic(), namespacePrefix).getFullName(), tp.partition()));
                            } catch (KoPTopicException e) {
                                // In theory, this place will not be executed
                                log.warn("Invalid topic name: {}", tp.topic(), e);
                                return null;
                            }
                        }))
                .map(e -> e.offsetAndMetadata);
    }

    @VisibleForTesting
    Optional<CommitRecordMetadataAndOffset> offsetWithRecordMetadata(TopicPartition topicPartition) {
        return Optional.ofNullable(offsets.get(topicPartition));
    }

    public int numOffsets() {
        return offsets.size();
    }

    @VisibleForTesting
    int numPendingOffsetCommits() {
        return pendingOffsetCommits.size();
    }

    public boolean hasOffsets() {
        return !offsets.isEmpty()
            || !pendingOffsetCommits.isEmpty()
            || !pendingTransactionalOffsetCommits.isEmpty();
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
