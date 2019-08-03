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

import static com.google.common.base.Preconditions.checkState;
import static io.streamnative.kop.coordinator.group.GroupState.CompletingRebalance;
import static io.streamnative.kop.coordinator.group.GroupState.Dead;
import static io.streamnative.kop.coordinator.group.GroupState.Empty;
import static io.streamnative.kop.coordinator.group.GroupState.PreparingRebalance;
import static io.streamnative.kop.coordinator.group.GroupState.Stable;

import com.google.common.collect.Sets;
import io.streamnative.kop.coordinator.group.GroupMetadata.GroupOverview;
import io.streamnative.kop.coordinator.group.GroupMetadata.GroupSummary;
import io.streamnative.kop.utils.CoreUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.util.FutureUtil;
import scala.Array;

/**
 * Group coordinator.
 */
@Slf4j
public class GroupCoordinator {

    static final String NoState = "";
    static final String NoProtocolType = "";
    static final String NoProtocol = "";
    static final String NoLeader = "";
    static final int NoGeneration = -1;
    static final String NoMemberId = "";
    static final GroupSummary EmptyGroup = new GroupSummary(
        NoState,
        NoProtocolType,
        NoProtocol,
        Collections.emptyList()
    );
    static final GroupSummary DeadGroup = new GroupSummary(
        Dead.toString(),
        NoProtocolType,
        NoProtocol,
        Collections.emptyList()
    );

    private static boolean isValidGroupId(String groupId,
                                          ApiKeys api) {
        switch (api) {
            case OFFSET_COMMIT:
            case OFFSET_FETCH:
            case DESCRIBE_GROUPS:
            case DELETE_GROUPS:
                return null != groupId;
            default:
                return null != groupId && !groupId.isEmpty();
        }
    }

    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final GroupConfig groupConfig;
    private final GroupMetadataManager groupManager;
    private final Time time;

    public CompletableFuture<JoinGroupResult> handleJoinGroup(
        String groupId,
        String memberId,
        String clientId,
        String clientHost,
        int rebalanceTimeoutMs,
        int sessionTimeoutMs,
        String protocolType,
        Map<String, byte[]> protocols
    ) {
        Optional<Errors> errors = validateGroupStatus(groupId, ApiKeys.JOIN_GROUP);
        if (errors.isPresent()) {
            return CompletableFuture.completedFuture(
                joinError(memberId, errors.get()));
        }

        if (sessionTimeoutMs < groupConfig.getGroupMinSessionTimeoutMs()
            || sessionTimeoutMs > groupConfig.getGroupMaxSessionTimeoutMs()) {
            return CompletableFuture.completedFuture(
                joinError(memberId, Errors.INVALID_SESSION_TIMEOUT));
        } else {
            return groupManager.getGroup(groupId).map(group -> doJoinGroup(
                group,
                memberId,
                clientId,
                clientHost,
                rebalanceTimeoutMs,
                sessionTimeoutMs,
                protocolType,
                protocols
            )).orElseGet(() -> {
                if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) {
                    return CompletableFuture.completedFuture(
                        joinError(memberId, Errors.UNKNOWN_MEMBER_ID));
                } else {
                    GroupMetadata group = groupManager.addGroup(new GroupMetadata(
                        groupId, Empty
                    ));
                    return doJoinGroup(
                        group,
                        memberId,
                        clientId,
                        clientHost,
                        rebalanceTimeoutMs,
                        sessionTimeoutMs,
                        protocolType,
                        protocols
                    );
                }
            });
        }
    }

    private CompletableFuture<JoinGroupResult> doJoinGroup(
        GroupMetadata group,
        String memberId,
        String clientId,
        String clientHost,
        int rebalanceTimeoutMs,
        int sessionTimeoutMs,
        String protocolType,
        Map<String, byte[]> protocols
    ) {
        return group.inLock(() -> unsafeJoinGroup(
            group,
            memberId,
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols
        ));
    }

    private CompletableFuture<JoinGroupResult> unsafeJoinGroup(
        GroupMetadata group,
        String memberId,
        String clientId,
        String clientHost,
        int rebalanceTimeoutMs,
        int sessionTimeoutMs,
        String protocolType,
        Map<String, byte[]> protocols
    ) {
        if (!group.is(Empty) && (
            !group.protocolType().isPresent()
                || group.protocolType().get() != protocolType
                || !group.supportsProtocols(protocols.keySet()))) {
            // if the new member does not support the group protocol, reject it
            return CompletableFuture.completedFuture(
                joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL));
        } else if (group.is(Empty)
            && (protocols.isEmpty() || protocolType.isEmpty())) {
            //reject if first member with empty group protocol or protocolType is empty
            return CompletableFuture.completedFuture(
                joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL));
        } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
            // if the member trying to register with a un-recognized id, send the response to let
            // it reset its member id and retry
            return CompletableFuture.completedFuture(
                joinError(memberId, Errors.UNKNOWN_MEMBER_ID));
        } else {
            CompletableFuture<JoinGroupResult> resultFuture;
            switch (group.currentState()) {
                case Dead:
                    // if the group is marked as dead, it means some other thread has just removed the group
                    // from the coordinator metadata; this is likely that the group has migrated to some other
                    // coordinator OR the group is in a transient unstable phase. Let the member retry
                    // joining without the specified member id,
                    resultFuture = CompletableFuture.completedFuture(
                        joinError(memberId, Errors.UNKNOWN_MEMBER_ID));
                    break;
                case PreparingRebalance:
                    if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
                        resultFuture = addMemberAndRebalance(
                            rebalanceTimeoutMs,
                            sessionTimeoutMs,
                            clientId,
                            clientHost,
                            protocolType,
                            protocols,
                            group
                        );
                    } else {
                        MemberMetadata member = group.get(memberId);
                        resultFuture = updateMemberAndRebalance(group, member, protocols);
                    }
                    break;
                case CompletingRebalance:
                    if (JoinGroupRequest.UNKNOWN_MEMBER_ID == memberId) {
                        resultFuture = addMemberAndRebalance(
                            rebalanceTimeoutMs,
                            sessionTimeoutMs,
                            clientId,
                            clientHost,
                            protocolType,
                            protocols,
                            group
                        );
                    } else {
                        MemberMetadata member = group.get(memberId);
                        if (member.matches(protocols)) {
                            // member is joining with the same metadata (which could be because it failed to
                            // receive the initial JoinGroup response), so just return current group information
                            // for the current generation.
                            Map<String, byte[]> members;
                            if (group.isLeader(memberId)) {
                                members = group.currentMemberMetadata();
                            } else {
                                members = Collections.emptyMap();
                            }
                            resultFuture = CompletableFuture.completedFuture(
                                new JoinGroupResult(
                                    members,
                                    memberId,
                                    (int) group.generationId(),
                                    group.protocolOrNull(),
                                    group.leaderOrNull(),
                                    Errors.NONE
                                )
                            );
                        } else {
                            resultFuture = updateMemberAndRebalance(
                                group,
                                member,
                                protocols
                            );
                        }
                    }
                    break;
                case Empty:
                case Stable:
                    if (JoinGroupRequest.UNKNOWN_MEMBER_ID == memberId) {
                        // if the member id is unknown, register the member to the group
                        resultFuture = addMemberAndRebalance(
                            rebalanceTimeoutMs,
                            sessionTimeoutMs,
                            clientId,
                            clientHost,
                            protocolType,
                            protocols,
                            group
                        );
                    } else {
                        MemberMetadata member = group.get(memberId);
                        if (group.isLeader(memberId) || !member.matches(protocols)) {
                            // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                            // The latter allows the leader to trigger rebalances for changes affecting assignment
                            // which do not affect the member metadata (such as topic metadata changes for the consumer)
                            resultFuture = updateMemberAndRebalance(group, member, protocols);
                        } else {
                            // for followers with no actual change to their metadata, just return group information
                            // for the current generation which will allow them to issue SyncGroup
                            resultFuture = CompletableFuture.completedFuture(new JoinGroupResult(
                                Collections.emptyMap(),
                                memberId,
                                (int) group.generationId(),
                                group.protocolOrNull(),
                                group.leaderOrNull(),
                                Errors.NONE));
                        }
                    }
                    break;
                default:
                    resultFuture = FutureUtil.failedFuture(
                        new IllegalStateException("Unknown state " + group.currentState()));
                    break;
            }
            if (group.is(PreparingRebalance)) {
                // TODO: check and trigger rebalance
            }
            return resultFuture;
        }

    }

    public void handleSyncGroup(String groupId,
                                int generation,
                                String memberId,
                                Map<String, byte[]> groupAssignment,
                                BiConsumer<byte[], Errors> responseCallback) {
        Optional<Errors> errorsOpt = validateGroupStatus(groupId, ApiKeys.SYNC_GROUP);
        if (errorsOpt.isPresent()) {
            Errors error = errorsOpt.get();
            if (Errors.COORDINATOR_LOAD_IN_PROGRESS == error) {
                // The coordinator is loading, which means we've lost the state of the active rebalance and the
                // group will need to start over at JoinGroup. By returning rebalance in progress, the consumer
                // will attempt to rejoin without needing to rediscover the coordinator. Note that we cannot
                // return COORDINATOR_LOAD_IN_PROGRESS since older clients do not expect the error.
                responseCallback.accept(new byte[0], Errors.REBALANCE_IN_PROGRESS);
            } else {
                responseCallback.accept(new byte[0], error);
            }
        } else {
            Optional<GroupMetadata> groupOpt = groupManager.getGroup(groupId);
            if (groupOpt.isPresent()) {
                doSyncGroup(
                    groupOpt.get(),
                    generation,
                    memberId,
                    groupAssignment,
                    responseCallback
                );
            } else {
                responseCallback.accept(new byte[0], Errors.UNKNOWN_MEMBER_ID);
            }
        }
    }

    private void doSyncGroup(GroupMetadata group,
                             int generationId,
                             String memberId,
                             final Map<String, byte[]> groupAssignment,
                             BiConsumer<byte[], Errors> responseCallback) {
        group.inLock(() -> {
            if (!group.has(memberId)) {
                responseCallback.accept(new byte[0], Errors.UNKNOWN_MEMBER_ID);
            } else if (generationId != group.generationId()) {
                responseCallback.accept(new byte[0], Errors.ILLEGAL_GENERATION);
            } else {
                switch (group.currentState()) {
                    case Empty:
                    case Dead:
                        responseCallback.accept(new byte[0], Errors.UNKNOWN_MEMBER_ID);
                        break;

                    case PreparingRebalance:
                        responseCallback.accept(new byte[0], Errors.REBALANCE_IN_PROGRESS);
                        break;

                    case CompletingRebalance:
                        group.get(memberId).awaitingSyncCallback(responseCallback);

                        // if this is the leader, then we can attempt to persist state and transition to stable
                        if (group.isLeader(memberId)) {
                            log.info("Assignment received from leader for group {} for generation {}",
                                group.groupId(), group.generationId());

                            // fill any missing members with an empty assignment
                            Set<String> missing = Sets.difference(group.allMembers(), groupAssignment.keySet());
                            Map<String, byte[]> assignment = new HashMap<>();
                            assignment.putAll(groupAssignment);
                            assignment.putAll(
                                missing.stream()
                                    .collect(Collectors.toMap(
                                        k -> k,
                                        k -> new byte[0]
                                    ))
                            );

                            groupManager.storeGroup(group, assignment).thenApply(error -> {
                                return group.inLock(() -> {
                                    // another member may have joined the group while we were awaiting this callback,
                                    // so we must ensure we are still in the CompletingRebalance state and the same
                                    // generation when it gets invoked. if we have transitioned to another state,
                                    // then do nothing
                                    if (group.is(CompletingRebalance) && generationId == group.generationId()) {
                                        if (error != Errors.NONE) {
                                            resetAndPropagateAssignmentError(group, error);
                                            maybePrepareRebalance(group);
                                        } else {
                                            setAndPropagateAssignment(group, assignment);
                                            group.transitionTo(Stable);
                                        }
                                    }
                                    return null;
                                });
                            });
                        }
                        break;

                    case Stable:
                        // if the group is stable, we just return the current assignment
                        MemberMetadata memberMetadata = group.get(memberId);
                        responseCallback.accept(memberMetadata.assignment(), Errors.NONE);
                        completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId));
                        break;

                    default:
                        throw new IllegalStateException("Should not reach here");
                }
            }
            return null;
        });
    }

    public CompletableFuture<Errors> handleLeaveGroup(
        String groupId,
        String memberId
    ) {
        return validateGroupStatus(groupId, ApiKeys.LEAVE_GROUP).map(error ->
            CompletableFuture.completedFuture(error)
        ).orElseGet(() -> {
            return groupManager.getGroup(groupId).map(group -> {
                return group.inLock(() -> {
                    if (group.is(Dead) || !group.has(memberId)) {
                        return CompletableFuture.completedFuture(Errors.UNKNOWN_MEMBER_ID);
                    } else {
                        MemberMetadata member = group.get(memberId);
                        removeHeartbeatForLeavingMember(group, member);
                        if (log.isDebugEnabled()) {
                            log.debug("Member {} in group {} has left, removing it from the group",
                                member.memberId(), group.groupId());
                        }
                        removeMemberAndUpdateGroup(group, member);
                        return CompletableFuture.completedFuture(Errors.NONE);
                    }
                });
            }).orElseGet(() -> {
                // if the group is marked as dead, it means some other thread has just removed the group
                // from the coordinator metadata; this is likely that the group has migrated to some other
                // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
                // joining without specified consumer id,
                return CompletableFuture.completedFuture(Errors.UNKNOWN_MEMBER_ID);
            });
        });
    }

    public Map<String, Errors> handleDeleteGroups(Set<String> groupIds) {
        Map<String, Errors> groupErrors = new HashMap<>();
        List<GroupMetadata> groupsEligibleForDeletion = new ArrayList<>();

        groupIds.forEach(groupId -> {
            validateGroupStatus(groupId, ApiKeys.DELETE_GROUPS).map(error ->
                groupErrors.put(groupId, error)
            ).orElseGet(() -> groupManager.getGroup(groupId).map(group -> {
                return group.inLock(() -> {
                    switch(group.currentState()) {
                        case Dead:
                            if (groupManager.groupNotExists(groupId)) {
                                groupErrors.put(groupId, Errors.GROUP_ID_NOT_FOUND);
                            } else {
                                groupErrors.put(groupId, Errors.NOT_COORDINATOR);
                            }
                            break;
                        case Empty:
                            group.transitionTo(Dead);
                            groupsEligibleForDeletion.add(group);
                            break;
                        default:
                            groupErrors.put(groupId, Errors.NON_EMPTY_GROUP);
                            break;
                    }
                    return Errors.NONE;
                });
            }).orElseGet(() -> {
                Errors error;
                if (groupManager.groupNotExists(groupId)) {
                    error = Errors.GROUP_ID_NOT_FOUND;
                } else {
                    error = Errors.NOT_COORDINATOR;
                }
                groupErrors.put(groupId, error);
                return Errors.NONE;
            }));
        });

        if (!groupsEligibleForDeletion.isEmpty()) {
            // TODO:
            /// val offsetsRemoved = groupManager.cleanupGroupMetadata(groupsEligibleForDeletion, _.removeAllOffsets())
            /// groupErrors ++= groupsEligibleForDeletion.map(_.groupId -> Errors.NONE).toMap
            /// info(s"The following groups were deleted: ${groupsEligibleForDeletion.map(_.groupId).mkString(", ")}. " +
            ///     s"A total of $offsetsRemoved offsets were removed.")
        }

        return groupErrors;
    }

    public CompletableFuture<Errors> handleHeartbeat(String groupId,
                                                     String memberId,
                                                     int generationId) {
        return validateGroupStatus(groupId, ApiKeys.HEARTBEAT).map(error -> {
            if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                // the group is still loading, so respond just blindly
                return CompletableFuture.completedFuture(Errors.NONE);
            } else {
                return CompletableFuture.completedFuture(error);
            }
        }).orElseGet(() -> groupManager.getGroup(groupId).map(group ->
            group.inLock(() -> {
                switch(group.currentState()) {
                    case Dead:
                        // if the group is marked as dead, it means some other thread has just removed the group
                        // from the coordinator metadata; this is likely that the group has migrated to some other
                        // coordinator OR the group is in a transient unstable phase. Let the member retry
                        // joining without the specified member id,
                        return CompletableFuture.completedFuture(Errors.UNKNOWN_MEMBER_ID);

                    case Empty:
                        return CompletableFuture.completedFuture(Errors.UNKNOWN_MEMBER_ID);

                    case CompletingRebalance:
                        if (!group.has(memberId)) {
                            return CompletableFuture.completedFuture(Errors.UNKNOWN_MEMBER_ID);
                        } else {
                            return CompletableFuture.completedFuture(Errors.REBALANCE_IN_PROGRESS);
                        }

                    case PreparingRebalance:
                        if (!group.has(memberId)) {
                            return CompletableFuture.completedFuture(Errors.UNKNOWN_MEMBER_ID);
                        } else if (generationId != group.generationId()) {
                            return CompletableFuture.completedFuture(Errors.ILLEGAL_GENERATION);
                        } else {
                            MemberMetadata member = group.get(memberId);
                            completeAndScheduleNextHeartbeatExpiration(group, member);
                            return CompletableFuture.completedFuture(Errors.REBALANCE_IN_PROGRESS);
                        }

                    case Stable:
                        if (!group.has(memberId)) {
                            return CompletableFuture.completedFuture(Errors.UNKNOWN_MEMBER_ID);
                        } else if (generationId != group.generationId()) {
                            return CompletableFuture.completedFuture(Errors.ILLEGAL_GENERATION);
                        } else {
                            MemberMetadata member = group.get(memberId);
                            completeAndScheduleNextHeartbeatExpiration(group, member);
                            return CompletableFuture.completedFuture(Errors.NONE);
                        }

                    default:
                        return CompletableFuture.completedFuture(Errors.NONE);
                }
            })
        ).orElseGet(() ->
            CompletableFuture.completedFuture(Errors.UNKNOWN_MEMBER_ID)
        ));
    }

    KeyValue<Errors, List<GroupOverview>> handleListGroups() {
        if (!isActive.get()) {
            return new KeyValue<>(Errors.COORDINATOR_NOT_AVAILABLE, new ArrayList<>());
        } else {
            Errors errors;
            if (groupManager.isLoading()) {
                errors = Errors.COORDINATOR_LOAD_IN_PROGRESS;
            } else {
                errors = Errors.NONE;
            }
            List<GroupOverview> overviews = new ArrayList<>();
            groupManager.currentGroups().forEach(group -> overviews.add(group.overview()));
            return new KeyValue<>(
                errors,
                overviews
            );
        }
    }

    KeyValue<Errors, GroupSummary> handleDescribeGroup(String groupId) {
        return validateGroupStatus(groupId, ApiKeys.DESCRIBE_GROUPS).map(error ->
            new KeyValue<>(error, GroupCoordinator.EmptyGroup)
        ).orElseGet(() ->
            groupManager.getGroup(groupId)
                .map(group ->
                    group.inLock(() -> new KeyValue(Errors.NONE, group.summary())
                    ))
                .orElseGet(() -> new KeyValue<>(Errors.NONE, GroupCoordinator.DeadGroup))
        );
    }

    private Optional<Errors> validateGroupStatus(String groupId,
                                                 ApiKeys api) {
        if (isValidGroupId(groupId, api)) {
            return Optional.of(Errors.INVALID_GROUP_ID);
        } else if (isActive.get()) {
            return Optional.of(Errors.COORDINATOR_NOT_AVAILABLE);
        } else if (groupManager.isGroupLoading(groupId)) {
            return Optional.of(Errors.COORDINATOR_LOAD_IN_PROGRESS);
        } else if (groupManager.isGroupLocal(groupId)) {
            return Optional.of(Errors.NOT_COORDINATOR);
        } else {
            return Optional.empty();
        }
    }

    private void setAndPropagateAssignment(GroupMetadata group,
                                           Map<String, byte[]> assignment) {
        checkState(group.is(CompletingRebalance));
        group.allMemberMetadata().forEach(member -> member.assignment(assignment.get(member.memberId())));
        propagateAssignment(group, Errors.NONE);
    }

    private void resetAndPropagateAssignmentError(GroupMetadata group,
                                                  Errors error) {
        checkState(group.is(CompletingRebalance));
        group.allMemberMetadata().forEach(m -> m.assignment(new byte[0]));
        propagateAssignment(group, error);
    }

    private void propagateAssignment(GroupMetadata group, Errors error) {
        for (MemberMetadata member : group.allMemberMetadata()) {
            if (member.awaitingSyncCallback() != null) {
                member.awaitingSyncCallback().accept(member.assignment(), error);
                member.awaitingSyncCallback(null);

                // reset the session timeout for members after propagating the member's assignment.
                // This is because if any member's session expired while we were still awaiting either
                // the leader sync group or the storage callback, its expiration will be ignored and no
                // future heartbeat expectations will not be scheduled.
                completeAndScheduleNextHeartbeatExpiration(group, member)
            }
        }
    }

    private JoinGroupResult joinError(String memberId, Errors error) {
        return new JoinGroupResult(
            Collections.emptyMap(),
            memberId,
            0,
            GroupCoordinator.NoProtocol,
            GroupCoordinator.NoLeader,
            error);
    }

    /**
     * Complete existing DelayedHeartbeats for the given member and schedule the next one
     */
    private void completeAndScheduleNextHeartbeatExpiration(GroupMetadata group, MemberMetadata member) {
        // complete current heartbeat expectation
        member.latestHeartbeat(time.milliseconds());
        val memberKey = MemberKey(member.groupId, member.memberId)
        heartbeatPurgatory.checkAndComplete(memberKey)

        // reschedule the next heartbeat expiration deadline
        long newHeartbeatDeadline = member.latestHeartbeat() + member.sessionTimeoutMs();
        val delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.sessionTimeoutMs());
        heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
    }

    private void removeHeartbeatForLeavingMember(GroupMetadata group,
                                                 MemberMetadata member) {
        member.isLeaving(true);
        val memberKey = MemberKey(member.groupId, member.memberId)
        heartbeatPurgatory.checkAndComplete(memberKey);
    }

    private CompletableFuture<JoinGroupResult> addMemberAndRebalance(
        int rebalanceTimeoutMs,
        int sessionTimeoutMs,
        String clientId,
        String clientHost,
        String protocolType,
        Map<String, byte[]> protocols,
        GroupMetadata group
    ) {
        String memberId = clientId + "-" + group.generateMemberIdSuffix();
        MemberMetadata member = new MemberMetadata(
            memberId,
            group.groupId(),
            clientId,
            clientHost,
            rebalanceTimeoutMs,
            sessionTimeoutMs,
            protocolType,
            protocols);
        CompletableFuture<JoinGroupResult> joinFuture = new CompletableFuture<>();
        member.awaitingJoinCallback(joinFuture);
        // update the newMemberAdded flag to indicate that the join group can be further delayed
        if (group.is(PreparingRebalance) && group.generationId() == 0) {
            group.newMemberAdded(true);
        }

        group.add(member);
        maybePrepareRebalance(group);
        return joinFuture;
    }

    private CompletableFuture<JoinGroupResult> updateMemberAndRebalance(
        GroupMetadata group,
        MemberMetadata member,
        Map<String, byte[]> protocols
    ) {
        CompletableFuture<JoinGroupResult> resultFuture = new CompletableFuture<>();
        member.supportedProtocols(protocols);
        member.awaitingJoinCallback(resultFuture);
        maybePrepareRebalance(group);
        return resultFuture;
    }

    private void maybePrepareRebalance(GroupMetadata group) {
        group.inLock(() -> {
            if (group.canReblance()) {
                prepareRebalance(group);
            }
            return null;
        });
    }



    private void prepareRebalance(GroupMetadata group) {
        // if any members are awaiting sync, cancel their request and have them rejoin
        if (group.is(CompletingRebalance))
            resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

        val delayedRebalance = if (group.is(Empty))
            new InitialDelayedJoin(this,
                joinPurgatory,
                group,
                groupConfig.groupInitialRebalanceDelayMs,
                groupConfig.groupInitialRebalanceDelayMs,
                max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
        else
            new DelayedJoin(this, group, group.rebalanceTimeoutMs)

        group.transitionTo(PreparingRebalance)

        info(s"Preparing to rebalance group ${group.groupId} with old generation ${group.generationId} " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

        val groupKey = GroupKey(group.groupId)
        joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
    }

    private void removeMemberAndUpdateGroup(GroupMetadata group,
                                            MemberMetadata member) {
        group.remove(member.memberId());
        switch (group.currentState()) {
            case Dead:
            case Empty:
                break;
            case Stable:
            case CompletingRebalance:
                maybePrepareRebalance(group);
                break;
            case PreparingRebalance:
                // joinPurgatory.checkAndComplete(GroupKey(group.groupId))
                break;
            default:
                break;
        }
    }

    boolean tryCompleteJoin(GroupMetadata group,
                            Supplier<Boolean> forceComplete) {
        return group.inLock(() -> {
            if (group.notYetRejoinedMembers().isEmpty()) {
                return forceComplete.get();
            } else {
                return false;
            }
        });
    }

    void onExpireJoin() {
        // TODO: add metrics for restabilize timeouts
    }

    void onCompleteJoin(GroupMetadata group) {
        group.inLock(() -> {
            // remove any members who haven't joined the group yet
            group.notYetRejoinedMembers().forEach(failedMember -> {
                removeHeartbeatForLeavingMember(group, failedMember)
                group.remove(failedMember.memberId());
                // TODO: cut the socket connection to the client
            });

            if (!group.is(Dead)) {
                group.initNextGeneration();
                if (group.is(Empty)) {
                    log.info("Group {} with generation {} is now empty {}-{}",
                        group.groupId(), group.generationId(),
                        Topic.GROUP_METADATA_TOPIC_NAME, groupManager.partitionFor(group.groupId()));

                    groupManager.storeGroup(group, Collections.emptyMap(), error => {
                        if (error != Errors.NONE) {
                            // we failed to write the empty group metadata. If the broker fails before another rebalance,
                            // the previous generation written to the log will become active again (and most likely timeout).
                            // This should be safe since there are no active members in an empty generation, so we just warn.
                            warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
                        }
              })
                } else {
                    info(s"Stabilized group ${group.groupId} generation ${group.generationId} " +
                        s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

                    // trigger the awaiting join group response callback for all the members after rebalancing
                    for (MemberMetadata member : group.allMemberMetadata()) {
                        Objects.requireNonNull(member.awaitingJoinCallback());
                        JoinGroupRequest joinResult;
                        if (group.isLeader(member.memberId())) {
                            joinResult = new JoinGroupResult(
                                group.currentMemberMetadata
                            );
                        } else {
                            joinResult = new JoinGroupResult(
                                Collections.emptyMap()
                            );
                        }
                        = JoinGroupResult(
                            members = if (group.isLeader(member.memberId)) {
                            group.currentMemberMetadata
                        } else {
                            Map.empty
                        },
                        memberId = member.memberId,
                            generationId = group.generationId,
                            subProtocol = group.protocolOrNull,
                            leaderId = group.leaderOrNull,
                            error = Errors.NONE)

                        member.awaitingJoinCallback(joinResult)
                        member.awaitingJoinCallback = null
                        completeAndScheduleNextHeartbeatExpiration(group, member)
                    }
                }
            }
        });
    }

    boolean tryCompleteHeartbeat(GroupMetadata group,
                                 MemberMetadata member,
                                 long heartbeatDeadline,
                                 Supplier<Boolean> forceComplete) {
        return group.inLock(() -> {
            if (shouldKeepMemberAlive(member, heartbeatDeadline)
                || member.isLeaving()) {
                return forceComplete.get();
            } else {
                return false;
            }
        });
    }

    void onExpireHeartbeat(GroupMetadata group,
                           MemberMetadata member,
                           long heartbeatDeadline) {
        group.inLock(() -> {
            if (!shouldKeepMemberAlive(member, heartbeatDeadline)) {
                log.info("Member {} in group {} has failed, removing it from the group",
                    member.memberId(), group.groupId());
                removeMemberAndUpdateGroup(group, member);
            }
            return null;
        });
    }

    void onCompleteHeartbeat() {
        // TODO: add metrics for complete heartbeats
    }

    private boolean shouldKeepMemberAlive(MemberMetadata member,
                                          long heartbeatDeadline) {
        return member.awaitingJoinCallback() != null ||
            member.awaitingSyncCallback() != null ||
            member.latestHeartbeat() + member.sessionTimeoutMs() > heartbeatDeadline;
    }

}
