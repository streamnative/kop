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
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupState.CompletingRebalance;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupState.Dead;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupState.Empty;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupState.PreparingRebalance;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupState.Stable;
import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_EPOCH;
import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_ID;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata.GroupOverview;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata.GroupSummary;
import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationKey.GroupKey;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationKey.MemberKey;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationPurgatory;
import io.streamnative.pulsar.handlers.kop.utils.timer.Timer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Group coordinator.
 */
@Slf4j
public class GroupCoordinator {

    public static GroupCoordinator of(
        SystemTopicClient client,
        GroupConfig groupConfig,
        OffsetConfig offsetConfig,
        Timer timer,
        Time time
    ) {
        ScheduledExecutorService coordinatorExecutor = OrderedScheduler.newSchedulerBuilder()
            .name("group-coordinator-executor")
            .build();

        GroupMetadataManager metadataManager = new GroupMetadataManager(
            offsetConfig,
            client.newProducerBuilder(),
            client.newReaderBuilder(),
            coordinatorExecutor,
            time
        );

        DelayedOperationPurgatory<DelayedJoin> joinPurgatory = DelayedOperationPurgatory.<DelayedJoin>builder()
            .purgatoryName("group-coordinator-delayed-join")
            .timeoutTimer(timer)
            .build();

        DelayedOperationPurgatory<DelayedHeartbeat> heartbeatPurgatory =
            DelayedOperationPurgatory.<DelayedHeartbeat>builder()
                .purgatoryName("group-coordinator-delayed-heartbeat")
                .timeoutTimer(timer)
                .build();

        return new GroupCoordinator(
                groupConfig,
                metadataManager,
                heartbeatPurgatory,
                joinPurgatory,
                time
        );
    }

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

    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final GroupConfig groupConfig;
    private final GroupMetadataManager groupManager;
    private final DelayedOperationPurgatory<DelayedHeartbeat> heartbeatPurgatory;
    private final DelayedOperationPurgatory<DelayedJoin> joinPurgatory;
    private final Time time;

    public GroupCoordinator(
        GroupConfig groupConfig,
        GroupMetadataManager groupManager,
        DelayedOperationPurgatory<DelayedHeartbeat> heartbeatPurgatory,
        DelayedOperationPurgatory<DelayedJoin> joinPurgatory,
        Time time
        ) {
        this.groupConfig = groupConfig;
        this.groupManager = groupManager;
        this.heartbeatPurgatory = heartbeatPurgatory;
        this.joinPurgatory = joinPurgatory;
        this.time = time;
    }

    /**
     * Startup logic executed at the same time when the server starts up.
     */
    public void startup(boolean enableMetadataExpiration) {
        log.info("Starting up group coordinator.");
        groupManager.startup(enableMetadataExpiration);
        isActive.set(true);
        log.info("Group coordinator started.");
    }

    /**
     * Shutdown logic executed at the same time when server shuts down.
     * Ordering of actions should be reversed from the startup process.
     */
    public void shutdown() {
        log.info("Shutting down group coordinator ...");
        isActive.set(false);
        groupManager.shutdown();
        heartbeatPurgatory.shutdown();
        joinPurgatory.shutdown();
        log.info("Shutdown group coordinator completely.");
    }

    public int partitionFor(String coordinatorKey) {
        return groupManager.partitionFor(coordinatorKey);
    }

    public String getTopicPartitionName(int partition) {
        return groupManager.getTopicPartitionName(partition);
    }

    public ConcurrentMap<Integer, CompletableFuture<Producer<ByteBuffer>>> getOffsetsProducers() {
        return groupManager.getOffsetsProducers();
    }

    public ConcurrentMap<Integer, CompletableFuture<Reader<ByteBuffer>>> getOffsetsReaders() {
        return groupManager.getOffsetsReaders();
    }

    public GroupMetadataManager getGroupManager() {
        return groupManager;
    }

    public GroupConfig groupConfig() {
        return groupConfig;
    }

    public OffsetConfig offsetConfig() {
        return groupManager.offsetConfig();
    }

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

        if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs()
            || sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs()) {
            return CompletableFuture.completedFuture(
                joinError(memberId, Errors.INVALID_SESSION_TIMEOUT));
        } else {
            // only try to create the group if the group is not unknown AND
            // the member id is UNKNOWN, if member is specified but group does not
            // exist we should reject the request
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
                if (!JoinGroupRequest.UNKNOWN_MEMBER_ID.equals(memberId)) {
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
                || !Objects.equals(group.protocolType().get(), protocolType)
                || !group.supportsProtocols(protocols.keySet()))) {
            // if the new member does not support the group protocol, reject it
            return CompletableFuture.completedFuture(
                joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL));
        } else if (group.is(Empty)
            && (protocols.isEmpty() || protocolType.isEmpty())) {
            // reject if first member with empty group protocol or protocolType is empty
            return CompletableFuture.completedFuture(
                joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL));
        } else if (!JoinGroupRequest.UNKNOWN_MEMBER_ID.equals(memberId)
            && !group.has(memberId)) {
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
                    if (JoinGroupRequest.UNKNOWN_MEMBER_ID.equals(memberId)) {
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
                    if (JoinGroupRequest.UNKNOWN_MEMBER_ID.equals(memberId)) {
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
                                    group.generationId(),
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
                    if (JoinGroupRequest.UNKNOWN_MEMBER_ID.equals(memberId)) {
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
                                group.generationId(),
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
                joinPurgatory.checkAndComplete(new GroupKey(group.groupId()));
            }
            return resultFuture;
        }

    }


    public CompletableFuture<KeyValue<Errors, byte[]>> handleSyncGroup(
        String groupId,
        int generation,
        String memberId,
        Map<String, byte[]> groupAssignment
    ) {
        CompletableFuture<KeyValue<Errors, byte[]>> resultFuture = new CompletableFuture<>();
        handleSyncGroup(
            groupId,
            generation,
            memberId,
            groupAssignment,
            (assignment, errors) -> resultFuture.complete(
                new KeyValue<>(errors, assignment))
        );
        return resultFuture;
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
        Map<String, Errors> groupErrors = Collections.synchronizedMap(new HashMap<>());
        List<GroupMetadata> groupsEligibleForDeletion = new ArrayList<>();

        groupIds.forEach(groupId -> {
            Optional<Errors> validateErrorsOpt = validateGroupStatus(groupId, ApiKeys.DELETE_GROUPS);
            validateErrorsOpt.map(error -> {
                groupErrors.put(groupId, error);
                return error;
            }).orElseGet(() -> {
                return groupManager.getGroup(groupId).map(group -> {
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
                });
            });
        });

        if (!groupsEligibleForDeletion.isEmpty()) {
            groupManager.cleanGroupMetadata(
                groupsEligibleForDeletion.stream(),
                g -> g.removeAllOffsets()
            ).thenAccept(offsetsRemoved -> {
                log.info("The following groups were deleted {}. A total of {} offsets were removed.",
                    groupsEligibleForDeletion.stream()
                        .map(GroupMetadata::groupId)
                        .collect(Collectors.joining(",")),
                    offsetsRemoved
                );
            });
            groupErrors.putAll(
                groupsEligibleForDeletion.stream()
                    .collect(Collectors.toMap(
                        GroupMetadata::groupId,
                        ignored -> Errors.NONE
                    ))
            );
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

    public CompletableFuture<Map<TopicPartition, Errors>> handleTxnCommitOffsets(
        String groupId,
        long producerId,
        short producerEpoch,
        Map<TopicPartition, OffsetAndMetadata> offsetMetadata
    ) {
        return validateGroupStatus(groupId, ApiKeys.TXN_OFFSET_COMMIT).map(error ->
            CompletableFuture.completedFuture(
                CoreUtils.mapValue(
                    offsetMetadata,
                    ignored -> error
                )
            )
        ).orElseGet(() -> {
            GroupMetadata group = groupManager.getGroup(groupId).orElseGet(() ->
                groupManager.addGroup(new GroupMetadata(groupId, Empty))
            );
            return doCommitOffsets(
                group,
                NoMemberId,
                NoGeneration,
                producerId,
                producerEpoch,
                offsetMetadata
            );
        });
    }

    public CompletableFuture<Map<TopicPartition, Errors>> handleCommitOffsets(
        String groupId,
        String memberId,
        int generationId,
        Map<TopicPartition, OffsetAndMetadata> offsetMetadata
    ) {
        CompletableFuture<Map<TopicPartition, Errors>> result = validateGroupStatus(groupId, ApiKeys.OFFSET_COMMIT)
            .map(error ->
                CompletableFuture.completedFuture(
                    CoreUtils.mapValue(
                        offsetMetadata,
                        ignored -> error
                    )
                )
            ).orElseGet(() -> {
                return groupManager.getGroup(groupId)
                    .map(group ->
                        doCommitOffsets(
                            group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
                            offsetMetadata
                        )
                    ).orElseGet(() -> {
                        if (generationId < 0) {
                            // the group is not relying on Kafka for group management, so allow the commit
                            GroupMetadata group = groupManager.addGroup(new GroupMetadata(groupId, Empty));
                            return doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
                                offsetMetadata);
                        } else {
                            return CompletableFuture.completedFuture(
                                CoreUtils.mapValue(
                                    offsetMetadata,
                                    ignored -> Errors.ILLEGAL_GENERATION
                                )
                            );
                        }
                    });
            });

        return result;
    }

    public CompletableFuture<Void> scheduleHandleTxnCompletion(
        long producerId,
        Stream<TopicPartition> offsetsPartitions,
        TransactionResult transactionResult
    ) {
        boolean isCommit = TransactionResult.COMMIT == transactionResult;
        return groupManager.scheduleHandleTxnCompletion(
            producerId,
            offsetsPartitions.map(TopicPartition::partition)
                .collect(Collectors.toSet()),
            isCommit
        );
    }

    private CompletableFuture<Map<TopicPartition, Errors>> doCommitOffsets(
        GroupMetadata group,
        String memberId,
        int generationId,
        long producerId,
        short producerEpoch,
        Map<TopicPartition, OffsetAndMetadata> offsetMetadata
    ) {
        return group.inLock(() -> {
            if (group.is(Dead)) {
                return CompletableFuture.completedFuture(
                    CoreUtils.mapValue(offsetMetadata, ignored ->
                        Errors.UNKNOWN_MEMBER_ID));
            } else if ((generationId < 0 && group.is(Empty)) || (producerId != NO_PRODUCER_ID)) {
                // The group is only using Kafka to store offsets.
                // Also, for transactional offset commits we don't need to validate group membership
                // and the generation.
                return groupManager.storeOffsets(group, memberId, offsetMetadata, producerId, producerEpoch);
            } else if (group.is(CompletingRebalance)) {
                return CompletableFuture.completedFuture(
                    CoreUtils.mapValue(offsetMetadata, ignored ->
                        Errors.REBALANCE_IN_PROGRESS));
            } else if (!group.has(memberId)) {
                return CompletableFuture.completedFuture(
                    CoreUtils.mapValue(offsetMetadata, ignored ->
                        Errors.UNKNOWN_MEMBER_ID));
            } else if (generationId != group.generationId()) {
                return CompletableFuture.completedFuture(
                    CoreUtils.mapValue(offsetMetadata, ignored ->
                        Errors.ILLEGAL_GENERATION));
            } else {
                MemberMetadata member = group.get(memberId);
                completeAndScheduleNextHeartbeatExpiration(group, member);
                return groupManager.storeOffsets(
                    group, memberId, offsetMetadata
                );
            }
        });
    }

    public KeyValue<Errors, Map<TopicPartition, PartitionData>> handleFetchOffsets(
        String groupId,
        Optional<List<TopicPartition>> partitions
    ) {
        return validateGroupStatus(groupId, ApiKeys.OFFSET_FETCH).map(errors ->
            new KeyValue<Errors, Map<TopicPartition, PartitionData>>(
                errors,
                new HashMap<>()
            )
        ).orElseGet(() ->
            new KeyValue<>(
                Errors.NONE,
                groupManager.getOffsets(groupId, partitions)
            )
        );
    }

    public KeyValue<Errors, List<GroupOverview>> handleListGroups() {
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

    public KeyValue<Errors, GroupSummary> handleDescribeGroup(String groupId) {
        return validateGroupStatus(groupId, ApiKeys.DESCRIBE_GROUPS).map(error ->
            new KeyValue<>(error, GroupCoordinator.EmptyGroup)
        ).orElseGet(() ->
            groupManager.getGroup(groupId)
                .map(group ->
                    group.inLock(() -> new KeyValue<>(Errors.NONE, group.summary())
                    ))
                .orElseGet(() -> new KeyValue<>(Errors.NONE, GroupCoordinator.DeadGroup))
        );
    }

    public CompletableFuture<Integer> handleDeletedPartitions(Set<TopicPartition> topicPartitions) {
        return groupManager.cleanGroupMetadata(groupManager.currentGroupsStream(), group ->
            group.removeOffsets(topicPartitions.stream())
        ).thenApply(offsetsRemoved -> {
            log.info("Removed {} offsets associated with deleted partitions: {}",
                offsetsRemoved,
                topicPartitions.stream()
                    .map(TopicPartition::toString)
                    .collect(Collectors.joining(",")));
            return offsetsRemoved;
        });
    }

    private boolean isValidGroupId(String groupId,
                                   ApiKeys api) {
        switch (api) {
            case OFFSET_COMMIT:
            case OFFSET_FETCH:
            case DESCRIBE_GROUPS:
            case DELETE_GROUPS:
                // For backwards compatibility, we support the offset commit APIs for the empty groupId, and also
                // in DescribeGroups and DeleteGroups so that users can view and delete state of all groups.
                return groupId != null;
            default:
                return groupId != null && !groupId.isEmpty();

        }
    }

    private Optional<Errors> validateGroupStatus(String groupId,
                                                 ApiKeys api) {
        if (!isValidGroupId(groupId, api)) {
            return Optional.of(Errors.INVALID_GROUP_ID);
        } else if (!isActive.get()) {
            return Optional.of(Errors.COORDINATOR_NOT_AVAILABLE);
        } else if (groupManager.isGroupLoading(groupId)) {
            return Optional.of(Errors.COORDINATOR_LOAD_IN_PROGRESS);
        } else if (!groupManager.isGroupLocal(groupId)) {
            return Optional.of(Errors.NOT_COORDINATOR);
        } else {
            return Optional.empty();
        }
    }

    private void onGroupUnloaded(GroupMetadata group) {
        group.inLock(() -> {
            log.info("Unloading group metadata for {} with generation {}",
                group.groupId(), group.generationId());
            GroupState previousState = group.currentState();
            group.transitionTo(Dead);

            switch (previousState) {
                case Empty:
                case Dead:
                case PreparingRebalance:
                    for (MemberMetadata member : group.allMemberMetadata()) {
                        if (member.awaitingJoinCallback() != null) {
                            member.awaitingJoinCallback().complete(
                                joinError(member.memberId(), Errors.NOT_COORDINATOR)
                            );
                            member.awaitingJoinCallback(null);
                        }
                    }
                    joinPurgatory.checkAndComplete(new GroupKey(group.groupId()));
                    break;
                case Stable:
                case CompletingRebalance:
                    for (MemberMetadata member : group.allMemberMetadata()) {
                        if (member.awaitingSyncCallback() != null) {
                            member.awaitingSyncCallback().accept(
                                new byte[0], Errors.NOT_COORDINATOR
                            );
                            member.awaitingSyncCallback(null);
                        }
                        heartbeatPurgatory.checkAndComplete(
                            new MemberKey(member.groupId(), member.memberId())
                        );
                    }
                    break;
                default:
                    break;
            }
            return null;
        });
    }

    private void onGroupLoaded(GroupMetadata group) {
        group.inLock(() -> {
            log.info("Loading group metadata for {} with generation {}",
                group.groupId(), group.generationId());
            checkArgument(
                group.is(Stable) || group.is(Empty));
            group.allMemberMetadata().forEach(member ->
                completeAndScheduleNextHeartbeatExpiration(group, member));
            return null;
        });
    }

    public CompletableFuture<Void> handleGroupImmigration(int offsetTopicPartitionId) {
        return groupManager.scheduleLoadGroupAndOffsets(
            offsetTopicPartitionId,
            this::onGroupLoaded
        );
    }

    public void handleGroupEmigration(int offsetTopicPartition) {
        groupManager.removeGroupsForPartition(offsetTopicPartition, this::onGroupUnloaded);
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
                completeAndScheduleNextHeartbeatExpiration(group, member);
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
     * Complete existing DelayedHeartbeats for the given member and schedule the next one.
     */
    private void completeAndScheduleNextHeartbeatExpiration(GroupMetadata group, MemberMetadata member) {
        // complete current heartbeat expectation
        member.latestHeartbeat(time.milliseconds());
        MemberKey memberKey = new MemberKey(member.groupId(), member.memberId());
        heartbeatPurgatory.checkAndComplete(memberKey);

        // reschedule the next heartbeat expiration deadline
        long newHeartbeatDeadline = member.latestHeartbeat() + member.sessionTimeoutMs();
        DelayedHeartbeat delayedHeartbeat = new DelayedHeartbeat(
            this,
            group,
            member,
            newHeartbeatDeadline,
            member.sessionTimeoutMs());
        heartbeatPurgatory.tryCompleteElseWatch(
            delayedHeartbeat, Lists.newArrayList(memberKey));
    }

    private void removeHeartbeatForLeavingMember(GroupMetadata group,
                                                 MemberMetadata member) {
        member.isLeaving(true);
        MemberKey memberKey = new MemberKey(member.groupId(), member.memberId());
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
        if (group.is(CompletingRebalance)) {
            resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS);
        }

        DelayedJoin delayedRebalance;

        if (group.is(Empty)) {
            delayedRebalance = new InitialDelayedJoin(this,
                joinPurgatory,
                group,
                groupConfig.groupInitialRebalanceDelayMs(),
                groupConfig.groupInitialRebalanceDelayMs(),
                Math.max(group.rebalanceTimeoutMs() - groupConfig.groupInitialRebalanceDelayMs(), 0));
        } else {
            delayedRebalance = new DelayedJoin(this, group, group.rebalanceTimeoutMs());
        }

        group.transitionTo(PreparingRebalance);

        log.info("Preparing to rebalance group {} with old generation {} ({}-{})",
            group.groupId(),
            group.generationId(),
            Topic.GROUP_METADATA_TOPIC_NAME,
            groupManager.partitionFor(group.groupId()));

        GroupKey groupKey = new GroupKey(group.groupId());
        joinPurgatory.tryCompleteElseWatch(delayedRebalance, Lists.newArrayList(groupKey));
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
                joinPurgatory.checkAndComplete(new GroupKey(group.groupId()));
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
                removeHeartbeatForLeavingMember(group, failedMember);
                group.remove(failedMember.memberId());
                // TODO: cut the socket connection to the client
            });

            if (!group.is(Dead)) {
                group.initNextGeneration();
                if (group.is(Empty)) {
                    log.info("Group {} with generation {} is now empty {}-{}",
                        group.groupId(), group.generationId(),
                        Topic.GROUP_METADATA_TOPIC_NAME, groupManager.partitionFor(group.groupId()));

                    groupManager.storeGroup(group, Collections.emptyMap()).thenAccept(error -> {
                        if (error != Errors.NONE) {
                            // we failed to write the empty group metadata. If the broker fails before another
                            // rebalance, the previous generation written to the log will become active again
                            // (and most likely timeout). This should be safe since there are no active members
                            // in an empty generation, so we just warn.
                            log.warn("Failed to write empty metadata for group {}: {}",
                                group.groupId(), error.message());
                        }
                        if (log.isDebugEnabled()) {
                            log.warn("add partition ownership for group {}",
                                group.groupId());
                        }
                        groupManager.addPartitionOwnership(groupManager.partitionFor(group.groupId()));
                    });
                } else {
                    log.info("Stabilized group {} generation {} ({}-{})",
                        group.groupId(), group.generationId(),
                        Topic.GROUP_METADATA_TOPIC_NAME,
                        groupManager.partitionFor(group.groupId()));

                    // trigger the awaiting join group response callback for all the members after rebalancing
                    for (MemberMetadata member : group.allMemberMetadata()) {
                        Objects.requireNonNull(member.awaitingJoinCallback());
                        Map<String, byte[]> members;
                        if (group.isLeader(member.memberId())) {
                            members = group.currentMemberMetadata();
                        } else {
                            members = Collections.emptyMap();
                        }
                        JoinGroupResult joinResult = new JoinGroupResult(
                            members,
                            member.memberId(),
                            group.generationId(),
                            group.protocolOrNull(),
                            group.leaderOrNull(),
                            Errors.NONE);

                        member.awaitingJoinCallback().complete(joinResult);
                        member.awaitingJoinCallback(null);
                        completeAndScheduleNextHeartbeatExpiration(group, member);
                    }
                }
            }
            return null;
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
        return member.awaitingJoinCallback() != null
            || member.awaitingSyncCallback() != null
            || member.latestHeartbeat() + member.sessionTimeoutMs() > heartbeatDeadline;
    }

    private boolean isCoordinatorForGroup(String groupId) {
        return groupManager.isGroupLocal(groupId);
    }

    private boolean isCoordinatorLoadInProgress(String groupId) {
        return groupManager.isGroupLoading(groupId);
    }

    public boolean isActive() {
        return isActive.get();
    }
}
