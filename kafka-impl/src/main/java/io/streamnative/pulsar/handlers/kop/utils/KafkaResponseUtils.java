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
package io.streamnative.pulsar.handlers.kop.utils;

import io.streamnative.pulsar.handlers.kop.ApiVersion;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata;
import io.streamnative.pulsar.handlers.kop.scala.Either;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.pulsar.common.schema.KeyValue;

@Slf4j
public class KafkaResponseUtils {

    public static ApiVersionsResponse newApiVersions(List<ApiVersion> versionList) {
        ApiVersionsResponseData data = new ApiVersionsResponseData();
        versionList.forEach(apiVersion ->  {
            data.apiKeys().add(new ApiVersionsResponseData.ApiVersion()
                    .setApiKey(apiVersion.apiKey)
                    .setMinVersion(apiVersion.minVersion)
                    .setMaxVersion(apiVersion.maxVersion));
        });
        return new ApiVersionsResponse(data);
    }

    public static ApiVersionsResponse newApiVersions(Errors errors) {
        ApiVersionsResponseData data = new ApiVersionsResponseData()
                .setErrorCode(errors.code());
        return new ApiVersionsResponse(data);
    }

    public static CreatePartitionsResponse newCreatePartitions(Map<String, ApiError> topicToErrors) {
        CreatePartitionsResponseData data = new CreatePartitionsResponseData()
                .setThrottleTimeMs(AbstractResponse.DEFAULT_THROTTLE_TIME);
        topicToErrors.forEach((topic, errors) -> {
            data.results().add(new CreatePartitionsResponseData.CreatePartitionsTopicResult()
                    .setName(topic)
                    .setErrorCode(errors.error().code())
                    .setErrorMessage(errors.messageWithFallback()));
        });
        return new CreatePartitionsResponse(data);
    }

    public static CreateTopicsResponse newCreateTopics(Map<String, ApiError> errorMap) {
        CreateTopicsResponseData data = new CreateTopicsResponseData();
        errorMap.forEach((topic, errors) -> {
            data.topics().add(new CreateTopicsResponseData.CreatableTopicResult()
                            .setName(topic)
                            .setErrorMessage(errors.messageWithFallback())
                            .setErrorCode(errors.error().code()));
        });
        return new CreateTopicsResponse(data);
    }

    public static DeleteGroupsResponse newDeleteGroups(Map<String, Errors> groupToErrors) {
        DeleteGroupsResponseData deleteGroupsResponseData = new DeleteGroupsResponseData();
        groupToErrors.forEach((group, errors) -> {
            deleteGroupsResponseData.results().add(new DeleteGroupsResponseData.DeletableGroupResult()
                    .setGroupId(group)
                    .setErrorCode(errors.code()));
        });
        return new DeleteGroupsResponse(deleteGroupsResponseData);
    }

    public static DeleteTopicsResponse newDeleteTopics(Map<String, Errors> topicToErrors) {
        DeleteTopicsResponseData deleteTopicsResponseData = new DeleteTopicsResponseData();
        topicToErrors.forEach((topic, errors) -> {
            deleteTopicsResponseData.responses().add(new DeleteTopicsResponseData.DeletableTopicResult()
                    .setName(topic)
                    .setErrorCode(errors.code())
                    .setErrorMessage(errors.message()));
        });
        return new DeleteTopicsResponse(deleteTopicsResponseData);
    }

    public static DeleteRecordsResponse newDeleteRecords(Map<TopicPartition,
            Errors> responseMap) {
        DeleteRecordsResponseData data = new DeleteRecordsResponseData();
        responseMap.keySet()
                .stream()
                .map(TopicPartition::topic)
                .distinct()
                .forEach((String topic) -> {
                    DeleteRecordsResponseData.DeleteRecordsTopicResult deleteRecordsTopicResult =
                            new DeleteRecordsResponseData.DeleteRecordsTopicResult();
                    deleteRecordsTopicResult.setName(topic);
                    data.topics().add(deleteRecordsTopicResult);
                    DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection partitionResults =
                            deleteRecordsTopicResult.partitions();
                    responseMap.entrySet().stream().filter(
                            entry -> entry.getKey().topic().equals(topic)
                    ).forEach(partitions -> {
                        DeleteRecordsResponseData.DeleteRecordsPartitionResult result =
                                new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                                .setPartitionIndex(partitions.getKey().partition())
                                .setErrorCode(partitions.getValue().code());
                        partitionResults.add(result);

                    });
        });
        return new DeleteRecordsResponse(data);
    }

    public static DescribeGroupsResponse newDescribeGroups(
            Map<String, KeyValue<Errors, GroupMetadata.GroupSummary>> groupToSummary) {
        DescribeGroupsResponseData data = new DescribeGroupsResponseData();
        groupToSummary.forEach((String group, KeyValue<Errors, GroupMetadata.GroupSummary> pair) -> {
            final Errors errors = pair.getKey();
            final GroupMetadata.GroupSummary summary = pair.getValue();
            DescribeGroupsResponseData.DescribedGroup describedGroup = new DescribeGroupsResponseData.DescribedGroup()
                    .setGroupId(group)
                    .setErrorCode(errors.code())
                    .setGroupState(summary.state())
                    .setProtocolType(summary.protocolType())
                    .setProtocolData(summary.protocol());
            data.groups().add(describedGroup);
            summary.members().forEach((member) -> {
                describedGroup.members().add(new DescribeGroupsResponseData.DescribedGroupMember()
                        .setClientHost(member.clientHost())
                        .setMemberId(member.memberId())
                        .setClientId(member.clientId())
                        .setMemberMetadata(member.metadata())
                        .setMemberAssignment(member.assignment()));
                    });
        });
        return new DescribeGroupsResponse(data);
    }

    public static FindCoordinatorResponse newFindCoordinator(List<String> coordinatorKeys,
                                                             Node node,
                                                             int version) {
        FindCoordinatorResponseData data = new FindCoordinatorResponseData();
        if (version < FindCoordinatorRequest.MIN_BATCHED_VERSION) {
            data.setErrorMessage(Errors.NONE.message())
                    .setErrorCode(Errors.NONE.code())
                    .setPort(node.port())
                    .setHost(node.host())
                    .setNodeId(node.id());
        } else {
            // for new clients
            data.setCoordinators(coordinatorKeys
                    .stream()
                    .map(key -> new FindCoordinatorResponseData.Coordinator()
                            .setErrorCode(Errors.NONE.code())
                            .setErrorMessage(Errors.NONE.message())
                            .setHost(node.host())
                            .setPort(node.port())
                            .setNodeId(node.id())
                            .setKey(key))
                    .collect(Collectors.toList()));
        }

        return new FindCoordinatorResponse(data);
    }

    public static FindCoordinatorResponse newFindCoordinator(List<FindCoordinatorResponseData.Coordinator> coordinators,
                                                             int version) {
        FindCoordinatorResponseData data = new FindCoordinatorResponseData();
        if (version < FindCoordinatorRequest.MIN_BATCHED_VERSION) {
            FindCoordinatorResponseData.Coordinator coordinator = coordinators.get(0);
            data.setErrorMessage(coordinator.errorMessage())
                    .setErrorCode(coordinator.errorCode())
                    .setPort(coordinator.port())
                    .setHost(coordinator.host())
                    .setNodeId(coordinator.nodeId());
        } else {
            // for new clients
            data.setCoordinators(coordinators);
        }

        return new FindCoordinatorResponse(data);
    }


    public static HeartbeatResponse newHeartbeat(Errors errors) {
        HeartbeatResponseData data = new HeartbeatResponseData();
        data.setErrorCode(errors.code());
        return new HeartbeatResponse(data);
    }

    public static JoinGroupResponse newJoinGroup(Errors errors,
                                                 int generationId,
                                                 String groupProtocol,
                                                 String groupProtocolType,
                                                 String memberId,
                                                 String leaderId,
                                                 Map<String, byte[]> groupMembers,
                                                 short requestVersion) {
        JoinGroupResponseData data = new JoinGroupResponseData()
                .setErrorCode(errors.code())
                .setLeader(leaderId)
                .setGenerationId(generationId)
                .setMemberId(memberId)
                .setProtocolType(groupProtocolType)
                .setProtocolName(groupProtocol)
                .setMembers(groupMembers
                        .entrySet()
                        .stream()
                        .map(entry ->
                            new JoinGroupResponseData.JoinGroupResponseMember()
                                    .setMemberId(entry.getKey())
                                    .setMetadata(entry.getValue())
                        )
                        .collect(Collectors.toList()));

        if (errors == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
            data.setThrottleTimeMs(1000);
        }

        return new JoinGroupResponse(data, requestVersion);
    }

    public static LeaveGroupResponse newLeaveGroup(Errors errors) {
        LeaveGroupResponseData data = new LeaveGroupResponseData();
        data.setErrorCode(errors.code());
        return new LeaveGroupResponse(data);
    }

    public static ListGroupsResponse newListGroups(Either<Errors, List<GroupMetadata.GroupOverview>> results) {
        ListGroupsResponseData data = new ListGroupsResponseData();
        data.setErrorCode(results.isLeft() ? results.getLeft().code() : Errors.NONE.code());
        if (!results.isLeft()) {
            data.setGroups(results.getRight().stream().map(overView -> new ListGroupsResponseData.ListedGroup()
                            .setGroupId(overView.groupId())
                            .setProtocolType(overView.protocolType()))
                    .collect(Collectors.toList()));

        } else {
            data.setGroups(Collections.emptyList());
        }
        return new ListGroupsResponse(data);
    }

    public static ListOffsetsResponse newListOffset(
            Map<TopicPartition,
            Pair<Errors, Long>> partitionToOffset,
            boolean legacy) {
        if (legacy) {
            ListOffsetsResponseData data = new ListOffsetsResponseData();
            List<ListOffsetsResponseData.ListOffsetsTopicResponse> topicsResponse = data.topics();
            List<String> topics = partitionToOffset.keySet().stream().map(TopicPartition::topic)
                    .distinct().collect(Collectors.toList());
            topics.forEach(topic -> {
                ListOffsetsResponseData.ListOffsetsTopicResponse  listOffsetsTopicResponse =
                        new ListOffsetsResponseData.ListOffsetsTopicResponse()
                        .setName(topic);
                topicsResponse.add(listOffsetsTopicResponse);
                partitionToOffset.forEach((TopicPartition topicPartition, Pair<Errors, Long> errorsLongPair) ->  {
                    if (topicPartition.topic().equals(topic)) {
                        Errors errors = errorsLongPair.getKey();
                        long offset = errorsLongPair.getValue() != null ? errorsLongPair.getValue() : 0L;
                        listOffsetsTopicResponse.partitions()
                                .add(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                                .setOldStyleOffsets(Collections.singletonList(offset))
                                .setPartitionIndex(topicPartition.partition())
                                .setErrorCode(errors.code()));
                    }
                });
            });
            return new ListOffsetsResponse(data);
        } else {
            ListOffsetsResponseData data = new ListOffsetsResponseData();
            List<ListOffsetsResponseData.ListOffsetsTopicResponse> topicsResponse = data.topics();
            List<String> topics = partitionToOffset.keySet().stream().map(TopicPartition::topic)
                    .distinct().collect(Collectors.toList());
            topics.forEach(topic -> {
                ListOffsetsResponseData.ListOffsetsTopicResponse  listOffsetsTopicResponse =
                        new ListOffsetsResponseData.ListOffsetsTopicResponse()
                        .setName(topic);
                topicsResponse.add(listOffsetsTopicResponse);
                partitionToOffset.forEach((TopicPartition topicPartition, Pair<Errors, Long> errorsLongPair) ->  {
                    if (topicPartition.topic().equals(topic)) {
                        Errors errors = errorsLongPair.getKey();
                        long offset = errorsLongPair.getValue() != null ? errorsLongPair.getValue() : 0L;
                        listOffsetsTopicResponse.partitions()
                                .add(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
                                .setOffset(offset)
                                 .setTimestamp(0)
                                .setPartitionIndex(topicPartition.partition())
                                .setErrorCode(errors.code()));
                    }
                });
            });
            return new ListOffsetsResponse(data);
        }
    }

    public static MetadataResponse newMetadata(List<Node> nodes,
                                               String clusterName,
                                               int controllerId,
                                               List<MetadataResponse.TopicMetadata> topicMetadata,
                                               short apiVersion) {
        MetadataResponseData data = new MetadataResponseData()
        .setClusterId(clusterName)
        .setControllerId(controllerId);

        nodes.forEach(node -> {
            data.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                    .setHost(node.host())
                    .setNodeId(node.id())
                    .setPort(node.port())
                    .setRack(node.rack()));
        });

        topicMetadata.forEach(md -> {
            MetadataResponseData.MetadataResponseTopic metadataResponseTopic =
                    new MetadataResponseData.MetadataResponseTopic()
                    .setErrorCode(md.error().code())
                    .setName(md.topic())
                    .setIsInternal(md.isInternal());
            md.partitionMetadata().forEach(pd -> {
                metadataResponseTopic.partitions().add(new MetadataResponseData.MetadataResponsePartition()
                        .setPartitionIndex(pd.partition())
                        .setErrorCode(pd.error.code())
                        .setIsrNodes(pd.inSyncReplicaIds)
                        .setLeaderEpoch(pd.leaderEpoch.orElse(-1))
                        .setLeaderId(pd.leaderId.orElse(-1))
                        .setOfflineReplicas(pd.offlineReplicaIds)
                        .setReplicaNodes(pd.replicaIds));
            });

            data.topics().add(metadataResponseTopic);
        });
        return new MetadataResponse(data, apiVersion);
    }


    @Getter
    @AllArgsConstructor
    public static class BrokerLookupResult {
        public final TopicPartition topicPartition;
        public final Errors error;
        public final Node node;

        public MetadataResponse.PartitionMetadata toPartitionMetadata() {
            return new MetadataResponse.PartitionMetadata(error, topicPartition,
                    Optional.ofNullable(node).map(Node::id),
                    Optional.empty(),
                    node != null ? Collections.singletonList(node.id()) : Collections.emptyList(),
                    node != null ? Collections.singletonList(node.id()) : Collections.emptyList(),
                    Collections.emptyList()
                    );
        }
    }

    public static BrokerLookupResult newMetadataPartition(TopicPartition topicPartition,
                                                                              Node node) {
        return new BrokerLookupResult(topicPartition, Errors.NONE, node);
    }

    public static BrokerLookupResult newMetadataPartition(Errors errors,
                                                                          TopicPartition topicPartition) {
        return new BrokerLookupResult(topicPartition, errors, null);
    }

    public static OffsetCommitResponse newOffsetCommit(Map<TopicPartition, Errors> responseData) {
        return new OffsetCommitResponse(responseData);
    }

    public static OffsetFetchResponse.PartitionData newOffsetFetchPartition(long offset,
                                                                            String metadata) {
        return new OffsetFetchResponse.PartitionData(offset,
                Optional.empty(), // leaderEpoch is unknown in Pulsar
                metadata,
                Errors.NONE);
    }

    public static OffsetFetchResponse.PartitionData newOffsetFetchPartition() {
        return new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                Optional.empty(), // leaderEpoch is unknown in Pulsar
                "", // metadata
                Errors.NONE
        );
    }

    public static SaslAuthenticateResponse newSaslAuthenticate(byte[] saslAuthBytes) {
        SaslAuthenticateResponseData data = new SaslAuthenticateResponseData();
        data.setErrorCode(Errors.NONE.code());
        data.setErrorMessage(Errors.NONE.message());
        data.setAuthBytes(saslAuthBytes);
        return new SaslAuthenticateResponse(data);
    }

    public static SaslAuthenticateResponse newSaslAuthenticate(Errors errors, String message) {
        SaslAuthenticateResponseData data = new SaslAuthenticateResponseData();
        data.setErrorCode(errors.code());
        data.setErrorMessage(message);
        return new SaslAuthenticateResponse(data);
    }

    public static SaslHandshakeResponse newSaslHandshake(Errors errors, Set<String> allowedMechanisms) {
        SaslHandshakeResponseData data = new SaslHandshakeResponseData();
        data.setErrorCode(errors.code());
        data.setMechanisms(new ArrayList<>(allowedMechanisms));
        return new SaslHandshakeResponse(data);
    }

    public static SaslHandshakeResponse newSaslHandshake(Errors errors) {
        SaslHandshakeResponseData data = new SaslHandshakeResponseData();
        data.setErrorCode(errors.code());
        data.setMechanisms(Collections.emptyList());
        return new SaslHandshakeResponse(data);
    }

    public static SyncGroupResponse newSyncGroup(Errors errors,
                                                 String protocolType,
                                                 String protocolName,
                                                 byte[] assignment) {
        SyncGroupResponseData data = new SyncGroupResponseData();
        data.setErrorCode(errors.code());
        data.setProtocolType(protocolType);
        data.setProtocolName(protocolName);
        data.setAssignment(assignment);
        return new SyncGroupResponse(data);
    }

    @AllArgsConstructor
    public static class OffsetFetchResponseGroupData {
        String groupId;
        Errors errors;
        Map<TopicPartition, OffsetFetchResponse.PartitionData> partitionsResponses;
    }

    public static OffsetFetchResponse buildOffsetFetchResponse(
            List<OffsetFetchResponseGroupData> groups,
            int version) {

        if (version < 8) {
            // old clients
            OffsetFetchResponseGroupData offsetFetchResponseGroupData = groups.get(0);
            return new OffsetFetchResponse(offsetFetchResponseGroupData.errors,
                    offsetFetchResponseGroupData.partitionsResponses);
        } else {
            // new clients
            OffsetFetchResponseData data = new OffsetFetchResponseData();
            for (OffsetFetchResponseGroupData groupData : groups) {
                OffsetFetchResponseData.OffsetFetchResponseGroup offsetFetchResponseGroup =
                        new OffsetFetchResponseData.OffsetFetchResponseGroup()
                                .setErrorCode(groupData.errors.code())
                                .setGroupId(groupData.groupId)
                                .setTopics(new ArrayList<>());
                data.groups().add(offsetFetchResponseGroup);
                Set<String> topics = groupData.partitionsResponses.keySet().stream().map(TopicPartition::topic)
                        .collect(Collectors.toSet());
                topics.forEach(topic -> {
                    offsetFetchResponseGroup.topics().add(new OffsetFetchResponseData.OffsetFetchResponseTopics()
                            .setName(topic)
                            .setPartitions(groupData.partitionsResponses.entrySet()
                                    .stream()
                                    .filter(e -> e.getKey().topic().equals(topic))
                                    .map(entry -> {
                                        OffsetFetchResponse.PartitionData value = entry.getValue();
                                        if (log.isDebugEnabled()) {
                                            log.debug("Add resp for group {} topic {}: {}",
                                                    groupData.groupId, topic, value);
                                        }
                                        return new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                                .setErrorCode(value.error.code())
                                                .setMetadata(value.metadata)
                                                .setPartitionIndex(entry.getKey().partition())
                                                .setCommittedOffset(value.offset)
                                                .setCommittedLeaderEpoch(value.leaderEpoch.orElse(-1));
                                    })
                                    .collect(Collectors.toList())));
                });
            }
            return new OffsetFetchResponse(data);
        }

    }
}
