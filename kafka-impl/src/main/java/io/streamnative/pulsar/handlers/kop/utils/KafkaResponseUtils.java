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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
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
        ApiVersionsResponseData data = new ApiVersionsResponseData();
        data.setErrorCode(errors.code());
        return new ApiVersionsResponse(data);
    }

    public static CreatePartitionsResponse newCreatePartitions(Map<String, ApiError> topicToErrors) {
        CreatePartitionsResponseData data = new CreatePartitionsResponseData();
        data.setThrottleTimeMs(AbstractResponse.DEFAULT_THROTTLE_TIME);
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
                    DeleteRecordsResponseData.DeleteRecordsTopicResult deleteRecordsTopicResult = new DeleteRecordsResponseData.DeleteRecordsTopicResult();
                    deleteRecordsTopicResult.setName(topic);
                    DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection partitionResults = deleteRecordsTopicResult.partitions();
                    responseMap.entrySet().stream().filter(
                            entry -> entry.getKey().topic().equals(topic)
                    ).forEach(partitions -> {
                        DeleteRecordsResponseData.DeleteRecordsPartitionResult result = new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                                .setPartitionIndex(partitions.getKey().partition())
                                .setErrorCode(partitions.getValue().code());
                        partitionResults.add(result);

                    });
        });
        return new DeleteRecordsResponse(data);
    }

    public static DescribeGroupsResponse newDescribeGroups(
            Map<String, KeyValue<Errors, GroupMetadata.GroupSummary>> groupToSummary) {
        return new DescribeGroupsResponse(CoreUtils.mapValue(groupToSummary, pair -> {
            final Errors errors = pair.getKey();
            final GroupMetadata.GroupSummary summary = pair.getValue();
            List<DescribeGroupsResponse.GroupMember> members = summary.members().stream()
                    .map(member -> {
                        ByteBuffer metadata = ByteBuffer.wrap(member.metadata());
                        ByteBuffer assignment = ByteBuffer.wrap(member.assignment());
                        return new DescribeGroupsResponse.GroupMember(
                                member.memberId(),
                                member.clientId(),
                                member.clientHost(),
                                metadata,
                                assignment
                        );
                    }).collect(Collectors.toList());
            return new DescribeGroupsResponse.GroupMetadata(
                    errors,
                    summary.state(),
                    summary.protocolType(),
                    summary.protocol(),
                    members
            );
        }));
    }

    public static FindCoordinatorResponse newFindCoordinator(MetadataResponse.PartitionMetadata node) {
        FindCoordinatorResponseData data = new FindCoordinatorResponseData();
        data.setNodeId(node.leaderId.get());
        data.setErrorCode(Errors.NONE.code());
        return new FindCoordinatorResponse(data);
    }

    public static FindCoordinatorResponse newFindCoordinator(Errors errors) {
        FindCoordinatorResponseData data = new FindCoordinatorResponseData();
        data.setErrorCode(errors.code());
        data.setErrorMessage(errors.message());
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
                                                 String memberId,
                                                 String leaderId,
                                                 Map<String, byte[]> groupMembers) {
        JoinGroupResponseData data = new JoinGroupResponseData()
                .setErrorCode(errors.code())
                .setLeader(leaderId)
                .setGenerationId(generationId)
                .setMemberId(memberId)
                .setMembers(groupMembers
                        .entrySet()
                        .stream()
                        .map(entry ->
                            new JoinGroupResponseData.JoinGroupResponseMember()
                                    .setMemberId(entry.getKey())
                                    .setMetadata(entry.getValue())
                        )
                        .collect(Collectors.toList()));
        return new JoinGroupResponse(data);
    }

    public static LeaveGroupResponse newLeaveGroup(Errors errors) {
        LeaveGroupResponseData data = new LeaveGroupResponseData();
        data.setErrorCode(errors.code());
        return new LeaveGroupResponse(data);
    }

    public static ListGroupsResponse newListGroups(Errors errors,
                                                   List<GroupMetadata.GroupOverview> groups) {
        ListGroupsResponseData data = new ListGroupsResponseData();
        data.setErrorCode(errors.code());
        data.setGroups(groups.stream().map(overView -> new ListGroupsResponseData.ListedGroup()
                .setGroupId(overView.groupId())
                .setProtocolType(overView.protocolType()))
                .collect(Collectors.toList()));
        return new ListGroupsResponse(data)
        );
    }

    public static ListOffsetsResponse newListOffset(
            Map<TopicPartition,
            Pair<Errors, Long>> partitionToOffset,
            boolean legacy) {
        if (legacy) {
            ListOffsetsResponseData data = new ListOffsetsResponseData();
            List<ListOffsetsResponseData.ListOffsetsTopicResponse> topicsResponse = data.topics();
            List<String> topics = partitionToOffset.keySet().stream().map(TopicPartition::topic).distinct().collect(Collectors.toList());
            topics.forEach(topic -> {
                ListOffsetsResponseData.ListOffsetsTopicResponse  listOffsetsTopicResponse = new ListOffsetsResponseData.ListOffsetsTopicResponse()
                        .setName(topic);
                topicsResponse.add(listOffsetsTopicResponse);
                partitionToOffset.forEach((TopicPartition topicPartition, Pair<Errors, Long> errorsLongPair) ->  {
                    if (topicPartition.topic().equals(topic)) {
                        Errors errors = errorsLongPair.getKey();
                        long offset = errorsLongPair.getValue() != null ? errorsLongPair.getValue() : 0L;
                        listOffsetsTopicResponse.partitions().add(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
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
            List<String> topics = partitionToOffset.keySet().stream().map(TopicPartition::topic).distinct().collect(Collectors.toList());
            topics.forEach(topic -> {
                ListOffsetsResponseData.ListOffsetsTopicResponse  listOffsetsTopicResponse = new ListOffsetsResponseData.ListOffsetsTopicResponse()
                        .setName(topic);
                topicsResponse.add(listOffsetsTopicResponse);
                partitionToOffset.forEach((TopicPartition topicPartition, Pair<Errors, Long> errorsLongPair) ->  {
                    if (topicPartition.topic().equals(topic)) {
                        Errors errors = errorsLongPair.getKey();
                        long offset = errorsLongPair.getValue() != null ? errorsLongPair.getValue() : 0L;
                        listOffsetsTopicResponse.partitions().add(new ListOffsetsResponseData.ListOffsetsPartitionResponse()
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
            MetadataResponseData.MetadataResponseTopic metadataResponseTopic = new MetadataResponseData.MetadataResponseTopic()
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

    public static MetadataResponse.PartitionMetadata newMetadataPartition(int partition,
                                                                          Node node) {
        return new MetadataResponse.PartitionMetadata(Errors.NONE,
                partition,
                node, // leader
                Optional.empty(), // leaderEpoch is unknown in Pulsar
                Collections.singletonList(node.id()), // replicas
                Collections.singletonList(node.id()), // isr
                Collections.emptyList() // offline replicas
        );
    }

    public static MetadataResponse.PartitionMetadata newMetadataPartition(Errors errors,
                                                                          int partition) {
        return new MetadataResponse.PartitionMetadata(errors,
                partition,
                Node.noNode(), // leader
                Optional.empty(), // leaderEpoch is unknown in Pulsar
                Collections.singletonList(Node.noNode()), // replicas
                Collections.singletonList(Node.noNode()), // isr
                Collections.emptyList() // offline replicas
        );
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

    public static SaslAuthenticateResponse newSaslAuthenticate(ByteBuffer saslAuthBytes) {
        return new SaslAuthenticateResponse(Errors.NONE, "", saslAuthBytes);
    }

    public static SaslAuthenticateResponse newSaslAuthenticate(Errors errors, String message) {
        return new SaslAuthenticateResponse(errors, message);
    }

    public static SaslHandshakeResponse newSaslHandshake(Errors errors, Set<String> allowedMechanisms) {
        return new SaslHandshakeResponse(errors, allowedMechanisms);
    }

    public static SaslHandshakeResponse newSaslHandshake(Errors errors) {
        return new SaslHandshakeResponse(errors, Collections.emptySet());
    }

    public static SyncGroupResponse newSyncGroup(Errors errors, ByteBuffer memberState) {
        return new SyncGroupResponse(errors, memberState);
    }
}
