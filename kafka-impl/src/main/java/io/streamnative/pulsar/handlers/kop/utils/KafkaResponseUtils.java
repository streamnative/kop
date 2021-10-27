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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.pulsar.common.schema.KeyValue;

public class KafkaResponseUtils {

    public static ApiVersionsResponse newApiVersions(List<ApiVersion> versionList) {
        return new ApiVersionsResponse(0, Errors.NONE, versionList.stream()
                .map(apiVersion -> new ApiVersionsResponse.ApiVersion(
                        apiVersion.apiKey, apiVersion.minVersion, apiVersion.maxVersion))
                .collect(Collectors.toList())
        );
    }

    public static ApiVersionsResponse newApiVersions(Errors errors) {
        return new ApiVersionsResponse(0, errors, Collections.emptyList());
    }

    public static CreatePartitionsResponse newCreatePartitions(Map<String, ApiError> topicToErrors) {
        return new CreatePartitionsResponse(AbstractResponse.DEFAULT_THROTTLE_TIME, topicToErrors);
    }

    public static CreateTopicsResponse newCreateTopics(Map<String, ApiError> errorMap) {
        return new CreateTopicsResponse(errorMap);
    }

    public static DeleteGroupsResponse newDeleteGroups(Map<String, Errors> groupToErrors) {
        return new DeleteGroupsResponse(groupToErrors);
    }

    public static DeleteTopicsResponse newDeleteTopics(Map<String, Errors> topicToErrors) {
        return new DeleteTopicsResponse(topicToErrors);
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

    public static FindCoordinatorResponse newFindCoordinator(Node node) {
        return new FindCoordinatorResponse(Errors.NONE, node);
    }

    public static FindCoordinatorResponse newFindCoordinator(Errors errors) {
        return new FindCoordinatorResponse(errors, Node.noNode());
    }

    public static HeartbeatResponse newHeartbeat(Errors errors) {
        return new HeartbeatResponse(errors);
    }

    public static JoinGroupResponse newJoinGroup(Errors errors,
                                                 int generationId,
                                                 String groupProtocol,
                                                 String memberId,
                                                 String leaderId,
                                                 Map<String, ByteBuffer> groupMembers) {
        return new JoinGroupResponse(errors, generationId, groupProtocol, memberId, leaderId, groupMembers);
    }

    public static LeaveGroupResponse newLeaveGroup(Errors errors) {
        return new LeaveGroupResponse(errors);
    }

    public static ListGroupsResponse newListGroups(Errors errors,
                                                   List<GroupMetadata.GroupOverview> groups) {
        return new ListGroupsResponse(errors, groups.stream()
                .map(groupOverview -> new ListGroupsResponse.Group(
                        groupOverview.groupId(), groupOverview.protocolType()))
                .collect(Collectors.toList())
        );
    }

    public static ListOffsetResponse newListOffset(
            Map<TopicPartition,
            Pair<Errors, Long>> partitionToOffset,
            boolean legacy) {
        if (legacy) {
            return new ListOffsetResponse(CoreUtils.mapValue(partitionToOffset,
                    pair -> new ListOffsetResponse.PartitionData(
                            pair.getLeft(),
                            Optional.ofNullable(pair.getRight()).map(Collections::singletonList)
                                    .orElse(Collections.emptyList()))
                    ));
        } else {
            return new ListOffsetResponse(CoreUtils.mapValue(partitionToOffset,
                    pair -> new ListOffsetResponse.PartitionData(
                            pair.getLeft(), // error
                            0L, // timestamp
                            Optional.ofNullable(pair.getRight()).orElse(0L) // offset
                    )
            ));
        }
    }

    public static MetadataResponse newMetadata(List<Node> nodes,
                                               String clusterName,
                                               int controllerId,
                                               List<MetadataResponse.TopicMetadata> topicMetadata) {
        return new MetadataResponse(nodes, clusterName, controllerId, topicMetadata);
    }

    public static MetadataResponse.PartitionMetadata newMetadataPartition(int partition,
                                                                          Node node) {
        return new MetadataResponse.PartitionMetadata(Errors.NONE,
                partition,
                node, // leader
                Collections.singletonList(node), // replicas
                Collections.singletonList(node), // isr
                Collections.emptyList() // offline replicas
        );
    }

    public static MetadataResponse.PartitionMetadata newMetadataPartition(Errors errors,
                                                                          int partition) {
        return new MetadataResponse.PartitionMetadata(errors,
                partition,
                Node.noNode(), // leader
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
                metadata,
                Errors.NONE);
    }

    public static OffsetFetchResponse.PartitionData newOffsetFetchPartition() {
        return new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET,
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
