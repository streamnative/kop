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

import java.util.Collections;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;

public class KafkaCommonUtils {

    public static ListOffsetResponse.PartitionData newListOffsetResponsePartitionData(long offset) {
        return new ListOffsetResponse.PartitionData(Errors.NONE,
                0L, // timestamp
                offset,
                Optional.empty() // leader epoch
        );
    }

    public static ListOffsetResponse.PartitionData newListOffsetResponsePartitionData(Errors errors) {
        return new ListOffsetResponse.PartitionData(errors,
                ListOffsetResponse.UNKNOWN_TIMESTAMP,
                ListOffsetResponse.UNKNOWN_OFFSET,
                Optional.empty() // leader epoch
        );
    }

    public static MetadataResponse.PartitionMetadata newMetadataResponsePartitionMetadata(int partition,
                                                                                          Node node) {
        return new MetadataResponse.PartitionMetadata(Errors.NONE,
                partition,
                node, // leader
                Optional.empty(), // leader epoch
                Collections.singletonList(node), // replicas
                Collections.singletonList(node), // isr
                Collections.emptyList() // offline replicas
        );
    }

    public static MetadataResponse.PartitionMetadata newMetadataResponsePartitionMetadata(Errors errors,
                                                                                          int partition) {
        return new MetadataResponse.PartitionMetadata(errors,
                partition,
                Node.noNode(), // leader
                Optional.empty(), // leader epoch
                Collections.singletonList(Node.noNode()), // replicas
                Collections.singletonList(Node.noNode()), // isr
                Collections.emptyList() // offline replicas
        );
    }

    public static OffsetFetchResponse.PartitionData newOffsetFetchResponsePartitionData(long offset,
                                                                                        String metadata) {
        return new OffsetFetchResponse.PartitionData(offset,
                Optional.empty(), // leader epoch
                metadata,
                Errors.NONE);
    }

    public static OffsetFetchResponse.PartitionData newOffsetFetchResponsePartitionData() {
        return new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                Optional.empty(), // leader epoch
                "", // metadata
                Errors.NONE
        );
    }

    public static void forEachCreatePartitionsRequest(CreatePartitionsRequest request,
                                                      BiConsumer<String, NewPartitions> consumer) {
        request.newPartitions().forEach((topic, partitionDetails) -> {
            consumer.accept(topic,
                    NewPartitions.increaseTo(partitionDetails.totalCount(), partitionDetails.newAssignments()));
        });
    }

    public static void forEachListOffsetRequest(ListOffsetRequest request,
                                                BiConsumer<TopicPartition, Long> consumer) {
        request.partitionTimestamps().forEach((topicPartition, partitionData) -> {
            consumer.accept(topicPartition, partitionData.timestamp);
        });
    }

    public static class LegacyUtils {

        public static ListOffsetResponse.PartitionData newListOffsetResponsePartitionData(long offset) {
            return new ListOffsetResponse.PartitionData(Errors.NONE, Collections.singletonList(offset));
        }

        public static ListOffsetResponse.PartitionData newListOffsetResponsePartitionData(Errors errors) {
            return new ListOffsetResponse.PartitionData(errors,
                    Collections.emptyList());
        }

        public static void forEachListOffsetRequest(
                ListOffsetRequest request,
                Function<TopicPartition, Function<Long, Consumer<Integer>>> function) {
            request.partitionTimestamps().forEach((topicPartition, partitionData) -> {
                function.apply(topicPartition).apply(partitionData.timestamp).accept(partitionData.maxNumOffsets);
            });
        }

        // V2 adds retention time to the request and V5 removes retention time
        public static long getRetentionTime(OffsetCommitRequest request) {
            return request.retentionTime();
        }
    }

}
