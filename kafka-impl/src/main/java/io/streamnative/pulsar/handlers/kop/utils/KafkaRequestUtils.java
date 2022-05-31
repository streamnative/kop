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

import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetRequestV0;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;

public class KafkaRequestUtils {

    public static void forEachCreatePartitionsRequest(CreatePartitionsRequest request,
                                        BiConsumer<String, CreatePartitionsRequest.PartitionDetails> consumer) {
        request.newPartitions().forEach(consumer);
    }

    public static void forEachListOffsetRequest(ListOffsetRequest request,
                                        BiConsumer<TopicPartition, ListOffsetRequest.PartitionData> consumer) {
        request.partitionTimestamps().forEach(consumer);
    }

    public static String getMetadata(TxnOffsetCommitRequest.CommittedOffset committedOffset) {
        return Optional.ofNullable(committedOffset.metadata).orElse(OffsetAndMetadata.NoMetadata);
    }

    public static long getOffset(TxnOffsetCommitRequest.CommittedOffset committedOffset) {
        return committedOffset.offset;
    }

    public static class LegacyUtils {

        public static void forEachListOffsetRequest(
                ListOffsetRequestV0 request,
                Function<TopicPartition, Function<Long, Consumer<Integer>>> function) {
            request.offsetData().forEach((topicPartition, partitionData) -> {
                function.apply(topicPartition).apply(partitionData.timestamp).accept(partitionData.maxNumOffsets);
            });
        }

        // V2 adds retention time to the request and V5 removes retention time
        public static long getRetentionTime(OffsetCommitRequest request) {
            return request.retentionTime();
        }
    }
}
