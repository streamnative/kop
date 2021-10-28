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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;

public class KafkaRequestUtils {

    public static void forEachCreatePartitionsRequest(CreatePartitionsRequest request,
                                                      BiConsumer<String, NewPartitions> consumer) {
        request.newPartitions().forEach(consumer);
    }

    public static void forEachListOffsetRequest(ListOffsetRequest request,
                                                BiConsumer<TopicPartition, Long> consumer) {
        request.partitionTimestamps().forEach(consumer);
    }

    public static class LegacyUtils {

        public static void forEachListOffsetRequest(
                ListOffsetRequest request,
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
