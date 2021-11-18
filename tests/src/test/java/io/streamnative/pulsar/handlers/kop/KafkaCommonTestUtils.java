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
package io.streamnative.pulsar.handlers.kop;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.TxnOffsetCommitRequest;

public class KafkaCommonTestUtils {

    public static Map<TopicPartition, Long> newListOffsetTargetTimes(
            TopicPartition topicPartition,
            long timestamp) {
        return Collections.singletonMap(topicPartition, timestamp);
    }

    public static FetchRequest.PartitionData newFetchRequestPartitionData(long fetchOffset,
                                                                          long logStartOffset,
                                                                          int maxBytes) {
        return new FetchRequest.PartitionData(fetchOffset,
                logStartOffset,
                maxBytes
        );
    }

    public static TxnOffsetCommitRequest.CommittedOffset newTxnOffsetCommitRequestCommittedOffset(
            long offset,
            String metadata) {
        return new TxnOffsetCommitRequest.CommittedOffset(offset,
                metadata
        );
    }

    public static OffsetCommitRequest.PartitionData newOffsetCommitRequestPartitionData(long offset,
                                                                                        String metadata) {
        return new OffsetCommitRequest.PartitionData(offset,
                metadata
        );
    }


    public static Map<String, NewPartitions> newPartitionsMap(List<String> topics, int totalCount) {
        return topics.stream().collect(Collectors.toMap(topic -> topic, __ -> NewPartitions.increaseTo(totalCount)));
    }

    public static Map<String, NewPartitions> newPartitionsMap(String topic, int totalCount) {
        return newPartitionsMap(Collections.singletonList(topic), totalCount);
    }
}
