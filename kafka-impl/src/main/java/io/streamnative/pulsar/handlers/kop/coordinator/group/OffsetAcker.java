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

import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.nio.ByteBuffer;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;

/**
 * This class used to track all the partition offset commit position.
 */
@Slf4j
public class OffsetAcker {

    private final PulsarAdmin pulsarAdmin;

    public OffsetAcker(PulsarAdmin pulsarAdmin) {
        this.pulsarAdmin = pulsarAdmin;
    }

    public void addOffsetsTracker(String groupId, byte[] assignment) {
        ByteBuffer assignBuffer = ByteBuffer.wrap(assignment);
        Assignment assign = ConsumerProtocol.deserializeAssignment(assignBuffer);
        if (log.isDebugEnabled()) {
            log.debug(" Add offsets after sync group: {}", assign.toString());
        }
    }

    public void ackOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsetMetadata) {
        if (log.isDebugEnabled()) {
            log.debug(" ack offsets after commit offset for group: {}", groupId);
            offsetMetadata.forEach((partition, metadata) ->
                log.debug("\t partition: {}, offset: {}",
                    partition,  MessageIdUtils.getPosition(metadata.offset())));
        }
        offsetMetadata.forEach(((topicPartition, offsetAndMetadata) -> {
            MessageId messageId = MessageIdUtils.getMessageId(offsetAndMetadata.offset());
            pulsarAdmin.topics().resetCursorAsync(topicPartition.toString(), groupId, messageId);
        }));
    }
}
