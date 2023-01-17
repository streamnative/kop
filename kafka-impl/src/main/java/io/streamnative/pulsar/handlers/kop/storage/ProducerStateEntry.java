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
package io.streamnative.pulsar.handlers.kop.storage;

import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.kafka.common.record.RecordBatch;

/**
 * the batchMetadata is ordered such that the batch with the lowest sequence is at the head of the queue while the
 * batch with the highest sequence is at the tail of the queue. We will retain at most ProducerStateEntry.
 * NumBatchesToRetain elements in the queue. When the queue is at capacity, we remove the first element to make
 * space for the incoming batch.
 */
@Data
@Accessors(fluent = true)
@AllArgsConstructor
public class ProducerStateEntry {

    private long producerId;
    private Short producerEpoch;
    private Integer coordinatorEpoch;
    private Long lastTimestamp;
    private Optional<Long> currentTxnFirstOffset;


    public boolean maybeUpdateProducerEpoch(Short producerEpoch) {
        if (this.producerEpoch == null
                || !this.producerEpoch.equals(producerEpoch)) {
            this.producerEpoch = producerEpoch;
            return true;
        } else {
            return false;
        }
    }

    public void update(ProducerStateEntry nextEntry) {
        maybeUpdateProducerEpoch(nextEntry.producerEpoch);
        this.currentTxnFirstOffset(nextEntry.currentTxnFirstOffset);
        this.lastTimestamp(nextEntry.lastTimestamp);
    }

    public static ProducerStateEntry empty(long producerId){
        return new ProducerStateEntry(producerId,
                RecordBatch.NO_PRODUCER_EPOCH, -1, RecordBatch.NO_TIMESTAMP, Optional.empty());
    }
}

