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

import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingDeque;
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

    private static final Integer NumBatchesToRetain = 1;

    private Long producerId;
    private Deque<BatchMetadata> batchMetadata;
    private Short producerEpoch;
    private Integer coordinatorEpoch;
    private Long lastTimestamp;
    private Optional<Long> currentTxnFirstOffset;

    protected boolean isEmpty() {
        return batchMetadata.isEmpty();
    }

    public Integer firstSeq() {
        if (isEmpty()) {
            return RecordBatch.NO_SEQUENCE;
        } else {
            return batchMetadata.getFirst().firstSeq();
        }
    }

    public Long firstDataOffset() {
        if (isEmpty()) {
            return -1L;
        } else {
            return batchMetadata.getFirst().firstOffset();
        }
    }

    public Integer lastSeq() {
        if (isEmpty()) {
            return RecordBatch.NO_SEQUENCE;
        } else {
            return batchMetadata.getLast().getLastSeq();
        }
    }

    public Long lastDataOffset() {
        if (isEmpty()) {
            return -1L;
        } else {
            return batchMetadata.getLast().getLastOffset();
        }
    }

    public void addBatch(Short producerEpoch, Integer lastSeq, Long lastOffset,
                         Integer offsetDelta, Long timestamp) {
        maybeUpdateProducerEpoch(producerEpoch);
        addBatchMetadata(new BatchMetadata(lastSeq, lastOffset, offsetDelta, timestamp));
        this.lastTimestamp = timestamp;
    }

    public boolean maybeUpdateProducerEpoch(Short producerEpoch) {
        if (!this.producerEpoch.equals(producerEpoch)) {
            batchMetadata.clear();
            this.producerEpoch = producerEpoch;
            return true;
        } else {
            return false;
        }
    }

    public void addBatchMetadata(BatchMetadata batch) {
        if (batchMetadata.size() == ProducerStateEntry.NumBatchesToRetain) {
            batchMetadata.removeFirst();
        }
        batchMetadata.addLast(batch);
    }

    public void update(ProducerStateEntry nextEntry) {
        maybeUpdateProducerEpoch(nextEntry.producerEpoch);
        while (!nextEntry.batchMetadata.isEmpty()) {
            addBatchMetadata(nextEntry.batchMetadata.pollFirst());
        }
        this.currentTxnFirstOffset(nextEntry.currentTxnFirstOffset);
        this.lastTimestamp(nextEntry.lastTimestamp);
    }

    public Optional<BatchMetadata> findDuplicateBatch(RecordBatch batch) {
        if (batch.producerEpoch() != producerEpoch) {
            return Optional.empty();
        } else {
            return batchWithSequenceRange(batch.baseSequence(), batch.lastSequence());
        }
    }

    // Return the batch metadata of the cached batch having the exact sequence range, if any.
    private Optional<BatchMetadata> batchWithSequenceRange(Integer firstSeq, Integer lastSeq) {
        return batchMetadata.stream().filter(batchMetadata ->
                firstSeq.equals(batchMetadata.firstSeq())
                        && lastSeq.equals(batchMetadata.getLastSeq())).findFirst();
    }

    public static ProducerStateEntry empty(Long producerId){
        return new ProducerStateEntry(producerId, new LinkedBlockingDeque<>(),
                RecordBatch.NO_PRODUCER_EPOCH, -1, RecordBatch.NO_TIMESTAMP, Optional.empty());
    }
}

