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

import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchResponse;

/**
 * AbortedTxn is used cache the aborted index.
 */
@Data
@Accessors(fluent = true)
@AllArgsConstructor
class AbortedTxn {

    private static final int VersionOffset = 0;
    private static final int VersionSize = 2;
    private static final int ProducerIdOffset = VersionOffset + VersionSize;
    private static final int ProducerIdSize = 8;
    private static final int FirstOffsetOffset = ProducerIdOffset + ProducerIdSize;
    private static final int FirstOffsetSize = 8;
    private static final int LastOffsetOffset = FirstOffsetOffset + FirstOffsetSize;
    private static final int LastOffsetSize = 8;
    private static final int LastStableOffsetOffset = LastOffsetOffset + LastOffsetSize;
    private static final int LastStableOffsetSize = 8;
    private static final int TotalSize = LastStableOffsetOffset + LastStableOffsetSize;

    private static final Short CurrentVersion = 0;

    private final Long producerId;
    private final Long firstOffset;
    private final Long lastOffset;
    private final Long lastStableOffset;

    protected ByteBuffer toByteBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(AbortedTxn.TotalSize);
        buffer.putShort(CurrentVersion);
        buffer.putLong(producerId);
        buffer.putLong(firstOffset);
        buffer.putLong(lastOffset);
        buffer.putLong(lastStableOffset);
        buffer.flip();
        return buffer;
    }
}

@Data
@Accessors(fluent = true)
@AllArgsConstructor
class CompletedTxn {
    private Long producerId;
    private Long firstOffset;
    private Long lastOffset;
    private Boolean isAborted;
}

@Data
@Accessors(fluent = true)
@EqualsAndHashCode
class TxnMetadata {
    private final long producerId;
    private final long firstOffset;
    private long lastOffset;

    public TxnMetadata(long producerId, long firstOffset) {
        this.producerId = producerId;
        this.firstOffset = firstOffset;
    }
}

/**
 * BatchMetadata is used to check the message duplicate.
 */
@Getter
@AllArgsConstructor
class BatchMetadata {

    private final Integer lastSeq;
    private final Long lastOffset;
    // Should be seq delta, we use offsetDelta here because in future might change back.
    // When we preset the correct offset before message publish.
    private final Integer offsetDelta;
    private final Long timestamp;

    public int firstSeq() {
        return decrementSequence(lastSeq, offsetDelta);
    }

    public Long firstOffset() {
        return lastOffset - offsetDelta;
    }

    private int decrementSequence(int sequence, int decrement) {
        if (sequence < decrement) {
            return Integer.MAX_VALUE - (decrement - sequence) + 1;
        }
        return sequence - decrement;
    }

    @Override
    public String toString() {
        return "BatchMetadata("
                + "firstSeq=" + firstSeq() + ", "
                + "lastSeq=" + lastSeq + ", "
                + "firstOffset=" + firstOffset() + ", "
                + "lastOffset=" + lastOffset + ", "
                + "timestamp=" + timestamp + ")";
    }
}

/**
 * Producer state manager.
 */
@Slf4j
public class ProducerStateManager {

    private final String topicPartition;
    private final Map<Long, ProducerStateEntry> producers = Maps.newConcurrentMap();

    // ongoing transactions sorted by the first offset of the transaction
    private final TreeMap<Long, TxnMetadata> ongoingTxns = Maps.newTreeMap();
    private final List<AbortedTxn> abortedIndexList = new ArrayList<>();

    public ProducerStateManager(String topicPartition) {
        this.topicPartition = topicPartition;
    }

    public ProducerAppendInfo prepareUpdate(Long producerId, PartitionLog.AppendOrigin origin) {
        ProducerStateEntry currentEntry = lastEntry(producerId).orElse(ProducerStateEntry.empty(producerId));
        return new ProducerAppendInfo(topicPartition, producerId, currentEntry, origin);
    }

    /**
     * Compute the last stable offset of a completed transaction, but do not yet mark the transaction complete.
     * That will be done in `completeTxn` below. This is used to compute the LSO that will be appended to the
     * transaction index, but the completion must be done only after successfully appending to the index.
     */
    public long lastStableOffset(CompletedTxn completedTxn) {
        for (TxnMetadata txnMetadata : ongoingTxns.values()) {
            if (!completedTxn.producerId().equals(txnMetadata.producerId())) {
                return txnMetadata.firstOffset();
            }
        }
        return completedTxn.lastOffset() + 1;
    }

    public Optional<Long> firstUndecidedOffset() {
        Map.Entry<Long, TxnMetadata> entry = ongoingTxns.firstEntry();
        if (entry == null) {
            return Optional.empty();
        }
        return Optional.of(entry.getValue().firstOffset());
    }

    /**
     * Get the last written entry for the given producer id.
     */
    public Optional<ProducerStateEntry> lastEntry(Long producerId) {
        if (!producers.containsKey(producerId)) {
            return Optional.empty();
        }
        return Optional.of(producers.get(producerId));
    }

    /**
     * Update the mapping with the given append information.
     */
    public void update(ProducerAppendInfo appendInfo) {
        if (log.isDebugEnabled()) {
            log.debug("Updated producer {} state to {}", appendInfo.producerId(), appendInfo);
        }
        if (appendInfo.producerId() == RecordBatch.NO_PRODUCER_ID) {
            throw new IllegalArgumentException(String.format("Invalid producer id %s passed to update for %s",
                    appendInfo.producerId(), topicPartition));
        }

        ProducerStateEntry updatedEntry = appendInfo.toEntry();

        producers.compute(appendInfo.producerId(), (pid, stateEntry) -> {
            if (stateEntry == null) {
                stateEntry = updatedEntry;
            } else {
                stateEntry.update(updatedEntry);
            }
            return stateEntry;
        });

        for (TxnMetadata txn : appendInfo.startedTransactions()) {
            ongoingTxns.put(txn.firstOffset(), txn);
        }
    }

    public void updateTxnIndex(CompletedTxn completedTxn, long lastStableOffset) {
        if (completedTxn.isAborted()) {
            abortedIndexList.add(new AbortedTxn(completedTxn.producerId(), completedTxn.firstOffset(),
                    completedTxn.lastOffset(), lastStableOffset));
        }
    }

    public void completeTxn(CompletedTxn completedTxn) {
        TxnMetadata txnMetadata = ongoingTxns.remove(completedTxn.firstOffset());
        if (txnMetadata == null) {
            String msg = String.format("Attempted to complete transaction %s on partition "
                    + "%s which was not started.", completedTxn, topicPartition);
            throw new IllegalArgumentException(msg);
        }
    }

    public List<FetchResponse.AbortedTransaction> getAbortedIndexList(long fetchOffset) {
        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();
        for (AbortedTxn abortedTxn : abortedIndexList) {
            if (abortedTxn.lastOffset() >= fetchOffset) {
                abortedTransactions.add(
                        new FetchResponse.AbortedTransaction(abortedTxn.producerId(), abortedTxn.firstOffset()));
            }
        }
        return abortedTransactions;
    }

}
