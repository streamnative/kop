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

import com.google.common.collect.Maps;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingDeque;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchResponse;

/**
 * Producer state manager.
 */
@Slf4j
public class ProducerStateManager {

    private final String topicPartition;
    private final int maxProducerIdExpirationMs;

    private final Map<Long, ProducerStateEntry> producers = Maps.newConcurrentMap();
    private Long lastMapOffset = 0L;

    // ongoing transactions sorted by the first offset of the transaction
    private final TreeMap<Long, TxnMetadata> ongoingTxns = Maps.newTreeMap();
    private final List<AbortedTxn> abortedIndexList = new ArrayList<>();

    /**
     * AppendOrigin is used mark the data origin.
     */
    public enum AppendOrigin {
        Coordinator,
        Client
    }

    /**
     * TxnMetadata represents the ongoing transaction.
     */
    @EqualsAndHashCode
    public static class TxnMetadata {
        private final long producerId;
        private final long firstOffset;
        private long lastOffset;

        public TxnMetadata(long producerId, long firstOffset) {
            this.producerId = producerId;
            this.firstOffset = firstOffset;
        }

        public TxnMetadata(long producerId, long firstOffset, long lastOffset) {
            this.producerId = producerId;
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
        }

    }

    /**
     * BatchMetadata is used to check the message duplicate.
     */
    @AllArgsConstructor
    public static class BatchMetadata {

        private final Integer lastSeq;
        private final Long lastOffset;
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
     * the batchMetadata is ordered such that the batch with the lowest sequence is at the head of the queue while the
     * batch with the highest sequence is at the tail of the queue. We will retain at most ProducerStateEntry.
     * NumBatchesToRetain elements in the queue. When the queue is at capacity, we remove the first element to make
     * space for the incoming batch.
      */
    @AllArgsConstructor
    @Data
    public static class ProducerStateEntry {

        private static final Integer NumBatchesToRetain = 5;

        private Long producerId;
        private Deque<BatchMetadata> batchMetadata;
        private Short producerEpoch;
        private Integer coordinatorEpoch;
        private Long lastTimestamp;
        private Optional<Long> currentTxnFirstOffset;

        private boolean isEmpty() {
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
                return batchMetadata.getLast().lastSeq;
            }
        }

        public Long lastDataOffset() {
            if (isEmpty()) {
                return -1L;
            } else {
                return batchMetadata.getLast().lastOffset;
            }
        }

        public Integer lastOffsetDelta() {
            if (isEmpty()) {
                return 0;
            } else {
                return batchMetadata.getLast().offsetDelta;
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
            this.currentTxnFirstOffset = nextEntry.currentTxnFirstOffset;
            this.lastTimestamp = nextEntry.lastTimestamp;
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
                    firstSeq == batchMetadata.firstSeq() && lastSeq.equals(batchMetadata.lastSeq)).findFirst();
        }

        public static ProducerStateEntry empty(Long producerId){
            return new ProducerStateEntry(producerId, new LinkedBlockingDeque<>(),
                    RecordBatch.NO_PRODUCER_EPOCH, -1, RecordBatch.NO_TIMESTAMP, Optional.empty());
        }

        @Override
        public String toString() {
            return "ProducerStateEntry{"
                    + "producerId=" + producerId
                    + ", producerEpoch=" + producerEpoch
                    + ", currentTxnFirstOffset=" + currentTxnFirstOffset
                    + ", coordinatorEpoch=" + coordinatorEpoch
                    + ", lastTimestamp=" + lastTimestamp
                    + ", batchMetadata=" + batchMetadata
                    + '}';
        }
    }

    /**
     * CompletedTxn.
     */
    @ToString
    @AllArgsConstructor
    @Data
    public static class CompletedTxn {
        private Long producerId;
        private Long firstOffset;
        private Long lastOffset;
        private Boolean isAborted;
    }

    /**
     * AbortedTxn is used cache the aborted index.
     */
    @AllArgsConstructor
    private static class AbortedTxn {

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

        public ByteBuffer toByteBuffer() {
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

    /**
     * Producer append info.
     */
    public static class ProducerAppendInfo {

        private final String topicPartition;
        private final Long producerId;
        private final ProducerStateEntry currentEntry;
        private final AppendOrigin origin;
        private final List<TxnMetadata> transactions = Lists.newArrayList();
        private ProducerStateEntry updatedEntry;

        public ProducerAppendInfo(String topicPartition, Long producerId,
                                  ProducerStateEntry currentEntry, AppendOrigin origin) {
            this.topicPartition = topicPartition;
            this.producerId = producerId;
            this.currentEntry = currentEntry;
            this.origin = origin;

            resetUpdatedEntry();
        }

        private void maybeValidateDataBatch(Short producerEpoch, Integer firstSeq) {
            checkProducerEpoch(producerEpoch);
            if (origin.equals(AppendOrigin.Client)) {
                checkSequence(producerEpoch, firstSeq);
            }
        }

        private void checkProducerEpoch(Short producerEpoch) {
            if (producerEpoch < updatedEntry.producerEpoch) {
                String message = String.format("Producer's epoch in %s is %s, which is smaller than the last seen "
                        + "epoch %s", topicPartition, producerEpoch, currentEntry.producerEpoch);
                throw new IllegalArgumentException(message);
            }
        }

        private void checkSequence(Short producerEpoch, Integer appendFirstSeq) {
            log.info("append data batch checkSequence producerEpoch: {}, appendFirstSeq: {}",
                    producerEpoch, appendFirstSeq);
            if (!producerEpoch.equals(updatedEntry.producerEpoch)) {
                if (appendFirstSeq != 0 && updatedEntry.producerEpoch != RecordBatch.NO_PRODUCER_EPOCH) {
                    String msg = String.format("Invalid sequence number for new epoch in partition %s: %s "
                            + "(request epoch), %s (seq. number)", topicPartition, producerEpoch, appendFirstSeq);
                    throw new OutOfOrderSequenceException(msg);
                }
            } else {
                int currentLastSeq;
                if (!updatedEntry.isEmpty()) {
                    currentLastSeq = updatedEntry.lastSeq();
                } else if (producerEpoch.equals(currentEntry.producerEpoch)) {
                    currentLastSeq = currentEntry.lastSeq();
                } else {
                    currentLastSeq = RecordBatch.NO_SEQUENCE;
                }

                // If there is no current producer epoch (possibly because all producer records have been deleted due to
                // retention or the DeleteRecords API) accept writes with any sequence number
                if (!(currentEntry.producerEpoch == RecordBatch.NO_PRODUCER_EPOCH
                        || inSequence(currentLastSeq, appendFirstSeq))) {
                    String msg = String.format("Out of order sequence number for producerId %s in partition %s: %s "
                                    + "(incoming seq. number), %s (current end sequence number)",
                            currentEntry.producerId, topicPartition, appendFirstSeq, currentLastSeq);
                    throw new OutOfOrderSequenceException(msg);
                }

            }
        }

        private Boolean inSequence(Integer lastSeq, Integer nextSeq) {
            log.info("append data batch lastSeq: {}, nextSeq: {}", lastSeq, nextSeq);
            return nextSeq == lastSeq + 1L || (nextSeq == 0 && lastSeq == Integer.MAX_VALUE);
        }

        public Optional<CompletedTxn> append(RecordBatch batch, Optional<Long> firstOffset) {
            if (batch.isControlBatch()) {
                Iterator<Record> recordIterator = batch.iterator();
                if (recordIterator.hasNext()) {
                    Record record = recordIterator.next();
                    EndTransactionMarker endTxnMarker = EndTransactionMarker.deserialize(record);
                    return appendEndTxnMarker(
                            endTxnMarker, batch.producerEpoch(), batch.baseOffset(), record.timestamp());
                } else {
                    // An empty control batch means the entire transaction has been cleaned from the log,
                    // so no need to append
                    return Optional.empty();
                }
            } else {
                appendDataBatch(batch.producerEpoch(), batch.baseSequence(), batch.lastSequence(), batch.maxTimestamp(),
                        firstOffset.orElse(batch.baseOffset()), batch.lastOffset(), batch.isTransactional());
                return Optional.empty();
            }
        }

        public void appendDataBatch(Short epoch, Integer firstSeq, Integer lastSeq, Long lastTimestamp,
                            Long firstOffset, Long lastOffset, Boolean isTransactional) {
            log.info("append data batch epoch: {}, firstSeq: {}, lastSeq: {}, firstOffset: {}, lastOffset: {}",
                    epoch, firstSeq, lastSeq, firstOffset, lastOffset);
            maybeValidateDataBatch(epoch, firstSeq);
            updatedEntry.addBatch(epoch, lastSeq, lastOffset, (int) (lastOffset - firstOffset), lastTimestamp);

            if (updatedEntry.currentTxnFirstOffset.isPresent()) {
                if (!isTransactional) {
                    // Received a non-transactional message while a transaction is active
                    String msg = String.format("Expected transactional write from producer %s at offset %s in "
                                    + "partition %s", producerId, firstOffset, topicPartition);
                    throw new InvalidTxnStateException(msg);
                }
            } else {
                if (isTransactional) {
                    updatedEntry.currentTxnFirstOffset = Optional.of(firstOffset);
                    transactions.add(new TxnMetadata(producerId, firstOffset));
                }
            }
        }

        public Optional<CompletedTxn> appendEndTxnMarker(
                EndTransactionMarker endTxnMarker,
                Short producerEpoch,
                Long offset,
                Long timestamp) {
            checkProducerEpoch(producerEpoch);

            // Only emit the `CompletedTxn` for non-empty transactions. A transaction marker
            // without any associated data will not have any impact on the last stable offset
            // and would not need to be reflected in the transaction index.
            Optional<CompletedTxn> completedTxn = Optional.empty();
            if (updatedEntry.currentTxnFirstOffset.isPresent()) {
                completedTxn = Optional.of(
                        new CompletedTxn(producerId, updatedEntry.currentTxnFirstOffset.get(), offset,
                                endTxnMarker.controlType() == ControlRecordType.ABORT));
            }

            updatedEntry.maybeUpdateProducerEpoch(producerEpoch);
            updatedEntry.currentTxnFirstOffset = Optional.empty();
            updatedEntry.lastTimestamp = timestamp;
            return completedTxn;
        }

        public ProducerStateEntry toEntry() {
            return updatedEntry;
        }

        public List<TxnMetadata> startedTransactions() {
            return transactions;
        }

        private void resetUpdatedEntry() {
            updatedEntry = ProducerStateEntry.empty(producerId);
            updatedEntry.producerEpoch = currentEntry.producerEpoch;
            updatedEntry.coordinatorEpoch = currentEntry.coordinatorEpoch;
            updatedEntry.lastTimestamp = currentEntry.lastTimestamp;
            updatedEntry.currentTxnFirstOffset = currentEntry.currentTxnFirstOffset;
        }

        public void resetOffset(long baseOffset, boolean isTransactional) {
            log.info("append data batch reset offset: {}", baseOffset);
            short producerEpoch = updatedEntry.producerEpoch;
            BatchMetadata batchMetadata = updatedEntry.batchMetadata.pollFirst();
            if (batchMetadata == null) {
                return;
            }
            resetUpdatedEntry();
            transactions.clear();
            int offsetDelta = batchMetadata.lastSeq - batchMetadata.firstSeq();
            appendDataBatch(producerEpoch, batchMetadata.firstSeq(), batchMetadata.lastSeq, batchMetadata.timestamp,
                    baseOffset, baseOffset + offsetDelta, isTransactional);
        }

        @Override
        public String toString() {
            return "ProducerAppendInfo("
                    + "producerId=" + producerId + ", "
                    + "producerEpoch=" + updatedEntry.producerEpoch + ", "
                    + "firstSequence=" + updatedEntry.firstSeq() + ", "
                    + "lastSequence=" + updatedEntry.lastSeq() + ", "
                    + "currentTxnFirstOffset=" + updatedEntry.currentTxnFirstOffset + ", "
                    + "coordinatorEpoch=" + updatedEntry.coordinatorEpoch + ", "
                    + "lastTimestamp=" + updatedEntry.lastTimestamp + ", "
                    + "startedTransactions=" + transactions + ")";
        }

    }

    /**
     * Analyze result.
     */
    @Data
    @AllArgsConstructor
    public static class AnalyzeResult {
        private Map<Long, ProducerAppendInfo> appendInfoMap;
        private List<CompletedTxn> completedTxnList;
        private Optional<BatchMetadata> batchMetadata;
    }

    public ProducerStateManager(String topicPartition, int maxProducerIdExpirationMs) {
        this.topicPartition = topicPartition;
        this.maxProducerIdExpirationMs = maxProducerIdExpirationMs;
    }

    public ProducerAppendInfo prepareUpdate(Long producerId, AppendOrigin origin) {
        ProducerStateEntry currentEntry = lastEntry(producerId).orElse(ProducerStateEntry.empty(producerId));
        return new ProducerAppendInfo(topicPartition, producerId, currentEntry, origin);
    }

    public AnalyzeResult analyzeAndValidateProducerState(MemoryRecords records,
                                                         Optional<Long> firstOffset,
                                                         AppendOrigin origin) {
        Map<Long, ProducerAppendInfo> updatedProducers = Maps.newHashMap();
        List<CompletedTxn> completedTxns = Lists.newArrayList();

        for (RecordBatch batch : records.batches()) {
            if (batch.hasProducerId()) {
                Optional<ProducerStateEntry> maybeLastEntry = lastEntry(batch.producerId());

                // if this is a client produce request, there will be up to 5 batches which could have been duplicated.
                // If we find a duplicate, we return the metadata of the appended batch to the client.
                if (maybeLastEntry.isPresent()) {
                    Optional<BatchMetadata> maybeDuplicate = maybeLastEntry.get().findDuplicateBatch(batch);
                    if (maybeDuplicate.isPresent()) {
                        return new AnalyzeResult(updatedProducers, completedTxns, maybeDuplicate);
                    }
                }
                // We cache offset metadata for the start of each transaction. This allows us to
                // compute the last stable offset without relying on additional index lookups.
                Optional<CompletedTxn> maybeCompletedTxn =
                        updateProducers(batch, updatedProducers, firstOffset, origin);
                maybeCompletedTxn.ifPresent(completedTxns::add);
            }
        }
        return new AnalyzeResult(updatedProducers, completedTxns, Optional.empty());
    }

    private Optional<CompletedTxn> updateProducers(RecordBatch batch,
                                                   Map<Long, ProducerAppendInfo> producers,
                                                   Optional<Long> firstOffset,
                                                   AppendOrigin origin) {
        Long producerId = batch.producerId();
        ProducerAppendInfo appendInfo = producers.computeIfAbsent(producerId, pid -> prepareUpdate(producerId, origin));
        return appendInfo.append(batch, firstOffset);
    }


    /**
     * Compute the last stable offset of a completed transaction, but do not yet mark the transaction complete.
     * That will be done in `completeTxn` below. This is used to compute the LSO that will be appended to the
     * transaction index, but the completion must be done only after successfully appending to the index.
     */
    public long lastStableOffset(CompletedTxn completedTxn) {
        for (TxnMetadata txnMetadata : ongoingTxns.values()) {
            if (txnMetadata.producerId != completedTxn.producerId) {
                return txnMetadata.firstOffset;
            }
        }
        return completedTxn.lastOffset + 1;
    }

    public Optional<Long> firstUndecidedOffset() {
        Map.Entry<Long, TxnMetadata> entry = ongoingTxns.firstEntry();
        if (entry == null) {
            return Optional.empty();
        }
        return Optional.of(entry.getValue().firstOffset);
    }

    private Boolean isProducerExpired(Long currentTimeMs, ProducerStateEntry producerState) {
        return !producerState.currentTxnFirstOffset.isPresent()
                && currentTimeMs - producerState.lastTimestamp >= maxProducerIdExpirationMs;
    }

    /**
     * Expire any producer ids which have been idle longer than the configured maximum expiration timeout.
     */
    public void removeExpiredProducers(Long currentTimeMs) {
        for (Map.Entry<Long, ProducerStateEntry> entry : producers.entrySet()) {
            if (isProducerExpired(currentTimeMs, entry.getValue())) {
                producers.remove(entry.getKey());
            }
        }
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
        if (appendInfo.producerId == RecordBatch.NO_PRODUCER_ID) {
            throw new IllegalArgumentException("Invalid producer id ${appendInfo.producerId} passed to update for "
                    + "partition " + topicPartition);
        }

        log.info("Updated producer {} state to {}", appendInfo.producerId, appendInfo);
        ProducerStateEntry updatedEntry = appendInfo.toEntry();

        producers.compute(appendInfo.producerId, (pid, stateEntry) -> {
            if (stateEntry == null) {
                stateEntry = updatedEntry;
            } else {
                stateEntry.update(updatedEntry);
            }
            return stateEntry;
        });

        for (TxnMetadata txn : appendInfo.startedTransactions()) {
            ongoingTxns.put(txn.firstOffset, txn);
        }
    }

    public void completeTxn(CompletedTxn completedTxn) {
        TxnMetadata txnMetadata = ongoingTxns.remove(completedTxn.firstOffset);
        if (txnMetadata == null) {
            String msg = String.format("Attempted to complete transaction %s on partition "
                    + "%s which was not started.", completedTxn, topicPartition);
            throw new IllegalArgumentException(msg);
        }

        txnMetadata.lastOffset = completedTxn.lastOffset;

        if (completedTxn.isAborted) {
            abortedIndexList.add(new AbortedTxn(completedTxn.producerId, completedTxn.firstOffset,
                    completedTxn.lastOffset, lastStableOffset(completedTxn)));
        }
    }

    public void updateMapEndOffset(long offset) {
        lastMapOffset = offset;
    }

    public List<FetchResponse.AbortedTransaction> getAbortedIndexList(long fetchOffset) {
        List<FetchResponse.AbortedTransaction> abortedTransactions = new ArrayList<>();
        for (AbortedTxn abortedTxn : abortedIndexList) {
            if (abortedTxn.lastOffset >= fetchOffset) {
                abortedTransactions.add(
                        new FetchResponse.AbortedTransaction(abortedTxn.producerId, abortedTxn.firstOffset));
            }
        }
        return abortedTransactions;
    }

    /**
     * Returns the last offset of this map.
     */
    public Long mapEndOffset() {
        return lastMapOffset;
    }

    /**
     * Get a copy of the active producers.
     */
    public Map<Long, ProducerStateEntry> activeProducers() {
        return producers;
    }

    /**
     * Truncate the producer id mapping and remove all snapshots. This resets the state of the mapping.
     */
    public void truncate() {
        producers.clear();
        ongoingTxns.clear();
        lastMapOffset = 0L;
    }

}
