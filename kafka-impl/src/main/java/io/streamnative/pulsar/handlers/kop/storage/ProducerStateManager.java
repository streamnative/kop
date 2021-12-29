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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.kop.KafkaPositionImpl;
import io.streamnative.pulsar.handlers.kop.storage.snapshot.AbortedTxnEntry;
import io.streamnative.pulsar.handlers.kop.storage.snapshot.PidSnapshotMap;
import io.streamnative.pulsar.handlers.kop.storage.snapshot.ProducerSnapshotEntry;
import io.streamnative.pulsar.handlers.kop.systopic.SystemTopicProducerStateClient;
import io.streamnative.pulsar.handlers.kop.utils.timer.SystemTimer;
import io.streamnative.pulsar.handlers.kop.utils.timer.TimerTask;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.api.MessageId;

/**
 * AbortedTxn is used cache the aborted index.
 */
@Data
@Accessors(fluent = true)
@AllArgsConstructor
class AbortedTxn {
    private final Long producerId;
    private final Long firstOffset;
    private final Long lastOffset;
    private final Long lastStableOffset;
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
 * ProducerStateManage state.
 */
enum State {
    INIT, // init
    RECOVERING, // start recover
    READY, // finish recover
    RECOVER_ERROR // failed to recover
}

/**
 * Producer state manager.
 */
@Slf4j
public class ProducerStateManager extends TimerTask {

    private volatile State state;

    private final String topicPartition;

    @Getter
    @VisibleForTesting
    protected KafkaPositionImpl lastPosition;

    private final int maxProducerIdExpirationMs;

    private final Time time;

    private final SystemTimer timer;

    // snapshot and recover
    private final CompletableFuture<SystemTopicClient.Writer<PidSnapshotMap>> snapshotWriter;
    private final CompletableFuture<SystemTopicClient.Reader<PidSnapshotMap>> snapshotReader;

    private final Map<Long, ProducerStateEntry> producers = Maps.newConcurrentMap();

    // ongoing transactions sorted by the first offset of the transaction
    private final TreeMap<Long, TxnMetadata> ongoingTxns = Maps.newTreeMap();
    private final List<AbortedTxn> abortedIndexList = new ArrayList<>();

    private static final AtomicReferenceFieldUpdater<ProducerStateManager, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ProducerStateManager.class, State.class, "state");


    public ProducerStateManager(String topicPartition,
                                int maxProducerIdExpirationMs,
                                SystemTopicProducerStateClient systemTopicProducerStateClient,
                                Time time,
                                SystemTimer timer,
                                int takeSnapshotIntervalTime) {
        super(takeSnapshotIntervalTime);
        this.topicPartition = topicPartition;
        this.time = time;
        this.timer = timer;
        this.maxProducerIdExpirationMs = maxProducerIdExpirationMs;
        this.snapshotWriter = systemTopicProducerStateClient.newWriterAsync();
        this.snapshotReader = systemTopicProducerStateClient.newReaderAsync();
        this.state = State.INIT;
    }

    public boolean isEmpty() {
        return this.producers.isEmpty();
    }

    public State state() {
        return this.state;
    }

    public void transitionTo(State state) {
        STATE_UPDATER.set(this, state);
    }

    public boolean transitionToRecovering() {
        return STATE_UPDATER.compareAndSet(this, State.INIT, State.RECOVERING);
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

    public void updateEndPosition(KafkaPositionImpl position) {
        this.lastPosition = position;
    }

    /**
     * Returns the last offset of this map.
     */
    public Long mapEndOffset() {
        return lastPosition.getOffset();
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

    private Boolean isProducerExpired(Long currentTimeMs, ProducerStateEntry producerState) {
        return !producerState.currentTxnFirstOffset().isPresent()
                && currentTimeMs - producerState.lastTimestamp() >= maxProducerIdExpirationMs;
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
        this.lastPosition = KafkaPositionImpl.EARLIEST;
    }

    private PidSnapshotMap getSnapshot(Map<Long, ProducerStateEntry> entries,
                                         KafkaPositionImpl lastPosition) {
        PidSnapshotMap pidSnapshotMap = new PidSnapshotMap();
        ProducerSnapshotEntry[] producerEntries = new ProducerSnapshotEntry[entries.size()];
        AtomicInteger entryIndex = new AtomicInteger(0);
        entries.forEach((pid, entry) -> {
            ProducerSnapshotEntry.ProducerSnapshotEntryBuilder builder = ProducerSnapshotEntry.builder();
            builder.producerId(pid)
                    .producerEpoch(entry.producerEpoch())
                    .lastSequence(entry.lastSeq())
                    .lastOffset(entry.lastDataOffset())
                    .offsetDelta(entry.lastOffsetDelta())
                    .timestamp(entry.lastTimestamp())
                    .coordinatorEpoch(entry.coordinatorEpoch())
                    .currentTxnFirstOffset(entry.currentTxnFirstOffset().orElse(-1L));
            producerEntries[entryIndex.getAndIncrement()] = builder.build();
        });
        List<AbortedTxnEntry> abortedTxnEntries = new ArrayList<>();
        for (AbortedTxn abortedTxn : abortedIndexList) {
            AbortedTxnEntry abortedTxnEntry = new AbortedTxnEntry(
                    abortedTxn.producerId(),
                    abortedTxn.firstOffset(),
                    abortedTxn.lastOffset(),
                    abortedTxn.lastStableOffset());
            abortedTxnEntries.add(abortedTxnEntry);
        }
        pidSnapshotMap.setAbortedTxnEntries(abortedTxnEntries);
        pidSnapshotMap.setProducerEntries(Arrays.asList(producerEntries));
        pidSnapshotMap.setSnapshotOffset(lastPosition.getOffset());
        pidSnapshotMap.setEntryId(lastPosition.getEntryId());
        pidSnapshotMap.setLedgerId(lastPosition.getLedgerId());
        return pidSnapshotMap;
    }

    public CompletableFuture<MessageId> takeSnapshot() {
        return snapshotWriter.thenComposeAsync(writer -> writer.writeAsync(getSnapshot(producers, lastPosition)));
    }

    public CompletableFuture<Void> loadFromSnapshot() {
        return snapshotReader.thenComposeAsync(reader -> {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            reader.readNextAsync()
                    .whenComplete(((message, throwable) -> {
                        if (throwable != null) {
                            log.error("Failed to read snapshot log.", throwable.getCause());
                            completableFuture.completeExceptionally(throwable.getCause());
                            return;
                        }
                        if (message != null) {
                            try {
                                PidSnapshotMap pidSnapshotMap = message.getValue();
                                if (log.isDebugEnabled()) {
                                    log.debug("Load snapshot [{}]", pidSnapshotMap);
                                }
                                this.lastPosition = new KafkaPositionImpl(
                                        pidSnapshotMap.getSnapshotOffset(),
                                        pidSnapshotMap.getLedgerId(),
                                        pidSnapshotMap.getEntryId());
                                List<ProducerStateEntry> stateEntryList =
                                        readProducerStateEntryListFromSnapshot(pidSnapshotMap);
                                Long currentTime = time.milliseconds();
                                for (ProducerStateEntry entry : stateEntryList) {
                                    if (!isProducerExpired(currentTime, entry)) {
                                        loadProducerEntry(entry);
                                    }
                                }
                                List<AbortedTxn> abortedTxns = readAbortedTxnListFromSnapshot(pidSnapshotMap);
                                this.abortedIndexList.addAll(abortedTxns);
                            } catch (Exception e) {
                                log.error("Failed to decode snapshot log.", e);
                                completableFuture.completeExceptionally(e);
                            }
                        }
                        log.info("Finish load snapshot for topic {}", topicPartition);
                        completableFuture.complete(null);
                    }));
            return completableFuture;
        }).exceptionally(throwable -> {
            log.error("Failed load from snapshot.", throwable);
            return null;
        });
    }

    private List<ProducerStateEntry> readProducerStateEntryListFromSnapshot(PidSnapshotMap pidSnapshotMap) {
        if (pidSnapshotMap == null) {
            throw new UnknownServerException("Snapshot cannot be null.");
        }

        List<ProducerStateEntry> producerStateEntryList = new ArrayList<>();
        for (ProducerSnapshotEntry producerSnapshotEntry : pidSnapshotMap.getProducerEntries()) {
            long producerId = producerSnapshotEntry.getProducerId();
            short producerEpoch = producerSnapshotEntry.getProducerEpoch();
            int seq = producerSnapshotEntry.getLastSequence();
            long offset = producerSnapshotEntry.getLastOffset();
            long timestamp = producerSnapshotEntry.getTimestamp();
            int offsetDelta = producerSnapshotEntry.getOffsetDelta();
            int coordinatorEpoch = producerSnapshotEntry.getCoordinatorEpoch();
            long currentTxnFirstOffset = producerSnapshotEntry.getCurrentTxnFirstOffset();
            Deque<BatchMetadata> lastAppendedDataBatches = new ArrayDeque<>();
//            if (offset >= 0) {
//                lastAppendedDataBatches.add(new BatchMetadata(seq, offset, offsetDelta, timestamp));
//            }
            lastAppendedDataBatches.add(new BatchMetadata(seq, offset, offsetDelta, timestamp));
            Optional<Long> currentFirstOffset = currentTxnFirstOffset >= 0
                    ? Optional.of(currentTxnFirstOffset) : Optional.empty();
            ProducerStateEntry entry = new ProducerStateEntry(producerId, lastAppendedDataBatches, producerEpoch,
                    coordinatorEpoch, timestamp, currentFirstOffset);
            producerStateEntryList.add(entry);
        }
        return producerStateEntryList;
    }

    private List<AbortedTxn> readAbortedTxnListFromSnapshot(PidSnapshotMap pidSnapshotMap) {
        List<AbortedTxn> abortedTxns = new ArrayList<>();
        for (AbortedTxnEntry abortedTxnEntry : pidSnapshotMap.getAbortedTxnEntries()) {
            AbortedTxn abortedTxn = new AbortedTxn(
                    abortedTxnEntry.producerId(),
                    abortedTxnEntry.firstOffset(),
                    abortedTxnEntry.lastOffset(),
                    abortedTxnEntry.lastStableOffset());
            abortedTxns.add(abortedTxn);
        }
        return abortedTxns;
    }

    private void loadProducerEntry(ProducerStateEntry entry) {
        Long producerId = entry.producerId();
        producers.put(producerId, entry);
        entry.currentTxnFirstOffset().ifPresent(offset -> ongoingTxns.put(offset, new TxnMetadata(producerId, offset)));
    }

    @Override
    public void run() {
        if (this.state().equals(State.READY)) {
            takeSnapshotByTimeout();
        }
    }

    private void takeSnapshotByTimeout() {
        takeSnapshot();
        newTimeout();
    }

    protected void newTimeout() {
        this.timer.add(this);
    }
}
