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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchResponse;

/**
 * Producer state manager.
 */
@Slf4j
public class ProducerStateManager {

    @Getter
    private final String topicPartition;
    private final String kafkaTopicUUID;

    private final Map<Long, ProducerStateEntry> producers = Maps.newConcurrentMap();

    // ongoing transactions sorted by the first offset of the transaction
    private final TreeMap<Long, TxnMetadata> ongoingTxns = Maps.newTreeMap();
    private final List<AbortedTxn> abortedIndexList = new ArrayList<>();

    private final ProducerStateManagerSnapshotBuffer producerStateManagerSnapshotBuffer;

    private final int kafkaTxnProducerStateTopicSnapshotIntervalSeconds;
    private final int kafkaTxnPurgeAbortedTxnIntervalSeconds;

    private volatile long mapEndOffset = -1;

    private long lastSnapshotTime;
    private long lastPurgeAbortedTxnTime;

    private volatile long abortedTxnsPurgeOffset = -1;

    public ProducerStateManager(String topicPartition,
                                String kafkaTopicUUID,
                                ProducerStateManagerSnapshotBuffer producerStateManagerSnapshotBuffer,
                                int kafkaTxnProducerStateTopicSnapshotIntervalSeconds,
                                int kafkaTxnPurgeAbortedTxnIntervalSeconds) {
        this.topicPartition = topicPartition;
        this.kafkaTopicUUID = kafkaTopicUUID;
        this.producerStateManagerSnapshotBuffer = producerStateManagerSnapshotBuffer;
        this.kafkaTxnProducerStateTopicSnapshotIntervalSeconds = kafkaTxnProducerStateTopicSnapshotIntervalSeconds;
        this.kafkaTxnPurgeAbortedTxnIntervalSeconds = kafkaTxnPurgeAbortedTxnIntervalSeconds;
        this.lastSnapshotTime = System.currentTimeMillis();
        this.lastPurgeAbortedTxnTime = System.currentTimeMillis();
    }

    public CompletableFuture<Void> recover(PartitionLog partitionLog, Executor executor) {
        return producerStateManagerSnapshotBuffer
                .readLatestSnapshot(topicPartition)
                .thenCompose(snapshot -> applySnapshotAndRecover(snapshot, partitionLog, executor));
    }

    private CompletableFuture<Void> applySnapshotAndRecover(ProducerStateManagerSnapshot snapshot,
                                                            PartitionLog partitionLog,
                                                            Executor executor) {
        if (snapshot != null && kafkaTopicUUID != null
                && !kafkaTopicUUID.equals(snapshot.getTopicUUID())) {
            log.info("The latest snapshot for topic {} was for UUID {} that is different from {}. "
                    + "Ignoring it (topic has been re-created)", topicPartition, snapshot.getTopicUUID(),
                    kafkaTopicUUID);
            snapshot = null;
        }
        long offSetPosition = 0;
        synchronized (abortedIndexList) {
            this.abortedIndexList.clear();
            this.producers.clear();
            this.ongoingTxns.clear();
            if (snapshot != null) {
                this.abortedIndexList.addAll(snapshot.getAbortedIndexList());
                this.producers.putAll(snapshot.getProducers());
                this.ongoingTxns.putAll(snapshot.getOngoingTxns());
                this.mapEndOffset = snapshot.getOffset();
                offSetPosition = snapshot.getOffset();
                log.info("Recover topic {} from offset {}", topicPartition, offSetPosition);
                log.info("ongoingTxns transactions after recovery {}", snapshot.getOngoingTxns());
                log.info("Aborted transactions after recovery {}", snapshot.getAbortedIndexList());
            } else {
                log.info("No snapshot found for topic {}, recovering from the beginning", topicPartition);
            }
        }
        long startRecovery = System.currentTimeMillis();
        // recover from log
        CompletableFuture<Void> result =  partitionLog
                .recoverTxEntries(offSetPosition, executor)
                .thenApply(numEntries -> {
                    log.info("Recovery of {} finished. Scanned {} entries, time {} ms, new mapEndOffset {}",
                            topicPartition,
                            numEntries,
                            System.currentTimeMillis() - startRecovery,
                            mapEndOffset);
                    return null;
                });

        return result;
    }

    @VisibleForTesting
    public CompletableFuture<ProducerStateManagerSnapshot> takeSnapshot(Executor executor) {
        CompletableFuture<ProducerStateManagerSnapshot> result = new CompletableFuture<>();
        executor.execute(new SafeRunnable() {
            @Override
            public void safeRun() {
                if (mapEndOffset == -1) {
                    result.complete(null);
                    return;
                }
                ProducerStateManagerSnapshot snapshot = getProducerStateManagerSnapshot();
                log.info("Taking snapshot for {} at {}", topicPartition, snapshot);
                producerStateManagerSnapshotBuffer
                        .write(snapshot)
                        .whenComplete((res, error) -> {
                            if (error != null) {
                                result.completeExceptionally(error);
                            } else {
                                log.info("Snapshot for {} ({}) taken at offset {}",
                                        topicPartition, kafkaTopicUUID, snapshot.getOffset());
                                result.complete(snapshot);
                            }
                        });
            }
        });
        return result;
    }

    void maybeTakeSnapshot(Executor executor) {
        if (mapEndOffset == -1) {
            return;
        }
        long now = System.currentTimeMillis();
        long deltaFromLast = (now - lastSnapshotTime) / 1000;
        if (log.isDebugEnabled()) {
            log.debug("maybeTakeSnapshot deltaFromLast {} vs kafkaTxnProducerStateTopicSnapshotIntervalSeconds {} ",
                    deltaFromLast, kafkaTxnProducerStateTopicSnapshotIntervalSeconds);
        }
        if (deltaFromLast < kafkaTxnProducerStateTopicSnapshotIntervalSeconds) {
            return;
        }
        lastSnapshotTime = now;

        takeSnapshot(executor);
    }

    void updateAbortedTxnsPurgeOffset(long abortedTxnsPurgeOffset) {
        if (log.isDebugEnabled()) {
            log.debug("{} updateAbortedTxnsPurgeOffset {}", topicPartition, abortedTxnsPurgeOffset);
        }
        if (abortedTxnsPurgeOffset < 0) {
            return;
        }
        this.abortedTxnsPurgeOffset = abortedTxnsPurgeOffset;
    }

    long maybePurgeAbortedTx() {
        if (mapEndOffset == -1) {
            return 0;
        }
        long now = System.currentTimeMillis();
        long deltaFromLast = (now - lastPurgeAbortedTxnTime) / 1000;
        if (deltaFromLast < kafkaTxnPurgeAbortedTxnIntervalSeconds) {
            return 0;
        }
        lastPurgeAbortedTxnTime = now;
        return executePurgeAbortedTx();
    }

    @VisibleForTesting
    long executePurgeAbortedTx() {
        return purgeAbortedTxns(abortedTxnsPurgeOffset);
    }

    private ProducerStateManagerSnapshot getProducerStateManagerSnapshot() {
        ProducerStateManagerSnapshot snapshot;
        synchronized (abortedIndexList) {
            snapshot = new ProducerStateManagerSnapshot(
                    topicPartition,
                    kafkaTopicUUID,
                    mapEndOffset,
                    new HashMap<>(producers),
                    new TreeMap<>(ongoingTxns),
                    new ArrayList<>(abortedIndexList));
        }
        if (log.isDebugEnabled()) {
            log.debug("Snapshot for {}: {}", topicPartition, snapshot);
        }
        return snapshot;
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
            if (completedTxn.producerId() != txnMetadata.producerId()) {
                return txnMetadata.firstOffset();
            }
        }
        return completedTxn.lastOffset() + 1;
    }

    public Optional<Long> firstUndecidedOffset() {
        Map.Entry<Long, TxnMetadata> entry = ongoingTxns.firstEntry();
        if (log.isDebugEnabled()) {
            log.debug("firstUndecidedOffset {} (ongoingTxns {})", entry, ongoingTxns);
        }
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

    public void updateMapEndOffset(long mapEndOffset) {
        this.mapEndOffset = mapEndOffset;
    }

    public void updateTxnIndex(CompletedTxn completedTxn, long lastStableOffset) {
        if (completedTxn.isAborted()) {
            AbortedTxn abortedTxn = new AbortedTxn(completedTxn.producerId(), completedTxn.firstOffset(),
                    completedTxn.lastOffset(), lastStableOffset);
            if (log.isDebugEnabled()) {
                log.debug("Adding new AbortedTxn {}", abortedTxn);
            }
            synchronized (abortedIndexList) {
                abortedIndexList.add(abortedTxn);
            }
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

    public boolean hasSomeAbortedTransactions() {
        return !abortedIndexList.isEmpty();
    }

    public long purgeAbortedTxns(long offset) {
        AtomicLong count = new AtomicLong();
        synchronized (abortedIndexList) {
            abortedIndexList.removeIf(tx -> {
                boolean toRemove = tx.lastOffset() < offset;
                if (toRemove) {
                    log.info("Transaction {} can be removed (lastOffset {} < {})", tx, tx.lastOffset(), offset);
                    count.incrementAndGet();
                }
                return toRemove;
            });
            if (!abortedIndexList.isEmpty()) {
                log.info("There are still {} aborted tx on {}", abortedIndexList.size(), topicPartition);
            }
        }
        return count.get();
    }

    public List<FetchResponse.AbortedTransaction> getAbortedIndexList(long fetchOffset) {
        synchronized (abortedIndexList) {
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

    public void handleMissingDataBeforeRecovery(long minOffset, long snapshotOffset) {
        if (mapEndOffset == -1) {
            // empty topic
            return;
        }
        // topic has been trimmed
        if (snapshotOffset < minOffset) {
            log.info("{} handleMissingDataBeforeRecovery mapEndOffset {} snapshotOffset "
                            + "{} minOffset {} RESETTING STATE",
                    topicPartition,
                    mapEndOffset, minOffset);
            // topic was not empty (mapEndOffset has some value)
            // but there is no more data on the topic (trimmed?)
            ongoingTxns.clear();
            abortedIndexList.clear();
            producers.clear();
            mapEndOffset = -1;
        }
    }

}
