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
import io.streamnative.pulsar.handlers.kop.format.DecodeResult;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import io.streamnative.pulsar.handlers.kop.systopic.SystemTopicProducerStateClient;
import io.streamnative.pulsar.handlers.kop.utils.MessageMetadataUtils;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Crc32C;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.util.FutureUtil;

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
    private final Time time;
    private State state;

    // snapshot and recover
    private final CompletableFuture<SystemTopicClient.Writer<ByteBuffer>> snapshotWriter;
    private final CompletableFuture<SystemTopicClient.Reader<ByteBuffer>> snapshotReader;

    private final EntryFormatter entryFormatter;

    private final short producerSnapshotVersion = 1;

    private static final String VersionField = "version";
    private static final String CrcField = "crc";
    private static final String ProducerEntriesField = "producer_entries";
    private static final String SnapshotOffset = "snapshot_offset";

    private static final String ProducerIdField = "producer_id";
    private static final String LastSequenceField = "last_sequence";
    private static final String ProducerEpochField = "epoch";
    private static final String LastOffsetField = "last_offset";
    private static final String OffsetDeltaField = "offset_delta";
    private static final String TimestampField = "timestamp";
    private static final String CoordinatorEpochField = "coordinator_epoch";
    private static final String CurrentTxnFirstOffsetField = "current_txn_first_offset";

    private static final int VersionOffset = 0;
    private static final int CrcOffset = VersionOffset + 2;
    private static final int ProducerEntriesOffset = CrcOffset + 4;

    // snapshot and recover
    private final Schema producerSnapshotEntrySchema = new Schema(
            new Field(ProducerIdField, Type.INT64, "The producer ID"),
            new Field(ProducerEpochField, Type.INT16, "Current epoch of the producer"),
            new Field(LastSequenceField, Type.INT32, "Last written sequence of the producer"),
            new Field(LastOffsetField, Type.INT64, "Last written offset of the producer"),
            new Field(OffsetDeltaField, Type.INT32,
                    "The difference of the last sequence and first sequence in the last written batch"),
            new Field(TimestampField, Type.INT64, "Max timestamp from the last written entry"),
            new Field(CoordinatorEpochField, Type.INT32,
                    "The epoch of the last transaction coordinator to send an end transaction marker"),
            new Field(CurrentTxnFirstOffsetField, Type.INT64,
                    "The first offset of the on-going transaction (-1 if there is none)"));

    private final Schema pidSnapshotMapSchema = new Schema(
            new Field(VersionField, Type.INT16, "Version of the snapshot file"),
            new Field(CrcField, Type.UNSIGNED_INT32, "CRC of the snapshot data"),
            new Field(SnapshotOffset, Type.INT64, "The snapshot offset"),
            new Field(ProducerEntriesField, new ArrayOf(producerSnapshotEntrySchema),
                    "The entries in the producer table"));

    /**
     * ProducerStateManage state.
     */
    private enum State {
        INIT, // init
        RECOVERING, // start recover
        READY, // finish recover
        RECOVER_ERROR // failed to recover
    }

    public ProducerStateManager(
            String topicPartition,
            int maxProducerIdExpirationMs,
            EntryFormatter entryFormatter,
            SystemTopicProducerStateClient systemTopicProducerStateClient,
            Time time) {
        this.topicPartition = topicPartition;
        this.maxProducerIdExpirationMs = maxProducerIdExpirationMs;
        this.time = time;
        this.entryFormatter = entryFormatter;
        this.snapshotWriter = systemTopicProducerStateClient.newWriterAsync();
        this.snapshotReader = systemTopicProducerStateClient.newReaderAsync();
        this.state = State.INIT;
    }

    public ProducerAppendInfo prepareUpdate(Long producerId, PartitionLog.AppendOrigin origin) {
        ProducerStateEntry currentEntry = lastEntry(producerId).orElse(ProducerStateEntry.empty(producerId));
        return new ProducerAppendInfo(topicPartition, producerId, currentEntry, origin);
    }

    private Optional<CompletedTxn> updateProducers(
            RecordBatch batch,
            Map<Long, ProducerAppendInfo> producers,
            Optional<Long> firstOffset,
            PartitionLog.AppendOrigin origin) {
        Long producerId = batch.producerId();
        ProducerAppendInfo appendInfo =
                producers.computeIfAbsent(producerId, pid -> prepareUpdate(producerId, origin));
        return appendInfo.append(batch, firstOffset);
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
        txnMetadata.lastOffset(completedTxn.lastOffset());
    }

    public void updateMapEndOffset(long offset) {
        lastMapOffset = offset;
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

    private ByteBuffer writeSnapshot(Map<Long, ProducerStateEntry> entries, long snapshotOffset) {
        Struct struct = new Struct(pidSnapshotMapSchema);
        struct.set(VersionField, producerSnapshotVersion);
        struct.set(CrcField, 0L); // we'll fill this after writing the entries

        Object[] entriesArray = new Object[entries.size()];
        AtomicInteger entryIndex = new AtomicInteger(0);
        entries.forEach((pid, entry) -> {
            Struct producerEntryStruct = struct.instance(ProducerEntriesField);
            producerEntryStruct
                    .set(ProducerIdField, pid)
                    .set(ProducerEpochField, entry.producerEpoch())
                    .set(LastSequenceField, entry.lastSeq())
                    .set(LastOffsetField, entry.lastDataOffset())
                    .set(OffsetDeltaField, entry.lastOffsetDelta())
                    .set(TimestampField, entry.lastTimestamp())
                    .set(CoordinatorEpochField, entry.coordinatorEpoch())
                    .set(CurrentTxnFirstOffsetField, entry.currentTxnFirstOffset().orElse(-1L));
            entriesArray[entryIndex.getAndIncrement()] = producerEntryStruct;
        });
        struct.set(ProducerEntriesField, entriesArray);
        struct.set(SnapshotOffset, snapshotOffset);

        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);
        buffer.flip();

        // now fill in the CRC
        long crc = Crc32C.compute(buffer, ProducerEntriesOffset, buffer.limit() - ProducerEntriesOffset);
        ByteUtils.writeUnsignedInt(buffer, CrcOffset, crc);
        return buffer;
    }

    public CompletableFuture<MessageId> takeSnapshot() {
        return snapshotWriter.thenComposeAsync(writer -> writer.writeAsync(writeSnapshot(producers, lastMapOffset)));
    }

    public CompletableFuture<Void> loadFromSnapshot() {
        return snapshotReader.thenComposeAsync(reader -> {
            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            reader.readNextAsync()
                    .whenComplete(((message, throwable) -> {
                        if (throwable != null) {
                            log.error("Failed to read snapshot log.",
                                    throwable instanceof CompletionException ? throwable.getCause() : throwable);
                            completableFuture.completeExceptionally(throwable);
                            return;
                        }
                        if (message != null) {
                            try {
                                List<ProducerStateEntry> stateEntryList = readSnapshot(message.getValue());
                                Long currentTime = time.milliseconds();
                                for (ProducerStateEntry entry : stateEntryList) {
                                    if (!isProducerExpired(currentTime, entry)) {
                                        loadProducerEntry(entry);
                                    }
                                }
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

    private List<ProducerStateEntry> readSnapshot(ByteBuffer buffer) {
        try {
            Struct struct = pidSnapshotMapSchema.read(buffer);

            Short version = struct.getShort(VersionField);
            if (version != producerSnapshotVersion) {
                throw new UnknownServerException("Snapshot contained an unknown file version " + version);
            }

            this.lastMapOffset = struct.getLong(SnapshotOffset);
            long crc = struct.getUnsignedInt(CrcField);
            long computedCrc =  Crc32C.compute(buffer, ProducerEntriesOffset, buffer.limit() - ProducerEntriesOffset);
            if (crc != computedCrc) {
                throw new UnknownServerException("Snapshot is corrupt (CRC is no longer valid). Stored crc: "
                        + crc + ". Computed crc: " + computedCrc);
            }

            List<ProducerStateEntry> producerStateEntryList = new ArrayList<>();
            for (Object producerEntryObj : struct.getArray(ProducerEntriesField)) {
                Struct producerEntryStruct = (Struct) producerEntryObj;
                Long producerId = producerEntryStruct.getLong(ProducerIdField);
                Short producerEpoch = producerEntryStruct.getShort(ProducerEpochField);
                Integer seq = producerEntryStruct.getInt(LastSequenceField);
                Long offset = producerEntryStruct.getLong(LastOffsetField);
                Long timestamp = producerEntryStruct.getLong(TimestampField);
                Integer offsetDelta = producerEntryStruct.getInt(OffsetDeltaField);
                Integer coordinatorEpoch = producerEntryStruct.getInt(CoordinatorEpochField);
                Long currentTxnFirstOffset = producerEntryStruct.getLong(CurrentTxnFirstOffsetField);
                Deque<BatchMetadata> lastAppendedDataBatches = new ArrayDeque<>();
                if (offset >= 0) {
                    lastAppendedDataBatches.add(new BatchMetadata(seq, offset, offsetDelta, timestamp));
                }

                Optional<Long> currentFirstOffset = currentTxnFirstOffset >= 0
                        ? Optional.of(currentTxnFirstOffset) : Optional.empty();
                ProducerStateEntry entry = new ProducerStateEntry(producerId, lastAppendedDataBatches, producerEpoch,
                        coordinatorEpoch, timestamp, currentFirstOffset);
                producerStateEntryList.add(entry);
            }
            return producerStateEntryList;
        } catch (SchemaException e) {
            throw new UnknownServerException("Snapshot failed schema validation: " + e.getMessage());
        }
    }

    private void loadProducerEntry(ProducerStateEntry entry) {
        Long producerId = entry.producerId();
        producers.put(producerId, entry);
        entry.currentTxnFirstOffset().ifPresent(offset -> ongoingTxns.put(offset, new TxnMetadata(producerId, offset)));
    }

    public CompletableFuture<Void> recover(ManagedLedger managedLedger) {
        log.info("Start recover fo topic {}", topicPartition);
        if (state.equals(State.READY)) {
            return CompletableFuture.completedFuture(null);
        }
        if (state.equals(State.RECOVER_ERROR)) {
            return FutureUtil.failedFuture(new Exception("Failed to recover for topic partition " + topicPartition));
        }
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        loadFromSnapshot().thenAccept(ignored -> {
            MessageMetadataUtils.asyncFindPosition(managedLedger, this.lastMapOffset, false).thenAccept(pos -> {
                try {
                    ManagedCursor cursor =
                            managedLedger.newNonDurableCursor(pos, "producer-state-recover");
                    ProducerStateLogRecovery recovery = new ProducerStateLogRecovery(cursor, 100);
                    recovery.recover();
                    state = State.READY;
                    completableFuture.complete(null);
                    log.info("Finish recover fo topic {}", topicPartition);
                } catch (ManagedLedgerException e) {
                    state = State.RECOVER_ERROR;
                    log.error("Failed to open non durable cursor for topic {}.", topicPartition, e);
                    completableFuture.completeExceptionally(e);
                }
            }).exceptionally(findSnapshotPosThrowable -> {
                completableFuture.completeExceptionally(findSnapshotPosThrowable);
                return null;
            });
        }).exceptionally(loadSnapshotThrowable -> {
            completableFuture.completeExceptionally(loadSnapshotThrowable);
            return null;
        });
        return completableFuture;
    }

    /**
     * ProducerStateLogRecovery is used to recover producer state from logs.
     */
    private class ProducerStateLogRecovery {

        private final ManagedCursor cursor;
        private int cacheQueueSize = 100;
        private final List<Entry> readEntryList = new ArrayList<>();
        private int maxErrorCount = 10;
        private int errorCount = 0;
        private boolean readComplete = false;
        private boolean havePendingRead = false;
        private boolean recoverComplete = false;
        private boolean recoverError = false;

        private ProducerStateLogRecovery(ManagedCursor cursor, int cacheQueueSize) {
            this.cursor = cursor;
            this.cacheQueueSize = cacheQueueSize;
        }

        private void fillCacheQueue() {
            havePendingRead = true;
            cursor.asyncReadEntries(cacheQueueSize, new AsyncCallbacks.ReadEntriesCallback() {
                @Override
                public void readEntriesComplete(List<Entry> entries, Object ctx) {
                    havePendingRead = false;
                    if (entries.size() == 0) {
                        log.info("Can't read more entries, finish to recover topic {}.", topicPartition);
                        readComplete = true;
                        return;
                    }
                    readEntryList.addAll(entries);
                }

                @Override
                public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                    havePendingRead = false;
                    if (exception instanceof ManagedLedgerException.NoMoreEntriesToReadException) {
                        log.info("No more entries to read, finish to recover topic {}.", topicPartition);
                        readComplete = true;
                        return;
                    }
                    checkErrorCount(exception);
                }
            }, null, null);
        }

        private void recover() {
            while (!recoverComplete && !recoverError && readEntryList.size() > 0) {
                if (!havePendingRead && !readComplete) {
                    fillCacheQueue();
                }
                if (readEntryList.size() > 0) {
                    List<Entry> entryList = new ArrayList<>(readEntryList);
                    readEntryList.clear();
                    fillCacheQueue();
                    DecodeResult decodeResult = entryFormatter.decode(entryList, RecordBatch.CURRENT_MAGIC_VALUE);
                    Map<Long, ProducerAppendInfo> appendInfoMap = new HashMap<>();
                    List<CompletedTxn> completedTxns = new ArrayList<>();
                    decodeResult.getRecords().batches().forEach(batch -> {
                        Optional<CompletedTxn> completedTxn =
                                updateProducers(batch, appendInfoMap, Optional.empty(), PartitionLog.AppendOrigin.Log);
                        completedTxn.ifPresent(completedTxns::add);
                    });
                    appendInfoMap.values().forEach(ProducerStateManager.this::update);
                    completedTxns.forEach(ProducerStateManager.this::completeTxn);
                    if (readComplete) {
                        recoverComplete = true;
                    }
                } else {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        checkErrorCount(e);
                    }
                }
            }
            log.info("Finish to recover from logs.");
        }

        private void checkErrorCount(Throwable throwable) {
            if (errorCount < maxErrorCount) {
                errorCount++;
                log.error("[{}] Recover error count {}. msg: {}.",
                        topicPartition, errorCount, throwable.getMessage(), throwable);
            } else {
                recoverError = true;
                log.error("[{}] Failed to recover.", topicPartition);
            }
        }

    }

}
