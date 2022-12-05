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
import java.util.concurrent.CompletableFuture;

import io.streamnative.pulsar.handlers.kop.KafkaTopicManager;
import io.streamnative.pulsar.handlers.kop.KafkaTopicManagerSharedState;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchResponse;

/**
 * Producer state manager.
 */
@Slf4j
public class ProducerStateManager {

    @Getter
    private final String topicPartition;
    private final Map<Long, ProducerStateEntry> producers = Maps.newConcurrentMap();

    // ongoing transactions sorted by the first offset of the transaction
    private final TreeMap<Long, TxnMetadata> ongoingTxns = Maps.newTreeMap();
    private final List<AbortedTxn> abortedIndexList = new ArrayList<>();

    private final ProducerStateManagerSnapshotBuffer producerStateManagerSnapshotBuffer;

    public ProducerStateManager(String topicPartition, ProducerStateManagerSnapshotBuffer producerStateManagerSnapshotBuffer) {
        this.topicPartition = topicPartition;
        this.producerStateManagerSnapshotBuffer = producerStateManagerSnapshotBuffer;
    }

    public CompletableFuture<Void> recover(PartitionLog partitionLog,
                                           KafkaTopicManagerSharedState sharedState) {
        return producerStateManagerSnapshotBuffer
                .readLatestSnapshot(topicPartition)
                .thenCompose(snapshot -> applySnapshotAndRecover(snapshot, partitionLog, sharedState));
    }

    private CompletableFuture<Void> applySnapshotAndRecover(ProducerStateManagerSnapshot snapshot,
                                                            PartitionLog partitionLog,
                                                            KafkaTopicManagerSharedState sharedState
                                                            ) {
        this.abortedIndexList.clear();
        if (snapshot.getAbortedIndexList() != null) {
            this.abortedIndexList.addAll(snapshot.getAbortedIndexList());
        }
        this.producers.clear();
        if (snapshot.getProducers() != null) {
            this.producers.putAll(snapshot.getProducers());
        }
        this.ongoingTxns.clear();
        if (snapshot.getOngoingTxns() != null) {
            this.ongoingTxns.putAll(snapshot.getOngoingTxns());
        }
        long offSetPosition = snapshot.getOffset();
        // recover from log
        // https://github.com/apache/kafka/blob/5002715485482a8bffd04c05110a29ca98ab097c/core/src/main/scala/kafka/log/Log.scala#L968
        return partitionLog.recoverTxEntries(offSetPosition, sharedState, this);
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
