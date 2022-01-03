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

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

/**
 * This class is used to validate the records appended by a given producer before they are written to the log.
 * It is initialized with the producer's state after the last successful append, and transitively validates the
 * sequence numbers and epochs of each new record. Additionally, this class accumulates transaction metadata
 * as the incoming records are validated.
 */
@Slf4j
@Data
@Accessors(fluent = true)
public class ProducerAppendInfo {

    private final String topicPartition;

    // The id of the producer appending to the log
    private final Long producerId;

    // The current entry associated with the producer id which contains metadata for a fixed number of
    // the most recent appends made by the producer. Validation of the first incoming append will
    // be made against the latest append in the current entry. New appends will replace older appends
    // in the current entry so that the space overhead is constant.
    private final ProducerStateEntry currentEntry;

    // Indicates the origin of to append which implies the extent of validation.
    // For example, offset commits, which originate from the group coordinator,
    // do not have sequence numbers and therefore only producer epoch validation is done.
    // Appends which come through replication are not validated (we assume the validation has already been done)
    // and appends from clients require full validation.
    private final PartitionLog.AppendOrigin origin;
    private final List<TxnMetadata> transactions = Lists.newArrayList();
    private ProducerStateEntry updatedEntry;

    public ProducerAppendInfo(String topicPartition,
                              Long producerId,
                              ProducerStateEntry currentEntry,
                              PartitionLog.AppendOrigin origin) {
        this.topicPartition = topicPartition;
        this.producerId = producerId;
        this.currentEntry = currentEntry;
        this.origin = origin;

        initUpdatedEntry();
    }

    private void checkProducerEpoch(Short producerEpoch) {
        if (producerEpoch < updatedEntry.producerEpoch()) {
            String message = String.format("Producer's epoch in %s is %s, which is smaller than the last seen "
                    + "epoch %s", topicPartition, producerEpoch, currentEntry.producerEpoch());
            throw new IllegalArgumentException(message);
        }
    }

    public Optional<CompletedTxn> append(RecordBatch batch, Optional<Long> firstOffset) {
        if (log.isDebugEnabled()) {
            log.debug("Append batch: pid: {} firstOffset {} baseSequence: {} lastSequence: {} "
                            + "baseOffset: {}  lastOffset: {} ",
                    batch.producerId(), firstOffset, batch.baseSequence(),
                    batch.lastSequence(), batch.baseOffset(), batch.lastOffset());
        }
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
            updateCurrentTxnFirstOffset(batch.isTransactional(), firstOffset.orElse(batch.baseOffset()));
            return Optional.empty();
        }
    }

    /**
     * When message publish success, we should save the current txn first offset.
     *
     * @param isTransactional is transaction or not
     * @param firstOffset transaction first offset
     */
    public void updateCurrentTxnFirstOffset(Boolean isTransactional, long firstOffset) {
        if (updatedEntry.currentTxnFirstOffset().isPresent()) {
            if (!isTransactional) {
                // Received a non-transactional message while a transaction is active
                String msg = String.format("Expected transactional write from producer %s at offset %s in "
                        + "partition %s", producerId, firstOffset, topicPartition);
                throw new InvalidTxnStateException(msg);
            }
        } else {
            if (isTransactional) {
                updatedEntry.currentTxnFirstOffset(Optional.of(firstOffset));
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
        Optional<CompletedTxn> completedTxn =
                updatedEntry.currentTxnFirstOffset().map(firstOffset ->
                        new CompletedTxn(producerId, firstOffset, offset,
                        endTxnMarker.controlType() == ControlRecordType.ABORT));
        updatedEntry.maybeUpdateProducerEpoch(producerEpoch);
        updatedEntry.currentTxnFirstOffset(Optional.empty());
        updatedEntry.lastTimestamp(timestamp);
        return completedTxn;
    }

    public ProducerStateEntry toEntry() {
        return updatedEntry;
    }

    public List<TxnMetadata> startedTransactions() {
        return transactions;
    }

    private void initUpdatedEntry() {
        updatedEntry = ProducerStateEntry.empty(producerId);
        updatedEntry.producerEpoch(currentEntry.producerEpoch());
        updatedEntry.coordinatorEpoch(currentEntry.coordinatorEpoch());
        updatedEntry.lastTimestamp(currentEntry.lastTimestamp());
        updatedEntry.currentTxnFirstOffset(currentEntry.currentTxnFirstOffset());
    }
}
