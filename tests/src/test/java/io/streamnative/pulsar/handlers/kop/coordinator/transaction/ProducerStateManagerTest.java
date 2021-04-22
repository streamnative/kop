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
package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.streamnative.pulsar.handlers.kop.ProducerStateManager;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.SystemTime;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

/**
 * Producer state manager test.
 */
@Slf4j
public class ProducerStateManagerTest {

    private ProducerStateManager stateManager;
    private TopicPartition partition = new TopicPartition("test", 0);
    private Long producerId = 1L;
    private Long maxPidExpirationMs = 10 * 1000L;

    @BeforeMethod
    private void setup() {
        stateManager = new ProducerStateManager(partition.toString(), maxPidExpirationMs.intValue());
    }

    @Test
    public void testBasicIdMapping() {
        short epoch = 0;

        // First entry for id 0 added
        append(stateManager, producerId, epoch, 0, 0L, 0L, false);

        // Second entry for id 0 added
        append(stateManager, producerId, epoch, 1, 0L, 1L, false);

        // Duplicates are checked separately and should result in OutOfOrderSequence if appended
        try {
            append(stateManager, producerId, epoch, 1, 0L, 1L, false);
            fail("Duplicates are checked separately and should result in OutOfOrderSequence if appended.");
        } catch (OutOfOrderSequenceException e) {
            log.info("Expected behavior.");
        }

        // Invalid sequence number (greater than next expected sequence number)
        try {
            append(stateManager, producerId, epoch, 5, 0L, 2L, false);
            fail("Invalid sequence number (greater than next expected sequence number).");
        } catch (OutOfOrderSequenceException e) {
            log.info("Expected behavior.");
        }

        // Change epoch
        append(stateManager, producerId, (short) (epoch + 1), 0, 0L, 3L, false);

        // Incorrect epoch

        try {
            append(stateManager, producerId, epoch, 0, 0L, 4L, false);
            fail("Incorrect epoch.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("which is smaller than the last seen"));
            log.info("Expected behavior.");
        }
    }

    @Test
    public void testAppendTxnMarkerWithNoProducerState() {
        short producerEpoch = 2;
        appendEndTxnMarker(stateManager, producerId, producerEpoch, ControlRecordType.COMMIT, 27L,
                -1, SystemTime.SYSTEM.milliseconds());

        ProducerStateManager.ProducerStateEntry firstEntry = stateManager.lastEntry(producerId).orElseGet(() -> {
            fail("Expected last entry to be defined");
            return null;
        });

        assertEquals(producerEpoch, firstEntry.getProducerEpoch().shortValue());
        assertEquals(producerId, firstEntry.getProducerId());
        assertEquals(RecordBatch.NO_SEQUENCE, firstEntry.lastSeq().intValue());

        // Fencing should continue to work even if the marker is the only thing left
        try {
            append(stateManager, producerId, (short) 0, 0, 0L, 4L, false);
            fail("Fencing should continue to work even if the marker is the only thing left.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("which is smaller than the last seen"));
            log.info("Expected behavior.");
        }

        // If the transaction marker is the only thing left in the log, then an attempt to write using a non-zero
        // sequence number should cause an OutOfOrderSequenceException, so that the producer can reset its state
        try {
            append(stateManager, producerId, producerEpoch, 17, 0L, 4L, false);
            fail("If the transaction marker is the only thing left in the log, then an attempt to write"
                    + "using a non-zero sequence number should cause an OutOfOrderSequenceException, so that the "
                    + "producer can reset its state.");
        } catch (OutOfOrderSequenceException e) {
            log.info("Expected behavior.");
        }

        // The broker should accept the request if the sequence number is reset to 0
        append(stateManager, producerId, producerEpoch, 0, 39L, 4L, false);
        ProducerStateManager.ProducerStateEntry secondEntry = stateManager.lastEntry(producerId).orElseGet(() -> {
            fail("Expected last entry to be defined");
            return null;
        });
        assertEquals(producerEpoch, secondEntry.getProducerEpoch().shortValue());
        assertEquals(producerId, secondEntry.getProducerId());
        assertEquals(0, secondEntry.lastSeq().intValue());
    }

    @Test
    public void testProducerSequenceWrapAround() {
        short epoch = 15;
        Integer sequence = Integer.MAX_VALUE;
        long offset = 735L;
        append(stateManager, producerId, epoch, sequence, offset);

        append(stateManager, producerId, epoch, 0, offset + 500);

        Optional<ProducerStateManager.ProducerStateEntry> maybeLastEntry = stateManager.lastEntry(producerId);
        assertTrue(maybeLastEntry.isPresent());

        ProducerStateManager.ProducerStateEntry lastEntry = maybeLastEntry.get();
        assertEquals(epoch, lastEntry.getProducerEpoch().shortValue());

        assertEquals(Integer.MAX_VALUE, lastEntry.firstSeq().intValue());
        assertEquals(0, lastEntry.lastSeq().intValue());
    }

    @Test
    public void testProducerSequenceWithWrapAroundBatchRecord() {
        short epoch = 15;

        ProducerStateManager.ProducerAppendInfo appendInfo = stateManager.prepareUpdate(
                producerId, ProducerStateManager.AppendOrigin.Coordinator);
        // Sequence number wrap around
        appendInfo.appendDataBatch(epoch, Integer.MAX_VALUE - 10, 9, SystemTime.SYSTEM.milliseconds(),
                2000L, 2020L, false);
        assertEquals(Optional.empty(), stateManager.lastEntry(producerId));
        stateManager.update(appendInfo);
        assertTrue(stateManager.lastEntry(producerId).isPresent());

        ProducerStateManager.ProducerStateEntry lastEntry = stateManager.lastEntry(producerId).get();
        assertEquals(Integer.MAX_VALUE - 10, lastEntry.firstSeq().intValue());
        assertEquals(9, lastEntry.lastSeq().intValue());
        assertEquals(2000L, lastEntry.firstDataOffset().longValue());
        assertEquals(2020L, lastEntry.lastDataOffset().longValue());
    }

    @Test(expectedExceptions = OutOfOrderSequenceException.class)
    public void testProducerSequenceInvalidWrapAround() {
        short epoch = 15;
        Integer sequence = Integer.MAX_VALUE;
        long offset = 735L;
        append(stateManager, producerId, epoch, sequence, offset);
        append(stateManager, producerId, epoch, 1, offset + 500);
    }

    @Test
    public void testNoValidationOnFirstEntryWhenLoadingLog() {
        short epoch = 5;
        Integer sequence = 16;
        long offset = 735L;
        append(stateManager, producerId, epoch, sequence, offset);

        Optional<ProducerStateManager.ProducerStateEntry> maybeLastEntry = stateManager.lastEntry(producerId);
        assertTrue(maybeLastEntry.isPresent());

        ProducerStateManager.ProducerStateEntry lastEntry = maybeLastEntry.get();
        assertEquals(epoch, lastEntry.getProducerEpoch().shortValue());
        assertEquals(sequence, lastEntry.firstSeq());
        assertEquals(sequence, lastEntry.lastSeq());
        assertEquals(offset, lastEntry.lastDataOffset().longValue());
        assertEquals(offset, lastEntry.firstDataOffset().longValue());
    }

    @Test
    public void testControlRecordBumpsProducerEpoch() {
        short producerEpoch = 0;
        append(stateManager, producerId, producerEpoch, 0, 0L);

        short bumpedProducerEpoch = 1;
        appendEndTxnMarker(stateManager, producerId, bumpedProducerEpoch, ControlRecordType.ABORT, 1L,
                -1, SystemTime.SYSTEM.milliseconds());

        Optional<ProducerStateManager.ProducerStateEntry> maybeLastEntry = stateManager.lastEntry(producerId);
        assertTrue(maybeLastEntry.isPresent());

        ProducerStateManager.ProducerStateEntry lastEntry = maybeLastEntry.get();
        assertEquals(bumpedProducerEpoch, lastEntry.getProducerEpoch().shortValue());
        assertEquals(Optional.empty(), lastEntry.getCurrentTxnFirstOffset());
        assertEquals(RecordBatch.NO_SEQUENCE, lastEntry.firstSeq().intValue());
        assertEquals(RecordBatch.NO_SEQUENCE, lastEntry.lastSeq().intValue());

        // should be able to append with the new epoch if we start at sequence 0
        append(stateManager, producerId, bumpedProducerEpoch, 0, 2L);
        assertEquals(0, stateManager.lastEntry(producerId).get().firstSeq().intValue());
    }

    @Test
    public void testTxnFirstOffsetMetadataCached() {
        short producerEpoch = 0;
        long offset = 992342L;
        int seq = 0;
        ProducerStateManager.ProducerAppendInfo producerAppendInfo =
                new ProducerStateManager.ProducerAppendInfo(
                        partition.toString(), producerId, ProducerStateManager.ProducerStateEntry.empty(producerId),
                        ProducerStateManager.AppendOrigin.Client);

        producerAppendInfo.appendDataBatch(producerEpoch, seq, seq, SystemTime.SYSTEM.milliseconds(),
                offset, offset, true);
        stateManager.update(producerAppendInfo);

        assertEquals(Optional.of(offset), stateManager.firstUndecidedOffset());
    }

    @Test
    public void testSkipEmptyTransactions() {
        short producerEpoch = 0;
        short coordinatorEpoch = 27;
        AtomicInteger seq = new AtomicInteger(0);

        // Start one transaction in a separate append
        ProducerStateManager.ProducerAppendInfo firstAppend = stateManager.prepareUpdate(
                producerId, ProducerStateManager.AppendOrigin.Client);
        appendData(16L, 20L, firstAppend, producerEpoch, seq);
        assertEquals(new ProducerStateManager.TxnMetadata(producerId, 16L), firstAppend.startedTransactions().get(0));
        stateManager.update(firstAppend);
        assertEquals(16, stateManager.firstUndecidedOffset().get().longValue());

        // Now do a single append which completes the old transaction, mixes in
        // some empty transactions, one non-empty complete transaction, and one
        // incomplete transaction
        ProducerStateManager.ProducerAppendInfo secondAppend = stateManager.prepareUpdate(
                producerId, ProducerStateManager.AppendOrigin.Client);
        Optional<ProducerStateManager.CompletedTxn> firstCompletedTxn =
                appendEndTxn(ControlRecordType.COMMIT, 21L, secondAppend, coordinatorEpoch, producerEpoch);
        assertEquals(
                Optional.of(new ProducerStateManager.CompletedTxn(producerId, 16L, 21L, false)),
                firstCompletedTxn);
        assertEquals(Optional.empty(),
                appendEndTxn(ControlRecordType.COMMIT, 22L, secondAppend, coordinatorEpoch, producerEpoch));
        assertEquals(Optional.empty(),
                appendEndTxn(ControlRecordType.ABORT, 23L, secondAppend, coordinatorEpoch, producerEpoch));
        appendData(24L, 27L, secondAppend, producerEpoch, seq);
        Optional<ProducerStateManager.CompletedTxn> secondCompletedTxn = appendEndTxn(
                ControlRecordType.ABORT, 28L, secondAppend, coordinatorEpoch, producerEpoch);
        assertTrue(secondCompletedTxn.isPresent());
        assertEquals(Optional.empty(),
                appendEndTxn(ControlRecordType.ABORT, 29L, secondAppend, coordinatorEpoch, producerEpoch));
        appendData(30L, 31L, secondAppend, producerEpoch, seq);

        assertEquals(2, secondAppend.startedTransactions().size());
        assertEquals(new ProducerStateManager.TxnMetadata(producerId, 24L),
                secondAppend.startedTransactions().get(0));
        assertEquals(new ProducerStateManager.TxnMetadata(producerId, 30L),
                secondAppend.startedTransactions().get(secondAppend.startedTransactions().size() - 1));
        stateManager.update(secondAppend);
        stateManager.completeTxn(firstCompletedTxn.get());
        stateManager.completeTxn(secondCompletedTxn.get());
        assertEquals(30L, stateManager.firstUndecidedOffset().get().longValue());
    }

    private Optional<ProducerStateManager.CompletedTxn> appendEndTxn(
            ControlRecordType recordType, Long offset, ProducerStateManager.ProducerAppendInfo appendInfo,
            short coordinatorEpoch, short producerEpoch) {
        return appendInfo.appendEndTxnMarker(new EndTransactionMarker(recordType, coordinatorEpoch),
                producerEpoch, offset, SystemTime.SYSTEM.milliseconds());
    }

    public void appendData(Long startOffset, Long endOffset,
                           ProducerStateManager.ProducerAppendInfo appendInfo,
                           short producerEpoch, AtomicInteger seq) {
        int count = (int) (endOffset - startOffset);
        appendInfo.appendDataBatch(producerEpoch, seq.get(), seq.addAndGet(count), SystemTime.SYSTEM.milliseconds(),
                startOffset, endOffset, true);
        seq.incrementAndGet();
    }

    @Test
    public void testLastStableOffsetCompletedTxn() {
        short producerEpoch = 0;

        long producerId1 = producerId;
        long startOffset1 = 992342L;
        beginTxn(producerId1, producerEpoch, startOffset1);

        long producerId2 = producerId + 1;
        long startOffset2 = startOffset1 + 25;
        beginTxn(producerId2, producerEpoch, startOffset2);

        long producerId3 = producerId + 2;
        long startOffset3 = startOffset1 + 57;
        beginTxn(producerId3, producerEpoch, startOffset3);

        long lastOffset1 = startOffset3 + 15;
        ProducerStateManager.CompletedTxn completedTxn1 =
                new ProducerStateManager.CompletedTxn(producerId1, startOffset1, lastOffset1, false);
        assertEquals(startOffset2, stateManager.lastStableOffset(completedTxn1));
        stateManager.completeTxn(completedTxn1);
        assertEquals(startOffset2, stateManager.firstUndecidedOffset().get().longValue());

        long lastOffset3 = lastOffset1 + 20;
        ProducerStateManager.CompletedTxn completedTxn3 =
                new ProducerStateManager.CompletedTxn(producerId3, startOffset3, lastOffset3, false);
        assertEquals(startOffset2, stateManager.lastStableOffset(completedTxn3));
        stateManager.completeTxn(completedTxn3);
        assertEquals(startOffset2, stateManager.firstUndecidedOffset().get().longValue());

        long lastOffset2 = lastOffset3 + 78;
        ProducerStateManager.CompletedTxn completedTxn2 =
                new ProducerStateManager.CompletedTxn(producerId2, startOffset2, lastOffset2, false);
        assertEquals(lastOffset2 + 1, stateManager.lastStableOffset(completedTxn2));
        stateManager.completeTxn(completedTxn2);
        assertEquals(Optional.empty(), stateManager.firstUndecidedOffset());
    }

    private void beginTxn(Long producerId, short producerEpoch, Long startOffset) {
        ProducerStateManager.ProducerAppendInfo producerAppendInfo = new ProducerStateManager.ProducerAppendInfo(
                partition.toString(),
                producerId,
                ProducerStateManager.ProducerStateEntry.empty(producerId),
                ProducerStateManager.AppendOrigin.Client
        );
        producerAppendInfo.appendDataBatch(producerEpoch, 0, 0, SystemTime.SYSTEM.milliseconds(),
                startOffset, startOffset, true);
        stateManager.update(producerAppendInfo);
    }

    @Test
    public void testPrepareUpdateDoesNotMutate() {
        short producerEpoch = 0;

        ProducerStateManager.ProducerAppendInfo appendInfo = stateManager.prepareUpdate(
                producerId, ProducerStateManager.AppendOrigin.Client);
        appendInfo.appendDataBatch(producerEpoch, 0, 5, SystemTime.SYSTEM.milliseconds(),
                15L, 20L, false);
        assertEquals(Optional.empty(), stateManager.lastEntry(producerId));
        stateManager.update(appendInfo);
        assertTrue(stateManager.lastEntry(producerId).isPresent());

        ProducerStateManager.ProducerAppendInfo nextAppendInfo = stateManager.prepareUpdate(
                producerId, ProducerStateManager.AppendOrigin.Client);
        nextAppendInfo.appendDataBatch(producerEpoch, 6, 10, SystemTime.SYSTEM.milliseconds(),
                26L, 30L, false);
        assertTrue(stateManager.lastEntry(producerId).isPresent());

        ProducerStateManager.ProducerStateEntry lastEntry = stateManager.lastEntry(producerId).get();
        assertEquals(0, lastEntry.firstSeq().intValue());
        assertEquals(5, lastEntry.lastSeq().intValue());
        assertEquals(20L, lastEntry.lastDataOffset().longValue());

        stateManager.update(nextAppendInfo);
        lastEntry = stateManager.lastEntry(producerId).get();
        assertEquals(0, lastEntry.firstSeq().intValue());
        assertEquals(10, lastEntry.lastSeq().intValue());
        assertEquals(30L, lastEntry.lastDataOffset().longValue());
    }

    @Test
    public void updateProducerTransactionState() {
        short producerEpoch = 0;
        short coordinatorEpoch = 15;
        long offset = 9L;
        append(stateManager, producerId, producerEpoch, 0, offset);

        ProducerStateManager.ProducerAppendInfo appendInfo =
                stateManager.prepareUpdate(producerId, ProducerStateManager.AppendOrigin.Client);
        appendInfo.appendDataBatch(producerEpoch, 1, 5, SystemTime.SYSTEM.milliseconds(),
                16L, 20L, true);
        ProducerStateManager.ProducerStateEntry lastEntry = appendInfo.toEntry();
        assertEquals(producerEpoch, lastEntry.getProducerEpoch().shortValue());
        assertEquals(1, lastEntry.firstSeq().intValue());
        assertEquals(5, lastEntry.lastSeq().intValue());
        assertEquals(16L, lastEntry.firstDataOffset().longValue());
        assertEquals(20L, lastEntry.lastDataOffset().longValue());
        assertEquals(Optional.of(16L), lastEntry.getCurrentTxnFirstOffset());
        assertEquals(
                Lists.newArrayList(new ProducerStateManager.TxnMetadata(producerId, 16L)),
                appendInfo.startedTransactions());

        appendInfo.appendDataBatch(producerEpoch, 6, 10, SystemTime.SYSTEM.milliseconds(),
                26L, 30L, true);
        lastEntry = appendInfo.toEntry();
        assertEquals(producerEpoch, lastEntry.getProducerEpoch().shortValue());
        assertEquals(1, lastEntry.firstSeq().intValue());
        assertEquals(10, lastEntry.lastSeq().intValue());
        assertEquals(16L, lastEntry.firstDataOffset().longValue());
        assertEquals(30L, lastEntry.lastDataOffset().longValue());
        assertEquals(Optional.of(16L), lastEntry.getCurrentTxnFirstOffset());
        assertEquals(
                Lists.newArrayList(new ProducerStateManager.TxnMetadata(producerId, 16L)),
                appendInfo.startedTransactions());

        EndTransactionMarker endTxnMarker = new EndTransactionMarker(ControlRecordType.COMMIT, coordinatorEpoch);
        Optional<ProducerStateManager.CompletedTxn> completedTxnOpt =
                appendInfo.appendEndTxnMarker(endTxnMarker, producerEpoch, 40L, SystemTime.SYSTEM.milliseconds());
        assertTrue(completedTxnOpt.isPresent());

        ProducerStateManager.CompletedTxn completedTxn = completedTxnOpt.get();
        assertEquals(producerId, completedTxn.getProducerId());
        assertEquals(16L, completedTxn.getFirstOffset().longValue());
        assertEquals(40L, completedTxn.getLastOffset().longValue());
        assertFalse(completedTxn.getIsAborted());

        lastEntry = appendInfo.toEntry();
        assertEquals(producerEpoch, lastEntry.getProducerEpoch().shortValue());
        // verify that appending the transaction marker doesn't affect the metadata of the cached record batches.
        assertEquals(1, lastEntry.firstSeq().intValue());
        assertEquals(10, lastEntry.lastSeq().intValue());
        assertEquals(16L, lastEntry.firstDataOffset().longValue());
        assertEquals(30L, lastEntry.lastDataOffset().longValue());
        assertEquals(Optional.empty(), lastEntry.getCurrentTxnFirstOffset());
        assertEquals(
                Lists.newArrayList(new ProducerStateManager.TxnMetadata(producerId, 16L)),
                appendInfo.startedTransactions());
    }

    @Test
    public void testOutOfSequenceAfterControlRecordEpochBump() {
        short epoch = 0;
        append(stateManager, producerId, epoch, 0, 0L, SystemTime.SYSTEM.milliseconds(), true);
        append(stateManager, producerId, epoch, 1, 1L, SystemTime.SYSTEM.milliseconds(), true);

        short bumpedEpoch = 1;
        appendEndTxnMarker(stateManager, producerId, bumpedEpoch, ControlRecordType.ABORT, 1L,
                -1, SystemTime.SYSTEM.milliseconds());

        // next append is invalid since we expect the sequence to be reset
        try {
            append(stateManager, producerId, bumpedEpoch, 2, 2L, SystemTime.SYSTEM.milliseconds(), true);
            fail("Next append is invalid since we expect the sequence to be reset.");
        } catch (OutOfOrderSequenceException e) {
            log.info("Expected behavior.");
        }

        try {
            append(stateManager, producerId, (short) (bumpedEpoch + 1), 2, 2L,
                    SystemTime.SYSTEM.milliseconds(), true);
            fail("[2] Next append is invalid since we expect the sequence to be reset.");
        } catch (OutOfOrderSequenceException e) {
            log.info("Expected behavior.");
        }

        // Append with the bumped epoch should be fine if starting from sequence 0
        append(stateManager, producerId, bumpedEpoch, 0, 0L, SystemTime.SYSTEM.milliseconds(), true);
        assertEquals(bumpedEpoch, stateManager.lastEntry(producerId).get().getProducerEpoch().shortValue());
        assertEquals(0, stateManager.lastEntry(producerId).get().lastSeq().shortValue());
    }

    @Test(expectedExceptions = InvalidTxnStateException.class)
    public void testNonTransactionalAppendWithOngoingTransaction() {
        short epoch = 0;
        append(stateManager, producerId, epoch, 0, 0L, SystemTime.SYSTEM.milliseconds(), true);
        append(stateManager, producerId, epoch, 1, 1L, SystemTime.SYSTEM.milliseconds(), false);
    }

    @Test
    public void testProducerStateAfterFencingAbortMarker() {
        short epoch = 0;
        append(stateManager, producerId, epoch, 0, 0L, SystemTime.SYSTEM.milliseconds(), true);
        appendEndTxnMarker(stateManager, producerId, (short) (epoch + 1), ControlRecordType.ABORT,
                1L, -1, SystemTime.SYSTEM.milliseconds());

        ProducerStateManager.ProducerStateEntry lastEntry = stateManager.lastEntry(producerId).get();
        assertEquals(Optional.empty(), lastEntry.getCurrentTxnFirstOffset());
        assertEquals(-1, lastEntry.lastDataOffset().longValue());
        assertEquals(-1, lastEntry.firstDataOffset().longValue());

        // The producer should not be expired because we want to preserve fencing epochs
        stateManager.removeExpiredProducers(SystemTime.SYSTEM.milliseconds());
        assertTrue(stateManager.lastEntry(producerId).isPresent());
    }

    @Test
    public void testPidExpirationTimeout() throws InterruptedException {
        short epoch = 5;
        int sequence = 37;
        append(stateManager, producerId, epoch, sequence, 1L);
        Thread.sleep(maxPidExpirationMs + 1);
        stateManager.removeExpiredProducers(SystemTime.SYSTEM.milliseconds());
        append(stateManager, producerId, epoch, sequence + 1, 2L);
        assertEquals(1, stateManager.activeProducers().size());
        assertEquals(sequence + 1, stateManager.activeProducers().get(producerId).lastSeq().shortValue());
        assertEquals(3L, stateManager.mapEndOffset().longValue());
    }

    @Test
    public void testProducersWithOngoingTransactionsDontExpire() throws InterruptedException {
        short epoch = 5;
        int sequence = 0;

        append(stateManager, producerId, epoch, sequence, 99L, SystemTime.SYSTEM.milliseconds(), true);
        assertEquals(Optional.of(99L), stateManager.firstUndecidedOffset());

        Thread.sleep(maxPidExpirationMs + 1);
        stateManager.removeExpiredProducers(SystemTime.SYSTEM.milliseconds());

        assertTrue(stateManager.lastEntry(producerId).isPresent());
        assertEquals(Optional.of(99L), stateManager.firstUndecidedOffset());

        stateManager.removeExpiredProducers(SystemTime.SYSTEM.milliseconds());
        assertTrue(stateManager.lastEntry(producerId).isPresent());
    }

    @Test
    public void testSequenceNotValidatedForGroupMetadataTopic() {
        TopicPartition partition = new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0);
        ProducerStateManager stateManager = new ProducerStateManager(partition.topic(), maxPidExpirationMs.intValue());

        short epoch = 0;
        append(stateManager, producerId, epoch, RecordBatch.NO_SEQUENCE, 99L, SystemTime.SYSTEM.milliseconds(),
                true, ProducerStateManager.AppendOrigin.Coordinator);
        append(stateManager, producerId, epoch, RecordBatch.NO_SEQUENCE, 100L, SystemTime.SYSTEM.milliseconds(),
                true, ProducerStateManager.AppendOrigin.Coordinator);
    }

    @Test
    public void testOldEpochForControlRecord() {
        short epoch = 5;
        int sequence = 0;

        assertEquals(Optional.empty(), stateManager.firstUndecidedOffset());

        append(stateManager, producerId, epoch, sequence, 99L, SystemTime.SYSTEM.milliseconds(), true);
        try {
            appendEndTxnMarker(stateManager, producerId, (short) 3, ControlRecordType.COMMIT, 100L,
                    -1, SystemTime.SYSTEM.milliseconds());
            fail("The control record with old epoch.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("which is smaller than the last seen"));
            log.info("Expected behavior.");
        }
    }

    @Test
    public void testAppendEmptyControlBatch() {
        long producerId = 23423L;
        long baseOffset = 15;

        RecordBatch batch = Mockito.mock(RecordBatch.class);
        Mockito.when(batch.isControlBatch()).thenReturn(true);
        Mockito.when(batch.iterator()).thenReturn(Collections.emptyIterator());

        // Appending the empty control batch should not throw and a new transaction shouldn't be started
        append(stateManager, producerId, baseOffset, batch, ProducerStateManager.AppendOrigin.Client);
        assertEquals(Optional.empty(), stateManager.lastEntry(producerId).get().getCurrentTxnFirstOffset());
    }

    private Optional<ProducerStateManager.CompletedTxn> appendEndTxnMarker(ProducerStateManager mapping,
                                                                           Long producerId,
                                                                           Short producerEpoch,
                                                                           ControlRecordType controlType,
                                                                           Long offset,
                                                                           Integer coordinatorEpoch,
                                                                           Long timestamp) {
        ProducerStateManager.ProducerAppendInfo producerAppendInfo = stateManager.prepareUpdate(
                producerId, ProducerStateManager.AppendOrigin.Coordinator);
        EndTransactionMarker endTxnMarker = new EndTransactionMarker(controlType, coordinatorEpoch);
        Optional<ProducerStateManager.CompletedTxn> completedTxnOpt =
                producerAppendInfo.appendEndTxnMarker(endTxnMarker, producerEpoch, offset, timestamp);
        mapping.update(producerAppendInfo);
        completedTxnOpt.ifPresent(mapping::completeTxn);
        mapping.updateMapEndOffset(offset + 1);
        return completedTxnOpt;
    }

    private void append(ProducerStateManager stateManager,
                        Long producerId,
                        Short producerEpoch,
                        Integer seq,
                        Long offset) {
        append(stateManager, producerId, producerEpoch, seq, offset, SystemTime.SYSTEM.milliseconds(),
                false, ProducerStateManager.AppendOrigin.Client);
    }

    private void append(ProducerStateManager stateManager,
                        Long producerId,
                        Short producerEpoch,
                        Integer seq,
                        Long offset,
                        Long timestamp,
                        Boolean isTransactional) {
        append(stateManager, producerId, producerEpoch, seq, offset, timestamp, isTransactional,
                ProducerStateManager.AppendOrigin.Client);
    }

    private void append(ProducerStateManager stateManager,
                        Long producerId,
                        Short producerEpoch,
                        Integer seq,
                        Long offset,
                        Long timestamp,
                        Boolean isTransactional,
                        ProducerStateManager.AppendOrigin origin) {
        ProducerStateManager.ProducerAppendInfo producerAppendInfo = stateManager.prepareUpdate(producerId, origin);
        producerAppendInfo.appendDataBatch(producerEpoch, seq, seq, timestamp, -1L, -1L, isTransactional);
        producerAppendInfo.resetOffset(offset, isTransactional);
        stateManager.update(producerAppendInfo);
        stateManager.updateMapEndOffset(offset + 1);
    }

    private void append(ProducerStateManager stateManager,
                        Long producerId,
                        Long offset,
                        RecordBatch batch,
                        ProducerStateManager.AppendOrigin origin) {
        ProducerStateManager.ProducerAppendInfo producerAppendInfo = stateManager.prepareUpdate(producerId, origin);
        producerAppendInfo.append(batch, Optional.empty());
        stateManager.update(producerAppendInfo);
        stateManager.updateMapEndOffset(offset + 1);
    }

}
