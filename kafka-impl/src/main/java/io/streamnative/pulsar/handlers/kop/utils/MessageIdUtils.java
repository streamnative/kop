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
package io.streamnative.pulsar.handlers.kop.utils;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.intercept.ManagedLedgerInterceptorImpl;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utils for Pulsar MessageId.
 */
public class MessageIdUtils {
    private static final Logger log = LoggerFactory.getLogger(MessageIdUtils.class);

    // use 28 bits for ledgerId,
    // 32 bits for entryId,
    // 12 bits for batchIndex.
    public static final int LEDGER_BITS = 20;
    public static final int ENTRY_BITS = 32;
    public static final int BATCH_BITS = 12;

    public static final long getOffset(long ledgerId, long entryId) {
        // Combine ledger id and entry id to form offset
        checkArgument(ledgerId >= 0, "Expected ledgerId >= 0, but get " + ledgerId);
        checkArgument(entryId >= 0, "Expected entryId >= 0, but get " + entryId);

        long offset = (ledgerId << (ENTRY_BITS + BATCH_BITS) | (entryId << BATCH_BITS));
        return offset;
    }

    public static final long getOffset(long ledgerId, long entryId, int batchIndex) {
        checkArgument(ledgerId >= 0, "Expected ledgerId >= 0, but get " + ledgerId);
        checkArgument(entryId >= 0, "Expected entryId >= 0, but get " + entryId);
        checkArgument(batchIndex >= 0, "Expected batchIndex >= 0, but get " + batchIndex);
        checkArgument(batchIndex < (1 << BATCH_BITS),
            "Expected batchIndex only take " + BATCH_BITS + " bits, but it is " + batchIndex);

        long offset = (ledgerId << (ENTRY_BITS + BATCH_BITS) | (entryId << BATCH_BITS)) + batchIndex;
        return offset;
    }

    public static final MessageId getMessageId(long offset) {
        // De-multiplex ledgerId and entryId from offset
        checkArgument(offset > 0, "Expected Offset > 0, but get " + offset);

        long ledgerId = offset >>> (ENTRY_BITS + BATCH_BITS);
        long entryId = (offset & 0x0F_FF_FF_FF_FF_FFL) >>> BATCH_BITS;

        return new MessageIdImpl(ledgerId, entryId, -1);
    }

    public static final PositionImpl getPosition(long offset) {
        // De-multiplex ledgerId and entryId from offset
        checkArgument(offset >= 0, "Expected Offset >= 0, but get " + offset);

        long ledgerId = offset >>> (ENTRY_BITS + BATCH_BITS);
        long entryId = (offset & 0x0F_FF_FF_FF_FF_FFL) >>> BATCH_BITS;

        return new PositionImpl(ledgerId, entryId);
    }

    // get the batchIndex contained in offset.
    public static final int getBatchIndex(long offset) {
        checkArgument(offset >= 0, "Expected Offset >= 0, but get " + offset);

        return (int) (offset & 0x0F_FF);
    }

    // get next offset that after batch Index.
    // In TopicConsumerManager, next read offset is updated after each entry reads,
    // if it read a batched message previously, the next offset waiting read is next entry.
    public static final long offsetAfterBatchIndex(long offset) {
        // De-multiplex ledgerId and entryId from offset
        checkArgument(offset >= 0, "Expected Offset >= 0, but get " + offset);

        int batchIndex = getBatchIndex(offset);
        // this is a for
        if (batchIndex != 0) {
            return (offset - batchIndex) + (1 << BATCH_BITS);
        }
        return offset;
    }

    public static long getCurrentOffset(ManagedLedger managedLedger) {
        return ((ManagedLedgerInterceptorImpl) managedLedger.getManagedLedgerInterceptor()).getIndex();
    }

    public static CompletableFuture<Long> getOffsetOfPosition(ManagedLedgerImpl managedLedger, PositionImpl position) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        managedLedger.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }

            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                try {
                   future.complete(peekBaseOffsetFromEntry(entry));
                } catch (Exception exception) {
                    future.completeExceptionally(exception);
                } finally {
                    if (entry != null) {
                        entry.release();
                    }
                }
            }
        }, null);
        return future;
    }

    public static PositionImpl getPositionForOffset(ManagedLedger managedLedger, Long offset) {
        try {
            return (PositionImpl) managedLedger.asyncFindPosition(new OffsetSearchPredicate(offset)).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("[{}] Failed to find position for offset {}", managedLedger.getName(), offset);
            throw new RuntimeException(managedLedger.getName() + " failed to find position for offset " + offset);
        }
    }

    public static long peekOffsetFromEntry(Entry entry) {
        return Commands.peekBrokerEntryMetadataIfExist(entry.getDataBuffer()).getIndex();
    }

    public static long peekBaseOffsetFromEntry(Entry entry) {

        return peekOffsetFromEntry(entry)
                - Commands.peekMessageMetadata(entry.getDataBuffer(), null, 0)
                    .getNumMessagesInBatch() + 1;
    }

    public static long getMockOffset(long ledgerId, long entryId) {
        return ledgerId + entryId;
    }
}
