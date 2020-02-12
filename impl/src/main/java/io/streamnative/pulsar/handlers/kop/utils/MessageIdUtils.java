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

import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

/**
 * Utils for Pulsar MessageId.
 */
public class MessageIdUtils {
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
}
