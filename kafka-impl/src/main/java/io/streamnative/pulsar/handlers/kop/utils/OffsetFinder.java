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
import static com.google.common.base.Preconditions.checkState;
import static org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;

import com.google.common.base.Predicate;
import io.streamnative.pulsar.handlers.kop.exceptions.MetadataCorruptedException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.FindPositionConstraint;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;

/**
 * given a timestamp find the first message (position) (published) at or before the timestamp.
 * Most of the code is similar to Pulsar class `PersistentMessageFinder`.
 */
@Slf4j
public class OffsetFinder implements AsyncCallbacks.FindEntryCallback {
    private final ManagedLedgerImpl managedLedger;
    private long timestamp = 0;

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    @SuppressWarnings("unused")
    private volatile int messageFindInProgress = FALSE;
    private static final AtomicIntegerFieldUpdater<OffsetFinder> messageFindInProgressUpdater =
        AtomicIntegerFieldUpdater.newUpdater(OffsetFinder.class, "messageFindInProgress");

    public OffsetFinder(ManagedLedgerImpl managedLedger) {
        this.managedLedger = managedLedger;
    }

    public void findMessages(final long timestamp, AsyncCallbacks.FindEntryCallback callback) {
        this.timestamp = timestamp;
        if (messageFindInProgressUpdater.compareAndSet(this, FALSE, TRUE)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Starting message position find at timestamp {}", managedLedger.getName(), timestamp);
            }

            asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries, entry -> {
                if (entry == null) {
                    return false;
                }
                try {
                    return MessageMetadataUtils.getPublishTime(entry.getDataBuffer()) <= timestamp;
                } catch (MetadataCorruptedException e) {
                    log.error("[{}] Error deserialize message for message position find", managedLedger.getName(), e);
                } finally {
                    entry.release();
                }
                return false;
            }, this, callback);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ignore message position find scheduled task, last find is still running",
                        managedLedger.getName());
            }
            callback.findEntryFailed(
                new ManagedLedgerException.ConcurrentFindCursorPositionException("last find is still running"),
                Optional.empty(),
                null);
        }
    }

    @Override
    public void findEntryComplete(Position position, Object ctx) {
        checkArgument(ctx instanceof AsyncCallbacks.FindEntryCallback);
        AsyncCallbacks.FindEntryCallback callback = (AsyncCallbacks.FindEntryCallback) ctx;
        if (position != null) {
            log.info("[{}] Found position {} closest to provided timestamp {}",
                    managedLedger.getName(), position, timestamp);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No position found closest to provided timestamp {}",
                        managedLedger.getName(), timestamp);
            }
        }
        messageFindInProgress = FALSE;
        callback.findEntryComplete(position, null);
    }

    @Override
    public void findEntryFailed(ManagedLedgerException exception, Optional<Position> position, Object ctx) {
        checkArgument(ctx instanceof AsyncCallbacks.FindEntryCallback);
        AsyncCallbacks.FindEntryCallback callback = (AsyncCallbacks.FindEntryCallback) ctx;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Message position find operation failed for provided timestamp {}",
                    managedLedger.getName(), timestamp, exception);
        }
        messageFindInProgress = FALSE;
        callback.findEntryFailed(exception, position, null);
    }

    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                        FindEntryCallback callback, Object ctx) {
        checkState(constraint == FindPositionConstraint.SearchAllAvailableEntries);

        // return PositionImpl(firstLedgerId, -1)
        PositionImpl startPosition = managedLedger.getFirstPosition();
        long max = managedLedger.getNumberOfEntries() - 1;

        if (startPosition == null) {
            callback.findEntryFailed(new ManagedLedgerException("Couldn't find start position"), Optional.empty(), ctx);
            return;
        } else {
            startPosition = managedLedger.getNextValidPosition(startPosition);
        }

        OpFindNewestEntry op = new OpFindNewestEntry(managedLedger, startPosition, condition, max, callback, ctx);
        op.find();
    }

    public static PositionImpl getFirstValidPosition(ManagedLedgerImpl managedLedger) {
        PositionImpl firstPosition = managedLedger.getFirstPosition();
        if (firstPosition == null) {
            return null;
        } else {
            final PositionImpl validPosition = managedLedger.getNextValidPosition(firstPosition);
            final NavigableMap<Long, LedgerInfo> ledgers = managedLedger.getLedgersInfo();
            if (!ledgers.containsKey(validPosition.getLedgerId())) {
                // It's a rare case if getNextValidPosition() returns a position that doesn't belong to the ledgers map
                // while the ledgers map contains a non-empty ledger. In this case, return the first position.
                final Map.Entry<Long, LedgerInfo> entry = ledgers.firstEntry();
                if (entry != null && entry.getValue().hasEntries() && entry.getValue().getEntries() > 0) {
                    log.warn("ManagedLedger {} is not empty and doesn't contain {}, return the first position {}:0",
                            managedLedger.getName(), validPosition, entry.getKey());
                    return PositionImpl.get(entry.getKey(), 0);
                }
            }
            return validPosition;
        }
    }
}
