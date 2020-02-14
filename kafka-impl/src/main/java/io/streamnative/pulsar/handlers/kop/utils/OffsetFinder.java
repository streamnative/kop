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

import com.google.common.base.Predicate;
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
import org.apache.pulsar.client.impl.MessageImpl;

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
                log.debug("[{}] Starting message position find at timestamp {}",  timestamp);
            }

            asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries, entry -> {
                MessageImpl msg = null;
                try {
                    msg = MessageImpl.deserialize(entry.getDataBuffer());
                    return msg.getPublishTime() <= timestamp;
                } catch (Exception e) {
                    log.error("[{}][{}] Error deserializing message for message position find",  e);
                } finally {
                    entry.release();
                    if (msg != null) {
                        msg.recycle();
                    }
                }
                return false;
            }, this, callback);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] Ignore message position find scheduled task, last find is still running");
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
            log.info("[{}][{}] Found position {} closest to provided timestamp {}", position,
                timestamp);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}][{}] No position found closest to provided timestamp {}", timestamp);
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
            log.debug("[{}][{}] message position find operation failed for provided timestamp {}",
                timestamp, exception);
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
            return managedLedger.getNextValidPosition(firstPosition);
        }
    }
}
