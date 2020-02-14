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

import com.google.common.base.Predicate;
import java.util.Optional;
import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.PositionBound;
import org.apache.bookkeeper.mledger.impl.PositionImpl;

/**
 * Used to find Entry/Offset from ManagedLedger.
 * Copy from org.apache.bookkeeper.mledger.impl.OpFindNewest.
 * only different is here we passed in ManagedLedgerImpl directly.
 */
class OpFindNewestEntry implements ReadEntryCallback {
    private final ManagedLedgerImpl managedLedger;
    private final PositionImpl startPosition;
    private final FindEntryCallback callback;
    private final Predicate<Entry> condition;
    private final Object ctx;

    enum State {
        checkFirst, checkLast, searching
    }

    PositionImpl searchPosition;
    long min;
    long max;
    Position lastMatchedPosition = null;
    State state;

    public OpFindNewestEntry(ManagedLedgerImpl managedLedger, PositionImpl startPosition, Predicate<Entry> condition,
                             long numberOfEntries, FindEntryCallback callback, Object ctx) {
        this.managedLedger = managedLedger;
        this.startPosition = startPosition;
        this.callback = callback;
        this.condition = condition;
        this.ctx = ctx;

        this.min = 0;
        this.max = numberOfEntries;

        this.searchPosition = startPosition;
        this.state = State.checkFirst;
    }

    @Override
    public void readEntryComplete(Entry entry, Object ctx) {
        final Position position = entry.getPosition();
        switch (state) {
            case checkFirst:
                if (!condition.apply(entry)) {
                    callback.findEntryComplete(startPosition, this.ctx);
                    return;
                } else {
                    lastMatchedPosition = position;

                    // check last entry
                    state = State.checkLast;
                    searchPosition = managedLedger.getPositionAfterN(searchPosition, max, PositionBound.startExcluded);
                    find();
                }
                break;
            case checkLast:
                if (condition.apply(entry)) {
                    callback.findEntryComplete(position, this.ctx);
                    return;
                } else {
                    // start binary search
                    state = State.searching;
                    searchPosition = managedLedger.getPositionAfterN(startPosition, mid(), PositionBound.startExcluded);
                    find();
                }
                break;
            case searching:
                if (condition.apply(entry)) {
                    // mid - last
                    lastMatchedPosition = position;
                    min = mid();
                } else {
                    // start - mid
                    max = mid() - 1;
                }

                if (max <= min) {
                    callback.findEntryComplete(lastMatchedPosition, this.ctx);
                    return;
                }
                searchPosition = managedLedger.getPositionAfterN(startPosition, mid(), PositionBound.startExcluded);
                find();
        }
    }

    @Override
    public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
        callback.findEntryFailed(exception, Optional.empty(), this.ctx);
    }

    public void find() {
        if (managedLedger.hasMoreEntries(searchPosition)) {
            managedLedger.asyncReadEntry(searchPosition, this, null);
        } else {
            callback.findEntryComplete(lastMatchedPosition, this.ctx);
        }
    }

    private long mid() {
        return min + Math.max((max - min) / 2, 1);
    }
}
