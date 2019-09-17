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
package org.apache.bookkeeper.mledger.impl;

import lombok.Getter;

/**
 * A wrapper to make ManagedLedgerImpl accessible.
 */
public class ManagedLedgerImplWrapper {
    @Getter
    private final ManagedLedgerImpl managedLedger;

    public ManagedLedgerImplWrapper(ManagedLedgerImpl managedLedger) {
        this.managedLedger = managedLedger;
    }

    public PositionImpl getNextValidPosition(final PositionImpl position) {
        return managedLedger.getNextValidPosition(position);
    }

    // return PositionImpl(firstLedgerId, -1)
    public PositionImpl getFirstPosition() {
        return managedLedger.getFirstPosition();
    }

    // combine getFirstPosition and getNextValidPosition together.
    public PositionImpl getFirstValidPosition() {
        PositionImpl firstPosition = managedLedger.getFirstPosition();
        if (firstPosition == null) {
            return null;
        } else {
            return getNextValidPosition(firstPosition);
        }
    }

    public PositionImpl getPreviousPosition(PositionImpl position) {
        return managedLedger.getPreviousPosition(position);
    }

    public PositionImpl getLastConfirmedEntry() {
        return (PositionImpl) managedLedger.getLastConfirmedEntry();
    }

}
