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

/**
 * Transaction state.
 */
public enum TransactionState {

    /**
     * Transaction has not existed yet.
     * transition: received AddPartitionsToTxnRequest => Ongoing
     *             received AddOffsetsToTxnRequest => Ongoing
     */
    EMPTY((byte) 0),

    /**
     * Transaction has started and ongoing.
     * transition: received EndTxnRequest with commit => PrepareCommit
     *             received EndTxnRequest with abort => PrepareAbort
     *             received AddPartitionsToTxnRequest => Ongoing
     *             received AddOffsetsToTxnRequest => Ongoing
     */
    ONGOING((byte) 1),

    /**
     * Group is preparing to commit.
     * transition: received acks from all partitions => CompleteCommit
     */
    PREPARE_COMMIT((byte) 2),

    /**
     * Group is preparing to abort.
     * transition: received acks from all partitions => CompleteAbort
     */
    PREPARE_ABORT((byte) 3),

    /**
     * Group has completed commit.
     * Will soon be removed from the ongoing transaction cache
     */
    COMPLETE_COMMIT((byte) 4),

    /**
     * Group has completed abort.
     * Will soon be removed from the ongoing transaction cache
     */
    COMPLETE_ABORT((byte) 5),

    /**
     * TransactionalId has expired and is about to be removed from the transaction cache.
     */
    DEAD((byte) 6),

    /**
     * We are in the middle of bumping the epoch and fencing out older producers.
     */
    PREPARE_EPOCH_FENCE((byte) 7);

    private final byte value;

    TransactionState(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static TransactionState byteToState(byte value) {
        switch (value) {
            case 0:
                return EMPTY;
            case 1:
                return ONGOING;
            case 2:
                return PREPARE_COMMIT;
            case 3:
                return PREPARE_ABORT;
            case 4:
                return COMPLETE_COMMIT;
            case 5:
                return COMPLETE_ABORT;
            case 6:
                return DEAD;
            case 7:
                return PREPARE_EPOCH_FENCE;
            default:
                throw new IllegalStateException(
                        "Unknown transaction state byte " + value + " from the transaction status message");
        }
    }

    public boolean isExpirationAllowed() {
        switch (this) {
            case EMPTY:
            case COMPLETE_COMMIT:
            case COMPLETE_ABORT:
                return true;
            case DEAD:
            case ONGOING:
            case PREPARE_ABORT:
            case PREPARE_COMMIT:
            case PREPARE_EPOCH_FENCE:
            default:
                return false;
        }
    }

    public org.apache.kafka.clients.admin.TransactionState toAdminState() {
        switch (this) {
            case EMPTY:
                return org.apache.kafka.clients.admin.TransactionState.EMPTY;
            case ONGOING:
                return org.apache.kafka.clients.admin.TransactionState.ONGOING;
            case PREPARE_COMMIT:
                return org.apache.kafka.clients.admin.TransactionState.PREPARE_COMMIT;
            case PREPARE_ABORT:
                return org.apache.kafka.clients.admin.TransactionState.PREPARE_ABORT;
            case COMPLETE_COMMIT:
                return org.apache.kafka.clients.admin.TransactionState.COMPLETE_COMMIT;
            case COMPLETE_ABORT:
                return org.apache.kafka.clients.admin.TransactionState.COMPLETE_ABORT;
            case PREPARE_EPOCH_FENCE:
                return org.apache.kafka.clients.admin.TransactionState.PREPARE_EPOCH_FENCE;
            case DEAD:
            default:
                return org.apache.kafka.clients.admin.TransactionState.UNKNOWN;
        }
    }
}
