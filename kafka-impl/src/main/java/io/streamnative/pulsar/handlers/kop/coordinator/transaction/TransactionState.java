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
    EMPTY,

    /**
     * Transaction has started and ongoing.
     * transition: received EndTxnRequest with commit => PrepareCommit
     *             received EndTxnRequest with abort => PrepareAbort
     *             received AddPartitionsToTxnRequest => Ongoing
     *             received AddOffsetsToTxnRequest => Ongoing
     */
    ONGOING,

    /**
     * Group is preparing to commit.
     * transition: received acks from all partitions => CompleteCommit
     */
    PREPARE_COMMIT,

    /**
     * Group is preparing to abort.
     * transition: received acks from all partitions => CompleteAbort
     */
    PREPARE_ABORT,

    /**
     * Group has completed commit.
     * Will soon be removed from the ongoing transaction cache
     */
    COMPLETE_COMMIT,

    /**
     * Group has completed abort.
     * Will soon be removed from the ongoing transaction cache
     */
    COMPLETE_ABORT,

    /**
     * TransactionalId has expired and is about to be removed from the transaction cache.
     */
    DEAD,

    /**
     * We are in the middle of bumping the epoch and fencing out older producers.
     */
    PREPARE_EPOCH_FENCE;

}
