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

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.inferred.freebuilder.shaded.com.google.common.collect.Maps;

/**
 * Transaction metadata.
 */
@Slf4j
@Builder
@Data
public class TransactionMetadata {

    private static final int DefaultTxnTimeOutMs = 1000 * 60;

    // transactional id
    private String transactionalId;

    //producer id
    private long producerId;

    // current epoch of the producer
    private short producerEpoch;

    // timeout to be used to abort long running transactions
    private int txnTimeoutMs = DefaultTxnTimeOutMs;

    // current state of the transaction
    private TransactionState state;

    // current set of partitions that are part of this transaction
    private Set<TopicPartition> topicPartitions;

    // time the transaction was started, i.e., when first partition is added
    private long txnStartTimestamp;

    // updated when any operation updates the TransactionMetadata. To be used for expiration
    private long txnLastUpdateTimestamp;

    // pending state is used to indicate the state that this transaction is going to
    // transit to, and for blocking future attempts to transit it again if it is not legal;
    // initialized as the same as the current state
    private TransactionState pendingState;

    private static Map<TransactionState, Set<TransactionState>> validPreviousStates;

    static {
        validPreviousStates = Maps.newHashMap();
        validPreviousStates.put(TransactionState.EMPTY, Sets.immutableEnumSet(
                TransactionState.EMPTY,
                TransactionState.COMPLETE_COMMIT,
                TransactionState.COMPLETE_ABORT));

        validPreviousStates.put(TransactionState.ONGOING, Sets.immutableEnumSet(
                TransactionState.ONGOING,
                TransactionState.EMPTY,
                TransactionState.COMPLETE_COMMIT,
                TransactionState.COMPLETE_ABORT));

        validPreviousStates.put(TransactionState.PREPARE_COMMIT, Sets.immutableEnumSet(
                TransactionState.ONGOING));

        validPreviousStates.put(TransactionState.PREPARE_ABORT, Sets.immutableEnumSet(
                TransactionState.ONGOING,
                TransactionState.PREPARE_EPOCH_FENCE));

        validPreviousStates.put(TransactionState.COMPLETE_COMMIT, Sets.immutableEnumSet(
                TransactionState.PREPARE_COMMIT));

        validPreviousStates.put(TransactionState.COMPLETE_ABORT, Sets.immutableEnumSet(
                TransactionState.PREPARE_ABORT));

        validPreviousStates.put(TransactionState.DEAD, Sets.immutableEnumSet(
                TransactionState.EMPTY,
                TransactionState.COMPLETE_COMMIT,
                TransactionState.COMPLETE_ABORT));

        validPreviousStates.put(TransactionState.PREPARE_EPOCH_FENCE, Sets.immutableEnumSet(
                TransactionState.ONGOING));
    }

    public TxnTransitMetadata prepareTransitionTo(TransactionState newState,
                                     long newProducerId,
                                     short newEpoch,
                                     int newTxnTimeoutMs,
                                     Set<TopicPartition> newTopicPartitions,
                                     long newTxnStartTimestamp,
                                     long updateTimestamp) {
        if (pendingState != null) {
            throw new IllegalStateException("Preparing transaction state transition to " + newState
                    + " while it already a pending state " + pendingState);
        }

        if (newProducerId < 0) {
            throw new IllegalArgumentException("Illegal new producer id $newProducerId");
        }

        if (newEpoch < 0) {
            throw new IllegalArgumentException("Illegal new producer epoch $newEpoch");
        }

        // check that the new state transition is valid and update the pending state if necessary
        if (validPreviousStates.get(newState).contains(state)) {
            pendingState = newState;
            return new TxnTransitMetadata(newProducerId, newEpoch, newTxnTimeoutMs, newState,
                    newTopicPartitions, newTxnStartTimestamp, updateTimestamp);
        } else {
            throw new IllegalStateException("Preparing transaction state transition to " + newState
                    + "failed since the target state" + newState
                    + "  is not a valid previous state of the current state " + state);
        }
    }

    /**
     * Transaction transit metadata.
     */
    @AllArgsConstructor
    @Data
    public static class TxnTransitMetadata {
        private long producerId;
        private short producerEpoch;
        private int txnTimeoutMs;
        private TransactionState txnState;
        private Set<TopicPartition> topicPartitions;
        private long txnStartTimestamp;
        private long txnLastUpdateTimestamp;
    }

    public void completeTransitionTo(TxnTransitMetadata transitMetadata) throws Exception {
        // metadata transition is valid only if all the following conditions are met:
        //
        // 1. the new state is already indicated in the pending state.
        // 2. the epoch should be either the same value, the old value + 1, or 0 if we have a new producerId.
        // 3. the last update time is no smaller than the old value.
        // 4. the old partitions set is a subset of the new partitions set.
        //
        // plus, we should only try to update the metadata after the corresponding log entry has been successfully
        // written and replicated (see TransactionStateManager#appendTransactionToLog)
        //
        // if valid, transition is done via overwriting the whole object to ensure synchronization

        TransactionState toState = pendingState;

        if (toState != transitMetadata.txnState) {
            throwStateTransitionFailure(transitMetadata);
        } else {
            switch(toState) {
                case EMPTY: // from initPid
                    if ((producerEpoch != transitMetadata.producerEpoch && !validProducerEpochBump(transitMetadata))
                            || transitMetadata.topicPartitions.isEmpty()
                            || transitMetadata.txnStartTimestamp != -1) {

                        throwStateTransitionFailure(transitMetadata);
                    } else {
                        txnTimeoutMs = transitMetadata.txnTimeoutMs;
                        producerEpoch = transitMetadata.producerEpoch;
                        producerId = transitMetadata.producerId;
                    }
                    break;
                case ONGOING: // from addPartitions
                    if (!validProducerEpoch(transitMetadata)
                            // || !transitMetadata.topicPartitions.containsAll(topicPartitions)
                            || txnTimeoutMs != transitMetadata.txnTimeoutMs
                            || txnStartTimestamp > transitMetadata.txnStartTimestamp) {

                        throwStateTransitionFailure(transitMetadata);
                    } else {
                        txnStartTimestamp = transitMetadata.txnStartTimestamp;
                        topicPartitions.addAll(transitMetadata.topicPartitions);
                    }
                    break;
                case PREPARE_ABORT: // from endTxn
                case PREPARE_COMMIT: // from endTxn
                    if (!validProducerEpoch(transitMetadata)
                            || !topicPartitions.equals(transitMetadata.topicPartitions)
                            || txnTimeoutMs != transitMetadata.txnTimeoutMs
                            || txnStartTimestamp != transitMetadata.txnStartTimestamp) {

                        throwStateTransitionFailure(transitMetadata);
                    }
                    break;
                case COMPLETE_ABORT: // from write markers
                case COMPLETE_COMMIT: // from write markers
                    if (!validProducerEpoch(transitMetadata)
                            || txnTimeoutMs != transitMetadata.txnTimeoutMs
                            || transitMetadata.txnStartTimestamp == -1) {

                        throwStateTransitionFailure(transitMetadata);
                    } else {
                        this.txnStartTimestamp = transitMetadata.txnStartTimestamp;
                        this.topicPartitions.clear();
                    }
                    break;
                case PREPARE_EPOCH_FENCE:
                    // We should never get here, since once we prepare to fence the epoch,
                    // we immediately set the pending state
                    // to PrepareAbort, and then consequently to CompleteAbort after the markers are written..
                    // So we should never ever try to complete a transition to PrepareEpochFence,
                    // as it is not a valid previous state for any other state,
                    // and hence can never be transitioned out of.
                    throwStateTransitionFailure(transitMetadata);
                    break;
                case DEAD:
                    // The transactionalId was being expired. The completion of the operation should result in
                    // removal of the the metadata from the cache,
                    // so we should never realistically transition to the dead state.
                    throw new IllegalStateException("TransactionalId " + transactionalId
                            + "is trying to complete a transition to "
                            + toState + ". This means that the transactionalId was being expired, and the only "
                            + "acceptable completion of this operation is to remove the transaction metadata "
                            + "from the cache, not to persist the " + toState + "in the");
            }

            log.info("TransactionalId {} complete transition from {} to {}", transactionalId, state, transitMetadata);
            this.txnLastUpdateTimestamp = transitMetadata.txnLastUpdateTimestamp;
            this.pendingState = null;
            this.state = toState;
        }
    }

    private boolean validProducerEpoch(TxnTransitMetadata transitMetadata) {
        short transitEpoch = transitMetadata.producerEpoch;
        long transitProducerId = transitMetadata.producerId;
        return transitEpoch == producerEpoch && transitProducerId == producerId;
    }

    private boolean validProducerEpochBump(TxnTransitMetadata transitMetadata) {
        short transitEpoch = transitMetadata.producerEpoch;
        long transitProducerId = transitMetadata.producerId;
        return transitEpoch == producerEpoch + 1 || (transitEpoch == 0 && transitProducerId != producerId);
    }

    public TxnTransitMetadata prepareAddPartitions(Set<TopicPartition> addedTopicPartitions, Long updateTimestamp) {
        long newTxnStartTimestamp;
        switch(state) {
            case EMPTY:
            case COMPLETE_ABORT:
            case COMPLETE_COMMIT:
                newTxnStartTimestamp = updateTimestamp;
                break;
            default:
                newTxnStartTimestamp = txnStartTimestamp;
        }
        Set<TopicPartition> newPartitionSet = new HashSet<>();
        if (topicPartitions != null) {
            newPartitionSet.addAll(topicPartitions);
        }
        newPartitionSet.addAll(new HashSet<>(addedTopicPartitions));
        return prepareTransitionTo(TransactionState.ONGOING, producerId, producerEpoch,
                txnTimeoutMs, newPartitionSet, newTxnStartTimestamp, updateTimestamp);
    }

    public TxnTransitMetadata prepareAbortOrCommit(TransactionState newState, Long updateTimestamp) {
        return prepareTransitionTo(newState, producerId, producerEpoch,
                txnTimeoutMs, topicPartitions, txnStartTimestamp, updateTimestamp);
    }

    public TxnTransitMetadata prepareComplete(Long updateTimestamp) {
        TransactionState newState;
        if (state == TransactionState.PREPARE_COMMIT) {
            newState = TransactionState.COMPLETE_COMMIT;
        } else {
            newState = TransactionState.COMPLETE_ABORT;
        }
        return prepareTransitionTo(newState, producerId, producerEpoch,
                txnTimeoutMs, Collections.emptySet(), txnStartTimestamp, updateTimestamp);
    }

    private void throwStateTransitionFailure(TxnTransitMetadata txnTransitMetadata) throws IllegalStateException {
        log.error("{} transition to {} failed: this should not happen.", this, txnTransitMetadata);
        throw new IllegalStateException("TransactionalId " + transactionalId + " failed transition to state "
                + txnTransitMetadata + "due to unexpected metadata");
    }

}
