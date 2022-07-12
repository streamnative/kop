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
import io.streamnative.pulsar.handlers.kop.scala.Either;
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;

/**
 * Transaction metadata.
 */
@Slf4j
@Builder
@Data
@AllArgsConstructor
public class TransactionMetadata {

    private static final int DefaultTxnTimeOutMs = 1000 * 60;

    // Transactional id
    private String transactionalId;

    // Producer id
    private long producerId;

    // Last producer id assigned to the producer
    private long lastProducerId;

    // Current epoch of the producer
    private short producerEpoch;

    // Last epoch of the producer
    private short lastProducerEpoch;

    // Timeout to be used to abort long running transactions
    private int txnTimeoutMs = DefaultTxnTimeOutMs;

    // Current state of the transaction
    private TransactionState state;

    // Current set of partitions that are part of this transaction
    private Set<TopicPartition> topicPartitions;

    // Time the transaction was started, i.e., when first partition is added
    private long txnStartTimestamp;

    // Updated when any operation updates the TransactionMetadata. To be used for expiration
    private long txnLastUpdateTimestamp;

    // Pending state is used to indicate the state that this transaction is going to
    // transit to, and for blocking future attempts to transit it again if it is not legal;
    // initialized as the same as the current state
    @Builder.Default
    private Optional<TransactionState> pendingState = Optional.empty();

    // Indicates that during a previous attempt to fence a producer, the bumped epoch may not have been
    // successfully written to the log. If this is true, we will not bump the epoch again when fencing
    private boolean hasFailedEpochFence = false;

    private final ReentrantLock lock = new ReentrantLock();

    private static Map<TransactionState, Set<TransactionState>> validPreviousStates;

    static {
        validPreviousStates = new HashMap<>();
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

    public <T> T inLock(Supplier<T> supplier) {
        return CoreUtils.inLock(lock, supplier);
    }

    public TxnTransitMetadata prepareTransitionTo(TransactionState newState,
                                     long newProducerId,
                                     short newEpoch,
                                     short newLastEpoch,
                                     int newTxnTimeoutMs,
                                     Set<TopicPartition> newTopicPartitions,
                                     long newTxnStartTimestamp,
                                     long updateTimestamp) {
        if (pendingState.isPresent()) {
            throw new IllegalStateException("Preparing transaction state transition to " + newState
                    + " while it already a pending state " + pendingState.get());
        }

        if (newProducerId < 0) {
            throw new IllegalArgumentException("Illegal new producer id $newProducerId");
        }

        if (newEpoch < 0) {
            throw new IllegalArgumentException("Illegal new producer epoch $newEpoch");
        }

        // check that the new state transition is valid and update the pending state if necessary
        if (validPreviousStates.get(newState).contains(state)) {
            TxnTransitMetadata txnTransitMetadata = TxnTransitMetadata.builder()
                    .producerId(newProducerId)
                    .lastProducerId(producerId)
                    .producerEpoch(newEpoch)
                    .lastProducerEpoch(newLastEpoch)
                    .txnTimeoutMs(newTxnTimeoutMs)
                    .txnState(newState)
                    .topicPartitions(newTopicPartitions)
                    .txnStartTimestamp(newTxnStartTimestamp)
                    .txnLastUpdateTimestamp(updateTimestamp).build();
            if (log.isDebugEnabled()) {
                log.debug("TransactionalId {} prepare transition from {} to {}",
                        transactionalId, state, txnTransitMetadata);
            }
            pendingState = Optional.of(newState);
            return txnTransitMetadata;
        } else {
            throw new IllegalStateException("Preparing transaction state transition to " + newState
                    + "failed since the target state" + newState
                    + "  is not a valid previous state of the current state " + state);
        }
    }

    /**
     * Transaction transit metadata.
     */
    @ToString
    @Builder
    @AllArgsConstructor
    @Data
    public static class TxnTransitMetadata {
        private long producerId;
        private long lastProducerId;
        private short producerEpoch;
        private short lastProducerEpoch;
        private int txnTimeoutMs;
        private TransactionState txnState;
        private Set<TopicPartition> topicPartitions;
        private long txnStartTimestamp;
        private long txnLastUpdateTimestamp;
    }

    public void completeTransitionTo(TxnTransitMetadata transitMetadata) {
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

        if (!pendingState.isPresent()) {
            throw new IllegalStateException("TransactionalId " + transactionalId
                    + " completing transaction state transition while it does not have a pending state");
        }
        TransactionState toState = pendingState.get();

        if (toState != transitMetadata.txnState) {
            throwStateTransitionFailure(transitMetadata);
        } else {
            switch(toState) {
                case EMPTY: // from initPid
                    if ((producerEpoch != transitMetadata.producerEpoch && !validProducerEpochBump(transitMetadata))
                            || !transitMetadata.topicPartitions.isEmpty()
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
            this.pendingState = Optional.empty();
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

    // this is visible for test only
    public TxnTransitMetadata prepareNoTransit() {
        // do not call transitTo as it will set the pending state,
        // a follow-up call to abort the transaction will set its pending state
        return new TxnTransitMetadata(producerId, lastProducerId, producerEpoch, lastProducerEpoch, txnTimeoutMs,
                state, topicPartitions, txnStartTimestamp, txnLastUpdateTimestamp);
    }

    public TxnTransitMetadata prepareFenceProducerEpoch() {
        if (producerEpoch == Short.MAX_VALUE) {
            throw new IllegalStateException("Cannot fence producer with epoch equal to Short.MAX_VALUE "
                    + "since this would overflow");
        }

        // If we've already failed to fence an epoch (because the write to the log failed), we don't increase it again.
        // This is safe because we never return the epoch to client if we fail to fence the epoch
        short bumpedEpoch;
        if (hasFailedEpochFence) {
            bumpedEpoch = producerEpoch;
        } else {
            bumpedEpoch = (short) (producerEpoch + 1);
        }

        return prepareTransitionTo(
                TransactionState.PREPARE_EPOCH_FENCE,
                producerId,
                bumpedEpoch,
                RecordBatch.NO_PRODUCER_EPOCH,
                txnTimeoutMs,
                topicPartitions,
                txnStartTimestamp,
                txnLastUpdateTimestamp);
    }

    public Either<Errors, TxnTransitMetadata> prepareIncrementProducerEpoch(Integer newTxnTimeoutMs,
                                                                            Optional<Short> expectedProducerEpoch,
                                                                            Long updateTimestamp) {
        if (isProducerEpochExhausted()) {
            throw new IllegalStateException("Cannot allocate any more producer epochs for producerId $producerId");
        }

        short bumpedEpoch = (short) (producerEpoch + 1);

        final Either<Errors, BumpEpochResult> errorsOrBumpEpochResult;
        if (!expectedProducerEpoch.isPresent()) {
            // If no expected epoch was provided by the producer, bump the current epoch and set the last epoch to -1
            // In the case of a new producer, producerEpoch will be -1 and bumpedEpoch will be 0
            errorsOrBumpEpochResult = Either.right(new BumpEpochResult(bumpedEpoch, RecordBatch.NO_PRODUCER_EPOCH));
        } else {
            if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH || expectedProducerEpoch.get() == producerEpoch) {
                // If the expected epoch matches the current epoch, or if there is no current epoch, the producer is
                // attempting to continue after an error and no other producer has been initialized. Bump the current
                // and last epochs. The no current epoch case means this is a new producer; producerEpoch will be -1
                // and bumpedEpoch will be 0
                errorsOrBumpEpochResult = Either.right(new BumpEpochResult(bumpedEpoch, producerEpoch));
            } else if (expectedProducerEpoch.get() == lastProducerEpoch) {
                // If the expected epoch matches the previous epoch, it is a retry of a successful call, so just return
                // the current epoch without bumping. There is no danger of this producer being fenced, because a new
                // producer calling InitProducerId would have caused the last epoch to be set to -1.
                // Note that if the IBP is prior to 2.4.IV1, the lastProducerId and lastProducerEpoch will not be
                // written to the transaction log, so a retry that spans a coordinator change will fail. We expect
                // this to be a rare case.
                errorsOrBumpEpochResult = Either.right(new BumpEpochResult(producerEpoch, lastProducerEpoch));
            } else {
                // Otherwise, the producer has a fenced epoch and should receive an PRODUCER_FENCED error
                log.info("Expected producer epoch {} does not match current "
                        + "producer epoch {} or previous producer epoch {}",
                        expectedProducerEpoch, producerEpoch, lastProducerEpoch);
                // TODO the error should be Errors.PRODUCER_FENCED
                errorsOrBumpEpochResult = Either.left(Errors.UNKNOWN_SERVER_ERROR);
            }
        }

        return errorsOrBumpEpochResult.map(bumpEpochResult -> prepareTransitionTo(TransactionState.EMPTY,
                producerId, bumpEpochResult.bumpedEpoch, bumpEpochResult.lastEpoch, newTxnTimeoutMs,
                Collections.emptySet(), -1, updateTimestamp));
    }

    @AllArgsConstructor
    private static class BumpEpochResult {
        private final short bumpedEpoch;
        private final short lastEpoch;
    }

    public TxnTransitMetadata prepareProducerIdRotation(Long newProducerId,
                                  Integer newTxnTimeoutMs,
                                  Long updateTimestamp,
                                  Boolean recordLastEpoch) {
        if (hasPendingTransaction()) {
            throw new IllegalStateException("Cannot rotate producer ids while a transaction is still pending");
        }

        return prepareTransitionTo(
                TransactionState.EMPTY,
                newProducerId,
                (short) 0,
                recordLastEpoch ? producerEpoch : RecordBatch.NO_PRODUCER_EPOCH,
                newTxnTimeoutMs,
                Collections.emptySet(),
                -1,
                updateTimestamp);
    }

    private boolean hasPendingTransaction() {
        boolean flag = false;
        switch (state) {
            case ONGOING:
            case PREPARE_ABORT:
            case PREPARE_COMMIT:
                flag = true;
                break;
            default:
                // no op
        }
        return flag;
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
        return prepareTransitionTo(TransactionState.ONGOING, producerId, producerEpoch, lastProducerEpoch,
                txnTimeoutMs, newPartitionSet, newTxnStartTimestamp, updateTimestamp);
    }

    public TxnTransitMetadata prepareAbortOrCommit(TransactionState newState, Long updateTimestamp) {
        return prepareTransitionTo(newState, producerId, producerEpoch, lastProducerEpoch,
                txnTimeoutMs, topicPartitions, txnStartTimestamp, updateTimestamp);
    }

    public TxnTransitMetadata prepareComplete(Long updateTimestamp) {
        TransactionState newState;
        if (state == TransactionState.PREPARE_COMMIT) {
            newState = TransactionState.COMPLETE_COMMIT;
        } else {
            newState = TransactionState.COMPLETE_ABORT;
        }
        // Since the state change was successfully written to the log, unset the flag for a failed epoch fence
        hasFailedEpochFence = false;

        return prepareTransitionTo(newState, producerId, producerEpoch, lastProducerEpoch,
                txnTimeoutMs, Collections.emptySet(), txnStartTimestamp, updateTimestamp);
    }

    public TxnTransitMetadata prepareDead() {
        return prepareTransitionTo(TransactionState.DEAD, producerId, producerEpoch, lastProducerEpoch, txnTimeoutMs,
                Collections.emptySet(), txnStartTimestamp, txnLastUpdateTimestamp);
    }

    private void throwStateTransitionFailure(TxnTransitMetadata txnTransitMetadata) throws IllegalStateException {
        log.error("{} transition to {} failed: this should not happen.", this, txnTransitMetadata);
        throw new IllegalStateException("TransactionalId " + transactionalId + " failed transition to state "
                + txnTransitMetadata + " due to unexpected metadata");
    }

    public boolean isProducerEpochExhausted() {
        return isEpochExhausted(producerEpoch);
    }

    public boolean isEpochExhausted(short producerEpoch) {
        return producerEpoch >= Short.MAX_VALUE - 1;
    }

    public void removePartition(TopicPartition topicPartition) {
        if (state != TransactionState.PREPARE_COMMIT && state != TransactionState.PREPARE_ABORT) {
            throw new IllegalStateException(
                    String.format("Transaction metadata's current state is %s, and its pending state is %s while "
                                + "trying to remove partitions whose txn marker has been sent, this is not expected",
                                state, pendingState));
        }
        topicPartitions.remove(topicPartition);
    }

    public void addPartitions(Set<TopicPartition> partitions) {
        topicPartitions.addAll(partitions);
    }

    public Boolean pendingTransitionInProgress() {
        return this.pendingState.isPresent();
    }

}
