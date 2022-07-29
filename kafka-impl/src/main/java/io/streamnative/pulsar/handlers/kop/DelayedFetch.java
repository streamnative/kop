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
package io.streamnative.pulsar.handlers.kop;

import io.streamnative.pulsar.handlers.kop.storage.PartitionLog;
import io.streamnative.pulsar.handlers.kop.storage.ReplicaManager;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperation;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.FetchRequest;

@Slf4j
public class DelayedFetch extends DelayedOperation {
    private final CompletableFuture<Map<TopicPartition, PartitionLog.ReadRecordsResult>> callback;
    private final ReplicaManager replicaManager;
    private final int fetchMaxBytes;
    private final AtomicInteger maxReadEntriesNum;
    final boolean readCommitted;
    final String namespacePrefix;

    final MessageFetchContext context;

    private volatile Boolean hasError;

    protected static final AtomicReferenceFieldUpdater<DelayedFetch, Boolean> HAS_ERROR_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(DelayedFetch.class, Boolean.class, "hasError");

    private final Map<TopicPartition, FetchRequest.PartitionData> readPartitionInfo;

    private final Map<TopicPartition, PartitionLog.ReadRecordsResult> readRecordsResult;

    public DelayedFetch(long delayMs,
                        final int fetchMaxBytes,
                        final int maxReadEntriesNum,
                        final boolean readCommitted,
                        final String namespacePrefix,
                        final MessageFetchContext context,
                        Map<TopicPartition, FetchRequest.PartitionData> readPartitionInfo,
                        Map<TopicPartition, PartitionLog.ReadRecordsResult> readRecordsResult,
                        ReplicaManager replicaManager,
                        CompletableFuture<Map<TopicPartition, PartitionLog.ReadRecordsResult>> callback) {
        super(delayMs, Optional.empty());
        this.readCommitted = readCommitted;
        this.namespacePrefix = namespacePrefix;
        this.context = context;
        this.callback = callback;
        this.readRecordsResult = readRecordsResult;
        this.readPartitionInfo = readPartitionInfo;
        this.replicaManager = replicaManager;
        this.fetchMaxBytes = fetchMaxBytes;
        this.maxReadEntriesNum = new AtomicInteger(maxReadEntriesNum);
        this.hasError = false;
    }

    @Override
    public void onExpiration() {
        log.info("[Test] onExpiration {}", fetchMaxBytes);
        if (this.callback.isDone()) {
            return;
        }
        callback.complete(readRecordsResult);
    }

    @Override
    public void onComplete() {
        log.info("[Test] onComplete {}", fetchMaxBytes);
        if (this.callback.isDone()) {
            return;
        }
        if (HAS_ERROR_UPDATER.get(this)) {
            callback.complete(readRecordsResult);
        }
        replicaManager.readFromLocalLog(
            readCommitted, namespacePrefix, fetchMaxBytes, maxReadEntriesNum.get(), readPartitionInfo, context
        ).thenAccept(readRecordsResult -> {
            this.context.getStatsLogger().getWaitingFetchesTriggered().add(1);
            this.callback.complete(readRecordsResult);
        }).thenAccept(__ -> {
            readRecordsResult.forEach((ignore, result) -> {
                if (result.decodeResult() != null) {
                    result.decodeResult().recycle();
                }
            });
        });
    }

    @Override
    public boolean tryComplete() {
        if (this.callback.isDone()) {
            return true;
        }
        for (Map.Entry<TopicPartition, PartitionLog.ReadRecordsResult> entry : readRecordsResult.entrySet()) {
            TopicPartition tp = entry.getKey();
            PartitionLog.ReadRecordsResult result = entry.getValue();
            PartitionLog partitionLog = replicaManager.getPartitionLog(tp, namespacePrefix);
            PositionImpl currLastPosition = (PositionImpl) partitionLog.getLastPosition(context.getTopicManager());
            if (currLastPosition.compareTo(PositionImpl.EARLIEST) == 0) {
                HAS_ERROR_UPDATER.compareAndSet(this, false, true);
                return forceComplete();
            }
            PositionImpl lastPosition = (PositionImpl) result.lastPosition();
            log.info("[Test] currLastPosition {} lastPosition {}", currLastPosition, lastPosition);
            if (currLastPosition.compareTo(lastPosition) > 0) {
                this.maxReadEntriesNum.set(maxReadEntriesNum.get() * 10);
                return forceComplete();
            }
        }
        return false;
    }
}
