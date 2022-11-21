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
package io.streamnative.pulsar.handlers.kop.storage;

import com.google.common.collect.ImmutableMap;
import io.streamnative.pulsar.handlers.kop.DelayedFetch;
import io.streamnative.pulsar.handlers.kop.DelayedProduceAndFetch;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.MessageFetchContext;
import io.streamnative.pulsar.handlers.kop.RequestStats;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperation;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationKey;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationPurgatory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.service.plugin.EntryFilterWithClassLoader;

/**
 * Used to append records. Mapping to Kafka ReplicaManager.scala.
 */
@Slf4j
public class ReplicaManager {
    private final PartitionLogManager logManager;
    private final DelayedOperationPurgatory<DelayedOperation> producePurgatory;
    private final DelayedOperationPurgatory<DelayedOperation> fetchPurgatory;
    private final String metadataNamespace;

    public ReplicaManager(KafkaServiceConfiguration kafkaConfig,
                          RequestStats requestStats,
                          Time time,
                          ImmutableMap<String, EntryFilterWithClassLoader> entryfilterMap,
                          DelayedOperationPurgatory<DelayedOperation> producePurgatory,
                          DelayedOperationPurgatory<DelayedOperation> fetchPurgatory) {
        this.logManager = new PartitionLogManager(kafkaConfig, requestStats, entryfilterMap,
                time);
        this.producePurgatory = producePurgatory;
        this.fetchPurgatory = fetchPurgatory;
        this.metadataNamespace = kafkaConfig.getKafkaMetadataNamespace();
    }

    public PartitionLog getPartitionLog(TopicPartition topicPartition, String namespacePrefix) {
        return logManager.getLog(topicPartition, namespacePrefix);
    }

    @AllArgsConstructor
    private static class PendingProduceCallback implements Runnable {

        final AtomicInteger topicPartitionNum;
        Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap;
        final CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> completableFuture;
        Map<TopicPartition, MemoryRecords> entriesPerPartition;
        @Override
        public void run() {
            topicPartitionNum.set(0);
            if (completableFuture.isDone()) {
                // It may be triggered again in DelayedProduceAndFetch
                return;
            }
            // add the topicPartition with timeout error if it's not existed in responseMap
            entriesPerPartition.keySet().forEach(topicPartition -> {
                if (!responseMap.containsKey(topicPartition)) {
                    responseMap.put(topicPartition, new ProduceResponse.PartitionResponse(Errors.REQUEST_TIMED_OUT));
                }
            });
            if (log.isDebugEnabled()) {
                log.debug("Complete handle appendRecords.");
            }
            completableFuture.complete(responseMap);

            // clear references to data,
            // this object will be retained in the purgatory
            // we don't need to keep a hard reference to the data
            // one we passed responseMap to the caller
            responseMap = null;
            entriesPerPartition = null;
        }
    }

    public CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> appendRecords(
            final long timeout,
            final boolean internalTopicsAllowed,
            final String namespacePrefix,
            final Map<TopicPartition, MemoryRecords> entriesPerPartition,
            final PartitionLog.AppendOrigin origin,
            final AppendRecordsContext appendRecordsContext) {
        CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> completableFuture =
                new CompletableFuture<>();
        final AtomicInteger topicPartitionNum = new AtomicInteger(entriesPerPartition.size());
        final Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap = new ConcurrentHashMap<>();

        PendingProduceCallback complete =
                new PendingProduceCallback(topicPartitionNum, responseMap, completableFuture, entriesPerPartition);
        BiConsumer<TopicPartition, ProduceResponse.PartitionResponse> addPartitionResponse =
                (topicPartition, response) -> {
            responseMap.put(topicPartition, response);
            // reset topicPartitionNum
            int restTopicPartitionNum = topicPartitionNum.decrementAndGet();
            if (restTopicPartitionNum < 0) {
                return;
            }
            if (restTopicPartitionNum == 0) {
                complete.run();
            }
        };
        entriesPerPartition.forEach((topicPartition, memoryRecords) -> {
            String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
            // reject appending to internal topics if it is not allowed
            if (!internalTopicsAllowed && KopTopic.isInternalTopic(fullPartitionName, metadataNamespace)) {
                addPartitionResponse.accept(topicPartition, new ProduceResponse.PartitionResponse(
                        Errors.forException(new InvalidTopicException(
                                String.format("Cannot append to internal topic %s", topicPartition.topic())))));
            } else {
                PartitionLog partitionLog = getPartitionLog(topicPartition, namespacePrefix);
                partitionLog.appendRecords(memoryRecords, origin, appendRecordsContext)
                        .thenAccept(offset -> addPartitionResponse.accept(topicPartition,
                                new ProduceResponse.PartitionResponse(Errors.NONE, offset, -1L, -1L)))
                        .exceptionally(ex -> {
                            addPartitionResponse.accept(topicPartition,
                                    new ProduceResponse.PartitionResponse(Errors.forException(ex.getCause())));
                            return null;
                        });
            }
        });
        // delay produce
        if (timeout <= 0) {
            complete.run();
        } else {
            // producePurgatory will retain a reference to the callback for timeout ms,
            // even if the operation succeeds
            List<Object> delayedCreateKeys =
                    entriesPerPartition.keySet().stream()
                            .map(DelayedOperationKey.TopicPartitionOperationKey::new).collect(Collectors.toList());
            DelayedProduceAndFetch delayedProduce = new DelayedProduceAndFetch(timeout, topicPartitionNum, complete);
            producePurgatory.tryCompleteElseWatch(delayedProduce, delayedCreateKeys);
        }
        return completableFuture;
    }


    public CompletableFuture<Map<TopicPartition, PartitionLog.ReadRecordsResult>> fetchMessage(
            final long timeout,
            final int fetchMinBytes,
            final int fetchMaxBytes,
            final ConcurrentHashMap<TopicPartition, FetchRequest.PartitionData> fetchInfos,
            final IsolationLevel isolationLevel,
            final MessageFetchContext context) {
        CompletableFuture<Map<TopicPartition, PartitionLog.ReadRecordsResult>> future =
                new CompletableFuture<>();
        final boolean readCommitted =
                (context.getTc() != null && isolationLevel.equals(IsolationLevel.READ_COMMITTED));
        final long startTime = SystemTime.SYSTEM.hiResClockMs();

        readFromLocalLog(readCommitted, fetchMaxBytes, context.getMaxReadEntriesNum(), fetchInfos, context)
                .thenAccept(readResults -> {
                    final MutableLong bytesReadable = new MutableLong(0);
                    final MutableBoolean errorReadingData = new MutableBoolean(false);
                    readResults.forEach((topicPartition, readRecordsResult) -> {
                        if (readRecordsResult.errors() != Errors.NONE) {
                            errorReadingData.setTrue();
                        }
                        if (readRecordsResult.decodeResult() != null) {
                            bytesReadable.addAndGet(readRecordsResult.decodeResult().getRecords().sizeInBytes());
                        }
                    });

                    long now = SystemTime.SYSTEM.hiResClockMs();
                    long currentWait = now - startTime;
                    long remainingMaxWait = timeout - currentWait;
                    long maxWait = Math.min(remainingMaxWait, timeout);
                    if (maxWait <= 0 || fetchInfos.isEmpty()
                            || bytesReadable.longValue() >= fetchMinBytes || errorReadingData.booleanValue()) {
                        future.complete(readResults);
                        return;
                    }
                    List<Object> delayedFetchKeys = fetchInfos.keySet().stream()
                            .map(DelayedOperationKey.TopicPartitionOperationKey::new).collect(Collectors.toList());
                    DelayedFetch delayedFetch = new DelayedFetch(
                            maxWait,
                            fetchMaxBytes,
                            bytesReadable.getValue(),
                            readCommitted,
                            context,
                            this,
                            fetchInfos,
                            readResults,
                            future
                    );
                    fetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedFetchKeys);
                });

        return future;
    }

    public CompletableFuture<Map<TopicPartition, PartitionLog.ReadRecordsResult>> readFromLocalLog(
            final boolean readCommitted,
            final int fetchMaxBytes,
            final int maxReadEntriesNum,
            final Map<TopicPartition, FetchRequest.PartitionData> readPartitionInfo,
            final MessageFetchContext context) {
        AtomicLong limitBytes = new AtomicLong(fetchMaxBytes);
        CompletableFuture<Map<TopicPartition, PartitionLog.ReadRecordsResult>> resultFuture = new CompletableFuture<>();
        ConcurrentHashMap<TopicPartition, PartitionLog.ReadRecordsResult> result = new ConcurrentHashMap<>();
        AtomicInteger restTopicPartitionNeedRead = new AtomicInteger(readPartitionInfo.size());

        Runnable complete = () -> {
            if (restTopicPartitionNeedRead.decrementAndGet() == 0) {
                resultFuture.complete(result);
            }
        };
        readPartitionInfo.forEach((tp, fetchInfo) -> {
            getPartitionLog(tp, context.getNamespacePrefix())
                    .readRecords(fetchInfo, readCommitted,
                            limitBytes, maxReadEntriesNum, context)
                    .thenAccept(readResult -> {
                        result.put(tp, readResult);
                        complete.run();
                    });
        });
        return resultFuture;
    }

    public void tryCompleteDelayedFetch(DelayedOperationKey key) {
        int completed = fetchPurgatory.checkAndComplete(key);
        if (log.isDebugEnabled()) {
            log.debug("Request key {} unblocked {} fetch requests.", key.keyLabel(), completed);
        }
    }

}
