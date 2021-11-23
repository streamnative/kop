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

import io.streamnative.pulsar.handlers.kop.DelayedProduceAndFetch;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.KafkaTopicManager;
import io.streamnative.pulsar.handlers.kop.PendingTopicFutures;
import io.streamnative.pulsar.handlers.kop.RequestStats;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperation;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationKey;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationPurgatory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;

@Slf4j
public class ReplicaManager {
    private final PartitionLogManager logManager;
    private final DelayedOperationPurgatory<DelayedOperation> producePurgatory;
    private final DelayedOperationPurgatory<DelayedOperation> fetchPurgatory;

    public ReplicaManager(KafkaServiceConfiguration config,
                          Time time,
                          DelayedOperationPurgatory<DelayedOperation> producePurgatory,
                          DelayedOperationPurgatory<DelayedOperation> fetchPurgatory) {
        this.logManager = new PartitionLogManager(config, time);
        this.producePurgatory = producePurgatory;
        this.fetchPurgatory = fetchPurgatory;
    }

    public PartitionLog getPartitionLog(TopicPartition topicPartition, String namespacePrefix) {
        return logManager.getLog(topicPartition, namespacePrefix);
    }

    public void appendRecords(
            final long timeout,
            final boolean internalTopicsAllowed,
            final short version,
            final KafkaTopicManager topicManager,
            final String namespacePrefix,
            final Map<TopicPartition, MemoryRecords> entriesPerPartition,
            final RequestStats requestStats,
            final TransactionCoordinator coordinator,
            final Consumer<Integer> startSendOperationForThrottlingConsumer,
            final Consumer<Integer> completeSendOperationForThrottlingConsumer,
            final Map<TopicPartition, PendingTopicFutures> pendingTopicFuturesMap,
            final CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback) {

        final AtomicInteger topicPartitionNum = new AtomicInteger(entriesPerPartition.size());
        final Map<TopicPartition, ProduceResponse.PartitionResponse> responseMap = new ConcurrentHashMap<>();

        Runnable complete = () -> {
            topicPartitionNum.set(0);
            if (responseCallback.isDone()) {
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
            responseCallback.complete(responseMap);
        };
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
            final Consumer<Long> offsetConsumer = offset -> addPartitionResponse.accept(
                    topicPartition,
                    new ProduceResponse.PartitionResponse(Errors.NONE, offset, -1L, -1L));
            final Consumer<Errors> errorsConsumer =
                    errors -> addPartitionResponse
                            .accept(topicPartition, new ProduceResponse.PartitionResponse(errors));
            final Consumer<Throwable> exceptionConsumer =
                    e -> addPartitionResponse
                            .accept(topicPartition, new ProduceResponse.PartitionResponse(Errors.forException(e)));

            String fullPartitionName = KopTopic.toString(topicPartition, namespacePrefix);
            // reject appending to internal topics if it is not allowed
            if (!internalTopicsAllowed && KopTopic.isInternalTopic(fullPartitionName)) {
                exceptionConsumer.accept(new InvalidTopicException(
                        String.format("Cannot append to internal topic %s", topicPartition.topic())));
            } else {
                PartitionLog partitionLog = getPartitionLog(topicPartition, namespacePrefix);
                partitionLog.appendRecords(memoryRecords,
                        version,
                        topicManager,
                        requestStats,
                        coordinator,
                        offsetConsumer,
                        errorsConsumer,
                        exceptionConsumer,
                        startSendOperationForThrottlingConsumer,
                        completeSendOperationForThrottlingConsumer,
                        pendingTopicFuturesMap);
            }

        });
        // delay produce
        if (timeout <= 0) {
            complete.run();
        } else {
            List<Object> delayedCreateKeys =
                    entriesPerPartition.keySet().stream()
                            .map(DelayedOperationKey.TopicPartitionOperationKey::new).collect(Collectors.toList());
            DelayedProduceAndFetch delayedProduce = new DelayedProduceAndFetch(timeout, topicPartitionNum, complete);
            producePurgatory.tryCompleteElseWatch(delayedProduce, delayedCreateKeys);
        }

    }

}
