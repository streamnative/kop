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

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * Pending context related to the produce task.
 */
@Slf4j
public class PendingProduce {

    private final CompletableFuture<PartitionResponse> responseFuture;
    private final KafkaTopicManager topicManager;
    private final String partitionName;
    private final int numMessages;
    private final CompletableFuture<PersistentTopic> topicFuture;
    private final CompletableFuture<List<ByteBuf>> byteBufFutureList;
    private CompletableFuture<Long> offsetFuture;

    public PendingProduce(CompletableFuture<PartitionResponse> responseFuture,
                          KafkaTopicManager topicManager,
                          String partitionName,
                          EntryFormatter entryFormatter,
                          MemoryRecords memoryRecords,
                          ExecutorService executor) {
        this.responseFuture = responseFuture;
        this.topicManager = topicManager;
        this.partitionName = partitionName;
        this.numMessages = EntryFormatter.parseNumMessages(memoryRecords);

        this.topicFuture = topicManager.getTopic(partitionName).exceptionally(e -> {
            log.error("Failed to getTopic for partition '{}': {}", partitionName, e);
            return null;
        });
        this.byteBufFutureList = new CompletableFuture<>();
        this.byteBufFutureList.exceptionally(e -> {
            log.error("Failed to compute ByteBuf for partition '{}': {}", partitionName, e);
            return null;
        });
        executor.execute(() -> {
            List<MemoryRecords> memoryRecordsList = EntryFormatter
                    .splitLargeBatchRecords(memoryRecords, numMessages);
            List<ByteBuf> byteBufList = new ArrayList<>();
            memoryRecordsList.forEach(memoryRecordsInList -> {
                ByteBuf byteBuf = entryFormatter.encode(memoryRecordsInList);
                byteBufList.add(byteBuf);
            });
            this.byteBufFutureList.complete(byteBufList);
        });
        this.offsetFuture = new CompletableFuture<>();
    }

    public boolean ready() {
        return topicFuture.isDone()
                && !topicFuture.isCompletedExceptionally()
                && byteBufFutureList.isDone()
                && !byteBufFutureList.isCompletedExceptionally();
    }

    public void whenComplete(Runnable runnable) {
        CompletableFuture.allOf(topicFuture, byteBufFutureList).whenComplete((ignored, e) -> {
            if (e == null) {
                runnable.run();
            } else {
                // The error logs have already been printed, so we needn't log error again.
                if (topicFuture.isCompletedExceptionally()) {
                    responseFuture.complete(new PartitionResponse(Errors.LEADER_NOT_AVAILABLE));
                } else if (byteBufFutureList.isCompletedExceptionally()) {
                    responseFuture.complete(new PartitionResponse(Errors.CORRUPT_MESSAGE));
                } else {
                    responseFuture.completeExceptionally(e);
                }
            }
        });
    }

    public void publishMessages() {
        if (!ready()) {
            throw new RuntimeException("Try to send while PendingProduce is not ready");
        }
        PersistentTopic persistentTopic;
        List<ByteBuf> byteBufList;
        try {
            persistentTopic = topicFuture.get();
            byteBufList = byteBufFutureList.get();
        } catch (InterruptedException | ExecutionException e) {
            // It shouldn't fail because we've already checked ready() before.
            throw new RuntimeException(e);
        }

        if (log.isDebugEnabled()) {
            log.debug("publishMessages for topic partition: {}, records size is {}", partitionName, numMessages);
        }
        AtomicInteger byteBufSize = new AtomicInteger();
        topicManager.registerProducerInPersistentTopic(partitionName, persistentTopic);
        byteBufList.forEach(byteBuf -> {
            byteBufSize.getAndAdd(byteBuf.readableBytes());
            // publish
            persistentTopic.publishMessage(byteBuf,
                    MessagePublishContext.get(offsetFuture, persistentTopic, System.nanoTime()));
            offsetFuture.whenComplete((offset, e) -> {
                if (e == null) {
                    responseFuture.complete(new PartitionResponse(Errors.NONE, offset, -1L, -1L));
                } else {
                    log.error("publishMessages for topic partition: {} failed when write.", partitionName, e);
                    responseFuture.complete(new PartitionResponse(Errors.KAFKA_STORAGE_ERROR));
                }
                byteBuf.release();
            });
        });
        // collect metrics
        Producer producer = topicManager.getReferenceProducer(partitionName);
        producer.updateRates(numMessages, byteBufSize.get());
        producer.getTopic().incrementPublishCount(numMessages, byteBufSize.get());
    }
}
