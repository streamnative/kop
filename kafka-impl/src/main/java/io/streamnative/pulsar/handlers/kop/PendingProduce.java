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
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;

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
    private final CompletableFuture<ByteBuf> byteBufFuture;
    private CompletableFuture<Long> offsetFuture;
    private final TransactionCoordinator transactionCoordinator;
    private long pid;
    private boolean isTransactional;

    public PendingProduce(CompletableFuture<PartitionResponse> responseFuture,
                          KafkaTopicManager topicManager,
                          String partitionName,
                          EntryFormatter entryFormatter,
                          MemoryRecords memoryRecords,
                          ExecutorService executor,
                          TransactionCoordinator transactionCoordinator) {
        this.responseFuture = responseFuture;
        this.topicManager = topicManager;
        this.partitionName = partitionName;
        this.numMessages = EntryFormatter.parseNumMessages(memoryRecords);

        this.topicFuture = topicManager.getTopic(partitionName).exceptionally(e -> {
            log.error("Failed to getTopic for partition '{}': {}", partitionName, e);
            return null;
        });
        this.byteBufFuture = new CompletableFuture<>();
        this.byteBufFuture.exceptionally(e -> {
            log.error("Failed to compute ByteBuf for partition '{}': {}", partitionName, e);
            return null;
        });
        executor.execute(() -> byteBufFuture.complete(entryFormatter.encode(memoryRecords, numMessages)));
        this.offsetFuture = new CompletableFuture<>();

        RecordBatch batch = memoryRecords.batchIterator().next();
        this.transactionCoordinator = transactionCoordinator;
        this.pid = batch.producerId();
        this.isTransactional = batch.isTransactional();
    }

    public boolean ready() {
        return topicFuture.isDone()
                && !topicFuture.isCompletedExceptionally()
                && byteBufFuture.isDone()
                && !byteBufFuture.isCompletedExceptionally();
    }

    public void whenComplete(Runnable runnable) {
        CompletableFuture.allOf(topicFuture, byteBufFuture).whenComplete((ignored, e) -> {
            if (e == null) {
                runnable.run();
            } else {
                // The error logs have already been printed, so we needn't log error again.
                if (topicFuture.isCompletedExceptionally()) {
                    responseFuture.complete(new PartitionResponse(Errors.LEADER_NOT_AVAILABLE));
                } else if (byteBufFuture.isCompletedExceptionally()) {
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
        ByteBuf byteBuf;
        try {
            persistentTopic = topicFuture.get();
            byteBuf = byteBufFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            // It shouldn't fail because we've already checked ready() before.
            throw new RuntimeException(e);
        }

        if (log.isDebugEnabled()) {
            log.debug("publishMessages for topic partition: {}, records size is {}", partitionName, numMessages);
        }
        topicManager.registerProducerInPersistentTopic(partitionName, persistentTopic);
        // collect metrics
        Producer producer = topicManager.getReferenceProducer(partitionName);
        producer.updateRates(numMessages, byteBuf.readableBytes());
        producer.getTopic().incrementPublishCount(numMessages, byteBuf.readableBytes());
        // publish
        persistentTopic.publishMessage(byteBuf, MessagePublishContext.get(offsetFuture, persistentTopic,
                persistentTopic.getManagedLedger(), numMessages, System.nanoTime()));
        offsetFuture.whenComplete((offset, e) -> {
            if (e == null) {
                if (this.isTransactional) {
                    transactionCoordinator.addActivePidOffset(TopicName.get(partitionName), pid, offset);
                }
                responseFuture.complete(new PartitionResponse(Errors.NONE, offset, -1L, -1L));
            } else {
                log.error("publishMessages for topic partition: {} failed when write.", partitionName, e);
                responseFuture.complete(new PartitionResponse(Errors.KAFKA_STORAGE_ERROR));
            }
            byteBuf.release();
        });
    }
}
