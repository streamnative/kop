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

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.KafkaTopicManager;
import io.streamnative.pulsar.handlers.kop.MessagePublishContext;
import io.streamnative.pulsar.handlers.kop.PendingTopicFutures;
import io.streamnative.pulsar.handlers.kop.RequestStats;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.format.EncodeRequest;
import io.streamnative.pulsar.handlers.kop.format.EncodeResult;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import io.streamnative.pulsar.handlers.kop.format.KafkaMixedEntryFormatter;
import io.streamnative.pulsar.handlers.kop.utils.MessageMetadataUtils;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;

/**
 * An append-only log for storing messages. Mapping to Kafka Log.scala.
 */
@Slf4j
@AllArgsConstructor
public class PartitionLog {
    private final KafkaServiceConfiguration kafkaConfig;
    private final Time time;
    private final TopicPartition topicPartition;
    private final String namespacePrefix;
    private final String fullPartitionName;
    private final EntryFormatter entryFormatter;
    private final Optional<TransactionCoordinator> transactionCoordinator;

    /**
     * Append this message to pulsar.
     *
     * @param records The log records to append
     * @param version Inter-broker message protocol version
     * @param appendRecordsContext See {@link AppendRecordsContext}
     */
    public CompletableFuture<Long> appendRecords(final MemoryRecords records,
                                           final short version,
                                           final AppendRecordsContext appendRecordsContext) {
        CompletableFuture<Long> appendFuture = new CompletableFuture<>();
        RequestStats requestStats = appendRecordsContext.getRequestStats();
        KafkaTopicManager topicManager = appendRecordsContext.getTopicManager();
        final long beforeRecordsProcess = time.nanoseconds();
        try {
            MemoryRecords validRecords = validateRecords(version, fullPartitionName, records);
            validRecords.batches().forEach(batch->{
                if (batch.sizeInBytes() > kafkaConfig.getMaxMessageSize()) {
                    throw new RecordTooLargeException(String.format("Message batch size is %s "
                                    + "in append to partition %s which exceeds the maximum configured size of %s .",
                            batch.sizeInBytes(), topicPartition, kafkaConfig.getMaxMessageSize()));
                }
            });
            // Append Message into pulsar
            final CompletableFuture<Optional<PersistentTopic>> topicFuture =
                    topicManager.getTopic(fullPartitionName);
            if (topicFuture.isCompletedExceptionally()) {
                topicFuture.exceptionally(e -> {
                    appendFuture.completeExceptionally(e);
                    return Optional.empty();
                });
                return appendFuture;
            }
            if (topicFuture.isDone() && !topicFuture.getNow(Optional.empty()).isPresent()) {
                appendFuture.completeExceptionally(Errors.NOT_LEADER_FOR_PARTITION.exception());
                return appendFuture;
            }
            final Consumer<Optional<PersistentTopic>> persistentTopicConsumer = persistentTopicOpt -> {
                if (!persistentTopicOpt.isPresent()) {
                    appendFuture.completeExceptionally(Errors.NOT_LEADER_FOR_PARTITION.exception());
                    return;
                }
                // TODO: validateMessagesAndAssignOffsets here.

                final EncodeRequest encodeRequest = EncodeRequest.get(validRecords);
                if (entryFormatter instanceof KafkaMixedEntryFormatter) {
                    final ManagedLedger managedLedger = persistentTopicOpt.get().getManagedLedger();
                    final long logEndOffset = MessageMetadataUtils.getLogEndOffset(managedLedger);
                    encodeRequest.setBaseOffset(logEndOffset);
                }

                final EncodeResult encodeResult = entryFormatter.encode(encodeRequest);
                encodeRequest.recycle();
                requestStats.getProduceEncodeStats().registerSuccessfulEvent(
                        time.nanoseconds() - beforeRecordsProcess, TimeUnit.NANOSECONDS);
                appendRecordsContext.getStartSendOperationForThrottling()
                        .accept(encodeResult.getEncodedByteBuf().readableBytes());

                publishMessages(persistentTopicOpt,
                        appendFuture,
                        encodeResult,
                        topicPartition,
                        appendRecordsContext);
            };

            if (topicFuture.isDone()) {
                persistentTopicConsumer.accept(topicFuture.getNow(Optional.empty()));
            } else {
                // topic is not available now
                appendRecordsContext.getPendingTopicFuturesMap()
                        .computeIfAbsent(topicPartition, ignored ->
                                new PendingTopicFutures(requestStats))
                        .addListener(topicFuture, persistentTopicConsumer, appendFuture);
            }
        } catch (Exception exception) {
            log.error("Failed to handle produce request for {}", topicPartition, exception);
            appendFuture.completeExceptionally(exception);
        }

        return appendFuture;
    }

    private void publishMessages(final Optional<PersistentTopic> persistentTopicOpt,
                                 final CompletableFuture<Long> appendFuture,
                                 final EncodeResult encodeResult,
                                 final TopicPartition topicPartition,
                                 final AppendRecordsContext appendRecordsContext) {
        final MemoryRecords records = encodeResult.getRecords();
        final int numMessages = encodeResult.getNumMessages();
        final ByteBuf byteBuf = encodeResult.getEncodedByteBuf();
        RequestStats requestStats = appendRecordsContext.getRequestStats();
        if (!persistentTopicOpt.isPresent()) {
            encodeResult.recycle();
            // It will trigger a retry send of Kafka client
            appendFuture.completeExceptionally(Errors.NOT_LEADER_FOR_PARTITION.exception());
            return;
        }
        PersistentTopic persistentTopic = persistentTopicOpt.get();
        if (persistentTopic.isSystemTopic()) {
            encodeResult.recycle();
            log.error("Not support producing message to system topic: {}", persistentTopic);
            appendFuture.completeExceptionally(Errors.INVALID_TOPIC_EXCEPTION.exception());
            return;
        }

        appendRecordsContext.getTopicManager().registerProducerInPersistentTopic(fullPartitionName, persistentTopic);

        // collect metrics
        encodeResult.updateProducerStats(topicPartition, requestStats, namespacePrefix);

        // publish
        final CompletableFuture<Long> offsetFuture = new CompletableFuture<>();
        final long beforePublish = time.nanoseconds();
        persistentTopic.publishMessage(byteBuf,
                MessagePublishContext.get(offsetFuture, persistentTopic, numMessages, System.nanoTime()));
        final RecordBatch batch = records.batchIterator().next();
        offsetFuture.whenComplete((offset, e) -> {
            appendRecordsContext.getCompleteSendOperationForThrottling().accept(byteBuf.readableBytes());
            encodeResult.recycle();
            if (e == null) {
                if (batch.isTransactional()) {
                    transactionCoordinator.ifPresent(coordinator -> coordinator.addActivePidOffset(
                            TopicName.get(fullPartitionName), batch.producerId(), offset));
                }
                requestStats.getMessagePublishStats().registerSuccessfulEvent(
                        time.nanoseconds() - beforePublish, TimeUnit.NANOSECONDS);
                appendFuture.complete(offset);
            } else {
                log.error("publishMessages for topic partition: {} failed when write.", fullPartitionName, e);
                requestStats.getMessagePublishStats().registerFailedEvent(
                        time.nanoseconds() - beforePublish, TimeUnit.NANOSECONDS);
                appendFuture.completeExceptionally(Errors.KAFKA_STORAGE_ERROR.exception());
            }
        });
    }

    private static MemoryRecords validateRecords(short version, String fullPartitionName, MemoryRecords records) {
        if (version >= 3) {
            Iterator<MutableRecordBatch> iterator = records.batches().iterator();
            if (!iterator.hasNext()) {
                throw new InvalidRecordException("Produce requests with version " + version + " must have at least "
                        + "one record batch");
            }

            MutableRecordBatch entry = iterator.next();
            if (entry.magic() != RecordBatch.MAGIC_VALUE_V2) {
                throw new InvalidRecordException("Produce requests with version " + version + " are only allowed to "
                        + "contain record batches with magic version 2");
            }

            if (iterator.hasNext()) {
                throw new InvalidRecordException("Produce requests with version " + version + " are only allowed to "
                        + "contain exactly one record batch");
            }
        }

        int validBytesCount = 0;
        for (RecordBatch batch : records.batches()) {
            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 && batch.baseOffset() != 0) {
                throw new InvalidRecordException("The baseOffset of the record batch in the append to "
                        + fullPartitionName + " should be 0, but it is " + batch.baseOffset());
            }

            batch.ensureValid();
            validBytesCount += batch.sizeInBytes();
        }

        if (validBytesCount < 0) {
            throw new CorruptRecordException("Cannot append record batch with illegal length "
                    + validBytesCount + " to log for " + fullPartitionName
                    + ". A possible cause is corrupted produce request.");
        }

        MemoryRecords validRecords;
        if (validBytesCount == records.sizeInBytes()) {
            validRecords = records;
        } else {
            ByteBuffer validByteBuffer = records.buffer().duplicate();
            validByteBuffer.limit(validBytesCount);
            validRecords = MemoryRecords.readableRecords(validByteBuffer);
        }

        return validRecords;
    }
}
