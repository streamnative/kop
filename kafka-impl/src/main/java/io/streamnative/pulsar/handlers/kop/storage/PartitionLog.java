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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;

@AllArgsConstructor
@Slf4j
public class PartitionLog {
    private KafkaServiceConfiguration kafkaConfig;
    private TopicPartition topicPartition;
    private String namespacePrefix;
    private String fullPartitionName;
    private EntryFormatter entryFormatter;


    // A lock that guards all modifications to the log
    private final Object lock = new Object();

    @Data
    @AllArgsConstructor
    public static final class LogAppendInfo {
        private Optional<Long> firstOffset;
        private Long lastOffset;
        private Integer shallowCount;
        private Boolean offsetsMonotonic;
        private Long lastOffsetOfFirstBatch;
        private Integer validBytes;

        public Long numMessages() {
            return firstOffset.map(firstOffsetVal -> {
                if (firstOffsetVal >= 0 && lastOffset >= 0) {
                    return lastOffset - firstOffsetVal + 1;
                }
                return 0L;
            }).orElse(0L);
        }
    }

    public void appendRecords(final MemoryRecords records,
                              final short version,
                              final KafkaTopicManager topicManager,
                              final RequestStats requestStats,
                              final TransactionCoordinator coordinator,
                              final Consumer<Long> offsetConsumer,
                              final Consumer<Errors> errorsConsumer,
                              final Consumer<Throwable> exceptionConsumer,
                              final Consumer<Integer> startSendOperationForThrottlingConsumer,
                              final Consumer<Integer> completeSendOperationForThrottlingConsumer,
                              final Map<TopicPartition, PendingTopicFutures> pendingTopicFuturesMap) {
        append(records,
                version,
                topicManager,
                requestStats,
                coordinator,
                false,
                offsetConsumer,
                errorsConsumer,
                exceptionConsumer,
                startSendOperationForThrottlingConsumer,
                completeSendOperationForThrottlingConsumer,
                pendingTopicFuturesMap);
    }

    /**
     * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
     *
     * This method will generally be responsible for assigning offsets to the messages,
     * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
     *
     * @param records The log records to append
     * @param version Inter-broker message protocol version
     * @param ignoreRecordSize true to skip validation of record size.
     */
    private void append(final MemoryRecords records,
                        final short version,
                        final KafkaTopicManager topicManager,
                        final RequestStats requestStats,
                        final TransactionCoordinator coordinator,
                        final boolean ignoreRecordSize,
                        final Consumer<Long> offsetConsumer,
                        final Consumer<Errors> errorsConsumer,
                        final Consumer<Throwable> exceptionConsumer,
                        final Consumer<Integer> startSendOperationForThrottlingConsumer,
                        final Consumer<Integer> completeSendOperationForThrottlingConsumer,
                        final Map<TopicPartition, PendingTopicFutures> pendingTopicFuturesMap
                        ) {
        final long beforeRecordsProcess = MathUtils.nowInNano();
        try {
            final LogAppendInfo appendInfo = analyzeAndValidateRecords(records, version, topicPartition, ignoreRecordSize);

            // return if we have no valid messages or if this is a duplicate of the last appended entry
            if (appendInfo.getShallowCount() == 0) {
                return;
            }
            // trim any invalid bytes or partial messages before appending it to the on-disk log
            MemoryRecords validRecords = trimInvalidBytes(records, appendInfo);
            synchronized (lock) {
                long offset  = -1;
                appendInfo.setFirstOffset(Optional.of(offset));
                // TODO: validateMessagesAndAssignOffsets

                // Append Message into pulsar
                final CompletableFuture<Optional<PersistentTopic>> topicFuture =
                        topicManager.getTopic(fullPartitionName);
                if (topicFuture.isCompletedExceptionally()) {
                    topicFuture.exceptionally(e -> {
                        exceptionConsumer.accept(e);
                        return Optional.empty();
                    });
                    return;
                }
                if (topicFuture.isDone() && !topicFuture.getNow(Optional.empty()).isPresent()) {
                    errorsConsumer.accept(Errors.NOT_LEADER_FOR_PARTITION);
                    return;
                }
                final Consumer<Optional<PersistentTopic>> persistentTopicConsumer = persistentTopicOpt -> {
                    if (!persistentTopicOpt.isPresent()) {
                        errorsConsumer.accept(Errors.NOT_LEADER_FOR_PARTITION);
                        return;
                    }

                    final EncodeRequest encodeRequest = EncodeRequest.get(validRecords);
                    if (entryFormatter instanceof KafkaMixedEntryFormatter) {
                        final ManagedLedger managedLedger = persistentTopicOpt.get().getManagedLedger();
                        final long logEndOffset = MessageMetadataUtils.getLogEndOffset(managedLedger);
                        encodeRequest.setBaseOffset(logEndOffset);
                    }

                    final EncodeResult encodeResult = entryFormatter.encode(encodeRequest);
                    encodeRequest.recycle();
                    requestStats.getProduceEncodeStats().registerSuccessfulEvent(
                            MathUtils.elapsedNanos(beforeRecordsProcess), TimeUnit.NANOSECONDS);
                    startSendOperationForThrottlingConsumer.accept(encodeResult.getEncodedByteBuf().readableBytes());
                    if (log.isDebugEnabled()) {
                        log.debug("Produce messages for topic {} partition {}",
                                topicPartition.topic(), topicPartition.partition());
                    }

                    publishMessages(persistentTopicOpt,
                            topicManager,
                            coordinator,
                            requestStats,
                            encodeResult,
                            topicPartition,
                            offsetConsumer,
                            errorsConsumer,
                            completeSendOperationForThrottlingConsumer);
                };

                if (topicFuture.isDone()) {
                    persistentTopicConsumer.accept(topicFuture.getNow(Optional.empty()));
                } else {
                    // topic is not available now
                    pendingTopicFuturesMap
                            .computeIfAbsent(topicPartition, ignored ->
                                    new PendingTopicFutures(requestStats))
                            .addListener(topicFuture, persistentTopicConsumer, exceptionConsumer);
                }
            }
        } catch (Exception exception) {
            log.error("Failed to handle produce request for {}", topicPartition, exception);
            exceptionConsumer.accept(exception);
        }

    }

    private void publishMessages(final Optional<PersistentTopic> persistentTopicOpt,
                                 final KafkaTopicManager topicManager,
                                 final TransactionCoordinator coordinator,
                                 final RequestStats requestStats,
                                 final EncodeResult encodeResult,
                                 final TopicPartition topicPartition,
                                 final Consumer<Long> offsetConsumer,
                                 final Consumer<Errors> errorsConsumer,
                                 final Consumer<Integer> completeSendOperationForThrottlingConsumer) {
        final MemoryRecords records = encodeResult.getRecords();
        final int numMessages = encodeResult.getNumMessages();
        final ByteBuf byteBuf = encodeResult.getEncodedByteBuf();
        if (!persistentTopicOpt.isPresent()) {
            encodeResult.recycle();
            // It will trigger a retry send of Kafka client
            errorsConsumer.accept(Errors.NOT_LEADER_FOR_PARTITION);
            return;
        }
        PersistentTopic persistentTopic = persistentTopicOpt.get();
        if (persistentTopic.isSystemTopic()) {
            encodeResult.recycle();
            log.error("Not support producing message to system topic: {}", persistentTopic);
            errorsConsumer.accept(Errors.INVALID_TOPIC_EXCEPTION);
            return;
        }

        topicManager.registerProducerInPersistentTopic(fullPartitionName, persistentTopic);

        // collect metrics
        encodeResult.updateProducerStats(topicPartition, requestStats, namespacePrefix);

        // publish
        final CompletableFuture<Long> offsetFuture = new CompletableFuture<>();
        final long beforePublish = MathUtils.nowInNano();
        persistentTopic.publishMessage(byteBuf,
                MessagePublishContext.get(offsetFuture, persistentTopic, numMessages, System.nanoTime()));
        final RecordBatch batch = records.batchIterator().next();
        offsetFuture.whenComplete((offset, e) -> {
            completeSendOperationForThrottlingConsumer.accept(byteBuf.readableBytes());
            encodeResult.recycle();
            if (e == null) {
                if (batch.isTransactional()) {
                    coordinator.addActivePidOffset(TopicName.get(fullPartitionName), batch.producerId(),
                            offset);
                }
                requestStats.getMessagePublishStats().registerSuccessfulEvent(
                        MathUtils.elapsedNanos(beforePublish), TimeUnit.NANOSECONDS);
                offsetConsumer.accept(offset);
            } else {
                log.error("publishMessages for topic partition: {} failed when write.", fullPartitionName, e);
                requestStats.getMessagePublishStats().registerFailedEvent(
                        MathUtils.elapsedNanos(beforePublish), TimeUnit.NANOSECONDS);
                errorsConsumer.accept(Errors.KAFKA_STORAGE_ERROR);
            }
        });
    }

    private LogAppendInfo analyzeAndValidateRecords(MemoryRecords records,
                                                    short version,
                                                    TopicPartition topicPartition,
                                                    boolean ignoreRecordSize) {
        int shallowMessageCount = 0;
        long lastOffset = -1L;
        Optional<Long> firstOffset = Optional.empty();
        long lastOffsetOfFirstBatch = -1L;
        boolean readFirstMessage = false;
        boolean monotonic = true;

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
                        + topicPartition + " should be 0, but it is " + batch.baseOffset());
            }
            if (!readFirstMessage) {
                if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                    firstOffset = Optional.of(batch.baseOffset());
                }
                lastOffsetOfFirstBatch = batch.lastOffset();
                readFirstMessage = true;
            }
            // check that offsets are monotonically increasing
            if (lastOffset >= batch.lastOffset()){
                monotonic = false;
            }

            // update the last offset seen
            lastOffset = batch.lastOffset();

            int batchSize = batch.sizeInBytes();
            if (!ignoreRecordSize && batchSize > kafkaConfig.getMaxMessageSize()) {
                throw new RecordTooLargeException(String.format("Message batch size is %s "
                                + "in append to partition %s which exceeds the maximum configured size of %s .",
                        batchSize, topicPartition, kafkaConfig.getMaxMessageSize()));
            }

            batch.ensureValid();
            shallowMessageCount += 1;
            validBytesCount += batchSize;
        }

        if (validBytesCount < 0) {
            throw new CorruptRecordException("Cannot append record batch with illegal length "
                    + validBytesCount + " to log for " + topicPartition
                    + ". A possible cause is corrupted produce request.");
        }

        return new LogAppendInfo(
                firstOffset,
                lastOffset,
                shallowMessageCount,
                monotonic,
                lastOffsetOfFirstBatch,
                validBytesCount);
    }

    private MemoryRecords trimInvalidBytes(MemoryRecords records, LogAppendInfo info) {
        Integer validBytes = info.getValidBytes();
        if (validBytes < 0){
            throw new CorruptRecordException(String.format("Cannot append record batch with illegal length %s to "
                    + "log for %s. A possible cause is a corrupted produce request.", validBytes, topicPartition));
        } else if (validBytes == records.sizeInBytes()) {
            return records;
        } else {
            ByteBuffer validByteBuffer = records.buffer().duplicate();
            validByteBuffer.limit(validBytes);
            return MemoryRecords.readableRecords(validByteBuffer);
        }
    }
}
