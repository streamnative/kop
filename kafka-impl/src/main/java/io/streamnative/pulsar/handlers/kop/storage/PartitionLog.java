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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.KafkaTopicManager;
import io.streamnative.pulsar.handlers.kop.MessagePublishContext;
import io.streamnative.pulsar.handlers.kop.PendingTopicFutures;
import io.streamnative.pulsar.handlers.kop.RequestStats;
import io.streamnative.pulsar.handlers.kop.format.EncodeRequest;
import io.streamnative.pulsar.handlers.kop.format.EncodeResult;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import io.streamnative.pulsar.handlers.kop.format.KafkaMixedEntryFormatter;
import io.streamnative.pulsar.handlers.kop.utils.KopLogValidator;
import io.streamnative.pulsar.handlers.kop.utils.MessageMetadataUtils;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * Analyze result.
 */
@Data
@Accessors(fluent = true)
@AllArgsConstructor
class AnalyzeResult {
    private Map<Long, ProducerAppendInfo> updatedProducers;
    private List<CompletedTxn> completedTxns;
    private Optional<BatchMetadata> maybeDuplicate;
}


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
    private final ProducerStateManager producerStateManager;

    private static final KopLogValidator.CompressionCodec DEFAULT_COMPRESSION =
            new KopLogValidator.CompressionCodec(CompressionType.NONE.name, CompressionType.NONE.id);

    @Data
    @Accessors(fluent = true)
    @AllArgsConstructor
    public static class LogAppendInfo {
        private Optional<Long> firstOffset;
        private int numMessages;
        private int shallowCount;
        private boolean isTransaction;
        private int validBytes;
        private KopLogValidator.CompressionCodec sourceCodec;
        private KopLogValidator.CompressionCodec targetCodec;
    }

    /**
     * AppendOrigin is used mark the data origin.
     */
    public enum AppendOrigin {
        Coordinator,
        Client,
        Log
    }

    public AnalyzeResult analyzeAndValidateProducerState(MemoryRecords records,
                                                         Optional<Long> firstOffset,
                                                         AppendOrigin origin) {
        Map<Long, ProducerAppendInfo> updatedProducers = Maps.newHashMap();
        List<CompletedTxn> completedTxns = Lists.newArrayList();

        for (RecordBatch batch : records.batches()) {
            if (batch.hasProducerId()) {
                if (origin.equals(AppendOrigin.Client)) {
                    Optional<ProducerStateEntry> maybeLastEntry =
                            producerStateManager.lastEntry(batch.producerId());

                    // if this is a client produce request,
                    // there will be up to 5 batches which could have been duplicated.
                    // If we find a duplicate, we return the metadata of the appended batch to the client.
                    if (maybeLastEntry.isPresent()) {
                        Optional<BatchMetadata> maybeDuplicate =
                                maybeLastEntry.get().findDuplicateBatch(batch);
                        if (maybeDuplicate.isPresent()) {
                            return new AnalyzeResult(updatedProducers, completedTxns, maybeDuplicate);
                        }
                    }
                }
                // We cache offset metadata for the start of each transaction. This allows us to
                // compute the last stable offset without relying on additional index lookups.
                Optional<CompletedTxn> maybeCompletedTxn =
                        updateProducers(batch, updatedProducers, firstOffset, origin);
                maybeCompletedTxn.ifPresent(completedTxns::add);
            }
        }
        return new AnalyzeResult(updatedProducers, completedTxns, Optional.empty());
    }

    private Optional<CompletedTxn> updateProducers(
            RecordBatch batch,
            Map<Long, ProducerAppendInfo> producers,
            Optional<Long> firstOffset,
            AppendOrigin origin) {
        Long producerId = batch.producerId();
        ProducerAppendInfo appendInfo =
                producers.computeIfAbsent(producerId, pid -> producerStateManager.prepareUpdate(producerId, origin));
        return appendInfo.append(batch, firstOffset);
    }

    public Optional<Long> firstUndecidedOffset() {
        return producerStateManager.firstUndecidedOffset();
    }

    public List<FetchResponse.AbortedTransaction> getAbortedIndexList(long fetchOffset) {
        return producerStateManager.getAbortedIndexList(fetchOffset);
    }

    /**
     * Append this message to pulsar.
     *
     * @param records The log records to append
     * @param origin  Declares the origin of to append which affects required validations
     * @param appendRecordsContext See {@link AppendRecordsContext}
     */
    public CompletableFuture<Long> appendRecords(final MemoryRecords records,
                                                 final AppendOrigin origin,
                                                 final AppendRecordsContext appendRecordsContext) {
        CompletableFuture<Long> appendFuture = new CompletableFuture<>();
        RequestStats requestStats = appendRecordsContext.getRequestStats();
        KafkaTopicManager topicManager = appendRecordsContext.getTopicManager();
        final long beforeRecordsProcess = time.nanoseconds();
        try {
            final LogAppendInfo appendInfo = analyzeAndValidateRecords(records);

            // return if we have no valid messages or if this is a duplicate of the last appended entry
            if (appendInfo.shallowCount() == 0) {
                appendFuture.complete(appendInfo.firstOffset().orElse(-1L));
                return appendFuture;
            }
            MemoryRecords validRecords = trimInvalidBytes(records, appendInfo);

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

                if (entryFormatter instanceof KafkaMixedEntryFormatter) {
                    final ManagedLedger managedLedger = persistentTopicOpt.get().getManagedLedger();
                    final long logEndOffset = MessageMetadataUtils.getLogEndOffset(managedLedger);
                    appendInfo.firstOffset(Optional.of(logEndOffset));
                }
                final EncodeRequest encodeRequest = EncodeRequest.get(validRecords, appendInfo);

                final EncodeResult encodeResult = entryFormatter.encode(encodeRequest);
                encodeRequest.recycle();

                requestStats.getProduceEncodeStats().registerSuccessfulEvent(
                        time.nanoseconds() - beforeRecordsProcess, TimeUnit.NANOSECONDS);
                appendRecordsContext.getStartSendOperationForThrottling()
                        .accept(encodeResult.getEncodedByteBuf().readableBytes());

                AnalyzeResult analyzeResult = analyzeAndValidateProducerState(
                        validRecords, appendInfo.firstOffset(), origin);
                if (analyzeResult.maybeDuplicate().isPresent()) {
                    log.error("Duplicate sequence number. topic: {}", topicPartition);
                    appendFuture.completeExceptionally(Errors.DUPLICATE_SEQUENCE_NUMBER.exception());
                    return;
                }
                publishMessages(persistentTopicOpt,
                        appendFuture,
                        appendInfo,
                        analyzeResult,
                        encodeResult,
                        appendRecordsContext);
            };

            if (topicFuture.isDone()) {
                persistentTopicConsumer.accept(topicFuture.getNow(Optional.empty()));
            } else {
                // topic is not available now
                appendRecordsContext.getPendingTopicFuturesMap()
                        .computeIfAbsent(topicPartition, ignored -> new PendingTopicFutures(requestStats))
                        .addListener(topicFuture, persistentTopicConsumer, appendFuture::completeExceptionally);
            }
        } catch (Exception exception) {
            log.error("Failed to handle produce request for {}", topicPartition, exception);
            appendFuture.completeExceptionally(exception);
        }

        return appendFuture;
    }

    private void publishMessages(final Optional<PersistentTopic> persistentTopicOpt,
                                 final CompletableFuture<Long> appendFuture,
                                 final LogAppendInfo appendInfo,
                                 final AnalyzeResult analyzeResult,
                                 final EncodeResult encodeResult,
                                 final AppendRecordsContext appendRecordsContext) {
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
        offsetFuture.whenComplete((offset, e) -> {
            appendRecordsContext.getCompleteSendOperationForThrottling().accept(byteBuf.readableBytes());
            encodeResult.recycle();
            if (e == null) {
                requestStats.getMessagePublishStats().registerSuccessfulEvent(
                        time.nanoseconds() - beforePublish, TimeUnit.NANOSECONDS);
                final long lastOffset = offset + numMessages - 1;
                analyzeResult.updatedProducers().forEach((pid, producerAppendInfo) -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Append pid: [{}], appendInfo: [{}], lastOffset: [{}]",
                                pid, producerAppendInfo, lastOffset);
                    }
                    // When we have real start offset, update current txn first offset.
                    producerAppendInfo.updateCurrentTxnFirstOffset(appendInfo.isTransaction(), offset);
                    producerStateManager.update(producerAppendInfo);
                });
                analyzeResult.completedTxns().forEach(completedTxn -> {
                    // update to real last offset
                    completedTxn.lastOffset(lastOffset);
                    long lastStableOffset = producerStateManager.lastStableOffset(completedTxn);
                    producerStateManager.updateTxnIndex(completedTxn, lastStableOffset);
                    producerStateManager.completeTxn(completedTxn);
                });
                appendFuture.complete(offset);
            } else {
                log.error("publishMessages for topic partition: {} failed when write.", fullPartitionName, e);
                requestStats.getMessagePublishStats().registerFailedEvent(
                        time.nanoseconds() - beforePublish, TimeUnit.NANOSECONDS);
                appendFuture.completeExceptionally(Errors.KAFKA_STORAGE_ERROR.exception());
            }
        });
    }

    @VisibleForTesting
    public LogAppendInfo analyzeAndValidateRecords(MemoryRecords records) {
        int numMessages = 0;
        int shallowMessageCount = 0;
        Optional<Long> firstOffset = Optional.empty();
        boolean readFirstMessage = false;
        boolean isTransaction = false;
        int validBytesCount = 0;

        KopLogValidator.CompressionCodec sourceCodec = DEFAULT_COMPRESSION;

        for (RecordBatch batch : records.batches()) {
            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 && batch.baseOffset() != 0) {
                throw new InvalidRecordException("The baseOffset of the record batch in the append to "
                        + topicPartition + " should be 0, but it is " + batch.baseOffset());
            }
            if (!readFirstMessage) {
                if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                    firstOffset = Optional.of(batch.baseOffset());
                }
                readFirstMessage = true;
            }

            int batchSize = batch.sizeInBytes();
            if (batchSize > kafkaConfig.getMaxMessageSize()) {
                throw new RecordTooLargeException(String.format("Message batch size is %s "
                                + "in append to partition %s which exceeds the maximum configured size of %s .",
                        batchSize, topicPartition, kafkaConfig.getMaxMessageSize()));
            }
            batch.ensureValid();
            shallowMessageCount += 1;
            validBytesCount += batchSize;
            numMessages += (batch.lastOffset() - batch.baseOffset() + 1);
            isTransaction = batch.isTransactional();

            if (batch.compressionType().id != CompressionType.NONE.id) {
                CompressionType compressionType = CompressionType.forId(batch.compressionType().id);
                sourceCodec = new KopLogValidator.CompressionCodec(
                        compressionType.name, compressionType.id);
            }
        }

        if (validBytesCount < 0) {
            throw new CorruptRecordException("Cannot append record batch with illegal length "
                    + validBytesCount + " to log for " + topicPartition
                    + ". A possible cause is corrupted produce request.");
        }

        KopLogValidator.CompressionCodec targetCodec =
                KopLogValidator.getTargetCodec(sourceCodec, kafkaConfig.getKafkaCompressionType());
        return new LogAppendInfo(firstOffset, numMessages, shallowMessageCount, isTransaction,
                validBytesCount, sourceCodec, targetCodec);
    }

    private MemoryRecords trimInvalidBytes(MemoryRecords records, LogAppendInfo info) {
        int validBytes = info.validBytes();
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
