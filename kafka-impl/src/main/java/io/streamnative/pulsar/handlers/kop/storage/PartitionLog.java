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
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Recycler;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.KafkaTopicConsumerManager;
import io.streamnative.pulsar.handlers.kop.KafkaTopicLookupService;
import io.streamnative.pulsar.handlers.kop.KafkaTopicManager;
import io.streamnative.pulsar.handlers.kop.MessageFetchContext;
import io.streamnative.pulsar.handlers.kop.MessagePublishContext;
import io.streamnative.pulsar.handlers.kop.PendingTopicFutures;
import io.streamnative.pulsar.handlers.kop.RequestStats;
import io.streamnative.pulsar.handlers.kop.exceptions.MetadataCorruptedException;
import io.streamnative.pulsar.handlers.kop.format.DecodeResult;
import io.streamnative.pulsar.handlers.kop.format.EncodeRequest;
import io.streamnative.pulsar.handlers.kop.format.EncodeResult;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatterFactory;
import io.streamnative.pulsar.handlers.kop.format.KafkaMixedEntryFormatter;
import io.streamnative.pulsar.handlers.kop.utils.KopLogValidator;
import io.streamnative.pulsar.handlers.kop.utils.MessageMetadataUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.NonDurableCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Analyze result.
 */
@Data
@Accessors(fluent = true)
@AllArgsConstructor
class AnalyzeResult {
    private Map<Long, ProducerAppendInfo> updatedProducers;
    private List<CompletedTxn> completedTxns;
}

/**
 * An append-only log for storing messages. Mapping to Kafka Log.scala.
 */
@Slf4j
public class PartitionLog {

    private static final String PID_PREFIX = "KOP-PID-PREFIX";

    private static final KopLogValidator.CompressionCodec DEFAULT_COMPRESSION =
            new KopLogValidator.CompressionCodec(CompressionType.NONE.name, CompressionType.NONE.id);

    private final KafkaServiceConfiguration kafkaConfig;
    private final RequestStats requestStats;
    private final Time time;
    private final TopicPartition topicPartition;
    private final String fullPartitionName;
    @Getter
    private volatile ProducerStateManager producerStateManager;

    private final List<EntryFilter> entryFilters;
    private final boolean preciseTopicPublishRateLimitingEnable;
    private final KafkaTopicLookupService kafkaTopicLookupService;

    private final ProducerStateManagerSnapshotBuffer producerStateManagerSnapshotBuffer;

    private final ExecutorService recoveryExecutor;

    @Getter
    private volatile PersistentTopic persistentTopic;

    private final CompletableFuture<PartitionLog> initFuture = new CompletableFuture<>();

    private volatile Map<String, String> topicProperties;

    private volatile EntryFormatter entryFormatter;

    private volatile String kafkaTopicUUID;

    private volatile AtomicBoolean unloaded = new AtomicBoolean();

    public PartitionLog(KafkaServiceConfiguration kafkaConfig,
                        RequestStats requestStats,
                        Time time,
                        TopicPartition topicPartition,
                        String fullPartitionName,
                        List<EntryFilter> entryFilters,
                        KafkaTopicLookupService kafkaTopicLookupService,
                        ProducerStateManagerSnapshotBuffer producerStateManagerSnapshotBuffer,
                        OrderedExecutor recoveryExecutor) {
        this.kafkaConfig = kafkaConfig;
        this.entryFilters = entryFilters;
        this.requestStats = requestStats;
        this.time = time;
        this.topicPartition = topicPartition;
        this.fullPartitionName = fullPartitionName;
        this.preciseTopicPublishRateLimitingEnable = kafkaConfig.isPreciseTopicPublishRateLimiterEnable();
        this.kafkaTopicLookupService = kafkaTopicLookupService;
        this.producerStateManagerSnapshotBuffer = producerStateManagerSnapshotBuffer;
        this.recoveryExecutor = recoveryExecutor.chooseThread(fullPartitionName);
    }

    public CompletableFuture<PartitionLog> initialise() {
        loadTopicProperties().whenComplete((___, errorLoadTopic) -> {
            if (errorLoadTopic != null) {
                initFuture.completeExceptionally(errorLoadTopic);
                return;
            }
            if (kafkaConfig.isKafkaTransactionCoordinatorEnabled()) {
                producerStateManager
                        .recover(this, recoveryExecutor)
                        .thenRun(() -> initFuture.complete(this))
                        .exceptionally(error -> {
                            initFuture.completeExceptionally(error);
                            return null;
                        });
            } else {
                initFuture.complete(this);
            }
        });
        return initFuture;
    }

    public CompletableFuture<PartitionLog> awaitInitialisation() {
        return initFuture;
    }

    public boolean isInitialised() {
        return initFuture.isDone() && !initFuture.isCompletedExceptionally();
    }

    public boolean isInitialisationFailed() {
        return initFuture.isDone() && initFuture.isCompletedExceptionally();
    }

    public void markAsUnloaded() {
        unloaded.set(true);
    }

    private CompletableFuture<Void> loadTopicProperties() {
        CompletableFuture<Optional<PersistentTopic>> persistentTopicFuture =
                kafkaTopicLookupService.getTopic(fullPartitionName, this);
        return persistentTopicFuture
                .thenCompose(this::fetchTopicProperties)
                .thenAccept(properties -> {
                    this.topicProperties = properties;
                    log.info("Topic properties for {} are {}", fullPartitionName, properties);
                    this.entryFormatter = buildEntryFormatter(topicProperties);
                    this.kafkaTopicUUID = properties.get("kafkaTopicUUID");
                    this.producerStateManager =
                            new ProducerStateManager(
                                    fullPartitionName,
                                    kafkaTopicUUID,
                                    producerStateManagerSnapshotBuffer,
                                    kafkaConfig.getKafkaTxnProducerStateTopicSnapshotIntervalSeconds(),
                                    kafkaConfig.getKafkaTxnPurgeAbortedTxnIntervalSeconds());
                });
    }

    private CompletableFuture<Map<String, String>> fetchTopicProperties(Optional<PersistentTopic> persistentTopic) {
        if (!persistentTopic.isPresent()) {
            log.info("Topic {} not loaded here", fullPartitionName);
            return FutureUtil.failedFuture(new NotLeaderOrFollowerException());
        }
        this.persistentTopic = persistentTopic.get();
        TopicName logicalName = TopicName.get(persistentTopic.get().getName());
        TopicName actualName;
        if (logicalName.isPartitioned()) {
            actualName = TopicName.getPartitionedTopicName(persistentTopic.get().getName());
        } else {
            actualName = logicalName;
        }
        return persistentTopic.get().getBrokerService()
                .fetchPartitionedTopicMetadataAsync(actualName)
                .thenApply(metadata -> {
                    if (metadata.partitions > 0) {
                        return metadata.properties;
                    } else {
                        return persistentTopic.get().getManagedLedger().getProperties();
                    }
                })
                .thenApply(map -> map != null ? map : Collections.emptyMap());
    }

    private EntryFormatter buildEntryFormatter(Map<String, String> topicProperties) {
        final String entryFormat;
        if (topicProperties != null) {
            entryFormat = topicProperties.getOrDefault("kafkaEntryFormat", kafkaConfig.getEntryFormat());
        } else {
            entryFormat = kafkaConfig.getEntryFormat();
        }
        if (log.isDebugEnabled()) {
            log.debug("entryFormat for {} is {} (topicProperties {})", fullPartitionName,
                    entryFormat, topicProperties);
        }
        return EntryFormatterFactory.create(kafkaConfig, entryFilters, entryFormat);
    }

    @Data
    @Accessors(fluent = true)
    @AllArgsConstructor
    public static class LogAppendInfo {
        private Optional<Long> firstOffset;
        private Optional<Long> producerId;
        private short producerEpoch;
        private int numMessages;
        private int shallowCount;
        private boolean isTransaction;
        private boolean isControlBatch;
        private int validBytes;
        private int firstSequence;
        private int lastSequence;
        private KopLogValidator.CompressionCodec sourceCodec;
        private KopLogValidator.CompressionCodec targetCodec;
    }

    @Data
    @ToString
    @Accessors(fluent = true)
    @AllArgsConstructor
    public static class ReadRecordsResult {

        private static final Recycler<ReadRecordsResult> RECYCLER = new Recycler<ReadRecordsResult>() {
            protected ReadRecordsResult newObject(Handle<ReadRecordsResult> handle) {
                return new ReadRecordsResult(handle);
            }
        };

        private final Recycler.Handle<ReadRecordsResult> recyclerHandle;

        private DecodeResult decodeResult;
        private List<FetchResponseData.AbortedTransaction> abortedTransactions;
        private long highWatermark;
        private long lastStableOffset;
        private Position lastPosition;
        private Errors errors;

        private PartitionLog partitionLog;

        private ReadRecordsResult(Recycler.Handle<ReadRecordsResult> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        public Errors errors() {
            return errors == null ? Errors.NONE : errors;
        }

        public static ReadRecordsResult get(DecodeResult decodeResult,
                                            List<FetchResponseData.AbortedTransaction> abortedTransactions,
                                            long highWatermark,
                                            long lastStableOffset,
                                            Position lastPosition,
                                            PartitionLog partitionLog) {
            return ReadRecordsResult.get(
                    decodeResult,
                    abortedTransactions,
                    highWatermark,
                    lastStableOffset,
                    lastPosition,
                    null,
                    partitionLog);
        }

        public static ReadRecordsResult get(DecodeResult decodeResult,
                                            List<FetchResponseData.AbortedTransaction> abortedTransactions,
                                            long highWatermark,
                                            long lastStableOffset,
                                            Position lastPosition,
                                            Errors errors,
                                            PartitionLog partitionLog) {
            ReadRecordsResult readRecordsResult = RECYCLER.get();
            readRecordsResult.decodeResult = decodeResult;
            readRecordsResult.abortedTransactions = abortedTransactions;
            readRecordsResult.highWatermark = highWatermark;
            readRecordsResult.lastStableOffset = lastStableOffset;
            readRecordsResult.lastPosition = lastPosition;
            readRecordsResult.errors = errors;
            readRecordsResult.partitionLog = partitionLog;
            return readRecordsResult;
        }

        public static ReadRecordsResult empty(long highWatermark,
                                              long lastStableOffset,
                                              Position lastPosition,
                                              PartitionLog partitionLog) {
            return ReadRecordsResult.get(
                    DecodeResult.get(MemoryRecords.EMPTY),
                    Collections.emptyList(),
                    highWatermark,
                    lastStableOffset,
                    lastPosition,
                    partitionLog);
        }

        public static ReadRecordsResult error(Errors errors, PartitionLog partitionLog) {
            return ReadRecordsResult.error(PositionImpl.EARLIEST, errors, partitionLog);
        }

        public static ReadRecordsResult error(Position position, Errors errors, PartitionLog partitionLog) {
            return ReadRecordsResult.get(null,
                    null,
                    -1,
                    -1,
                    position,
                    errors,
                    partitionLog);
        }

        public FetchResponseData.PartitionData toPartitionData() {

            // There are three cases:
            //
            // 1. errors == null :
            //        The decode result count > 0
            // 2. errors == ERROR.NONE :
            //        Get the empty result.
            // 3. errors == Others error :
            //        Get errors.
            if (errors != null) {
                return new FetchResponseData.PartitionData()
                        .setErrorCode(errors.code())
                        .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK)
                        .setLastStableOffset(FetchResponse.INVALID_LAST_STABLE_OFFSET)
                        .setLogStartOffset(FetchResponse.INVALID_LOG_START_OFFSET)
                        .setRecords(MemoryRecords.EMPTY);
            }
            return new FetchResponseData.PartitionData()
                    .setErrorCode(Errors.NONE.code())
                    .setHighWatermark(highWatermark)
                    .setLastStableOffset(lastStableOffset)
                    .setHighWatermark(highWatermark) // TODO: should it be changed to the logStartOffset?
                    .setAbortedTransactions(abortedTransactions)
                    .setRecords(decodeResult.getRecords());
        }

        public void recycle() {
            this.errors = null;
            this.lastPosition = null;
            this.lastStableOffset = -1;
            this.highWatermark = -1;
            this.abortedTransactions = null;
            this.partitionLog = null;
            if (this.decodeResult != null) {
                this.decodeResult.recycle();
                this.decodeResult = null;
            }
        }
    }
    /**
     * AppendOrigin is used mark the data origin.
     */
    public enum AppendOrigin {
        Coordinator,
        Client,
        Log
    }

    // TODO: the first and last offset only make sense here if there is only a sinlge completed txn.
    // It might make sense to refactor this method.
    public AnalyzeResult analyzeAndValidateProducerState(MemoryRecords records,
                                                         Optional<Long> firstOffset,
                                                         Long lastOffset,
                                                         AppendOrigin origin) {
        Map<Long, ProducerAppendInfo> updatedProducers = Maps.newHashMap();
        List<CompletedTxn> completedTxns = Lists.newArrayList();

        for (RecordBatch batch : records.batches()) {
            if (batch.hasProducerId()) {
                // We cache offset metadata for the start of each transaction. This allows us to
                // compute the last stable offset without relying on additional index lookups.
                Optional<CompletedTxn> maybeCompletedTxn =
                        updateProducers(batch, updatedProducers, firstOffset, origin);
                maybeCompletedTxn.ifPresent(txn -> {
                    if (lastOffset != null) {
                        txn.lastOffset(lastOffset);
                    }
                    completedTxns.add(txn);
                });
            }
        }

        return new AnalyzeResult(updatedProducers, completedTxns);
    }

    private Optional<CompletedTxn> updateProducers(
            RecordBatch batch,
            Map<Long, ProducerAppendInfo> producers,
            Optional<Long> firstOffset,
            AppendOrigin origin) {
        Long producerId = batch.producerId();
        ProducerAppendInfo appendInfo =
                producers.computeIfAbsent(producerId, pid -> producerStateManager.prepareUpdate(pid, origin));
        return appendInfo.append(batch, firstOffset);
    }

    public Optional<Long> firstUndecidedOffset() {
        return producerStateManager.firstUndecidedOffset();
    }

    public List<FetchResponseData.AbortedTransaction> getAbortedIndexList(long fetchOffset) {
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
        KafkaTopicManager topicManager = appendRecordsContext.getTopicManager();
        if (topicManager == null) {
            log.error("topicManager is null for {}???", fullPartitionName,
                    new Exception("topicManager is null for " + fullPartitionName).fillInStackTrace());
            return CompletableFuture
                    .failedFuture(new KafkaStorageException("topicManager is null for " + fullPartitionName));
        }
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

            final Consumer<PartitionLog> sequentialExecutor = ___ -> {
                final ManagedLedger managedLedger = persistentTopic.getManagedLedger();
                if (entryFormatter instanceof KafkaMixedEntryFormatter) {
                    final long logEndOffset = MessageMetadataUtils.getLogEndOffset(managedLedger);
                    appendInfo.firstOffset(Optional.of(logEndOffset));
                }
                final EncodeRequest encodeRequest = EncodeRequest.get(validRecords, appendInfo);

                requestStats.getPendingTopicLatencyStats().registerSuccessfulEvent(
                        time.nanoseconds() - beforeRecordsProcess, TimeUnit.NANOSECONDS);

                long beforeEncodingStarts = time.nanoseconds();
                final EncodeResult encodeResult = entryFormatter.encode(encodeRequest);
                encodeRequest.recycle();

                requestStats.getProduceEncodeStats().registerSuccessfulEvent(
                        time.nanoseconds() - beforeEncodingStarts, TimeUnit.NANOSECONDS);
                appendRecordsContext.getStartSendOperationForThrottling()
                        .accept(encodeResult.getEncodedByteBuf().readableBytes());

                publishMessages(
                        appendFuture,
                        appendInfo,
                        encodeResult,
                        appendRecordsContext);
            };

            appendRecordsContext.getPendingTopicFuturesMap()
                    .computeIfAbsent(topicPartition, ignored -> new PendingTopicFutures(requestStats))
                    .addListener(initFuture, sequentialExecutor, appendFuture::completeExceptionally);
        } catch (Exception exception) {
            log.error("Failed to handle produce request for {}", topicPartition, exception);
            appendFuture.completeExceptionally(exception);
        }

        return appendFuture;
    }

    public Position getLastPosition() {
        return persistentTopic.getLastPosition();
    }

    public CompletableFuture<ReadRecordsResult> readRecords(final FetchRequestData.FetchPartition partitionData,
                                                            final boolean readCommitted,
                                                            final AtomicLong limitBytes,
                                                            final int maxReadEntriesNum,
                                                            final MessageFetchContext context) {
        final long startPrepareMetadataNanos = MathUtils.nowInNano();
        final CompletableFuture<ReadRecordsResult> future = new CompletableFuture<>();
        final long offset = partitionData.fetchOffset();
        KafkaTopicManager topicManager = context.getTopicManager();
        // The future that is returned by getTopicConsumerManager is always completed normally
        topicManager.getTopicConsumerManager(fullPartitionName).thenAccept(tcm -> {
            if (tcm == null) {
                registerPrepareMetadataFailedEvent(startPrepareMetadataNanos);
                // remove null future cache
                context.getSharedState().getKafkaTopicConsumerManagerCache().removeAndCloseByTopic(fullPartitionName);
                if (log.isDebugEnabled()) {
                    log.debug("Fetch for {}: no tcm for topic {} return NOT_LEADER_FOR_PARTITION.",
                            topicPartition, fullPartitionName);
                }
                future.complete(ReadRecordsResult.error(Errors.NOT_LEADER_OR_FOLLOWER, this));
                return;
            }
            if (checkOffsetOutOfRange(tcm, offset, topicPartition, startPrepareMetadataNanos)) {
                future.complete(ReadRecordsResult.error(Errors.OFFSET_OUT_OF_RANGE, this));
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Fetch for {}: remove tcm to get cursor for fetch offset: {} .", topicPartition, offset);
            }

            final CompletableFuture<Pair<ManagedCursor, Long>> cursorFuture = tcm.removeCursorFuture(offset);

            if (cursorFuture == null) {
                // tcm is closed, just return a NONE error because the channel may be still active
                log.warn("KafkaTopicConsumerManager is closed, remove TCM of {}", fullPartitionName);
                registerPrepareMetadataFailedEvent(startPrepareMetadataNanos);
                context.getSharedState().getKafkaTopicConsumerManagerCache().removeAndCloseByTopic(fullPartitionName);
                future.complete(ReadRecordsResult.error(Errors.NONE, this));
                return;
            }
            cursorFuture.thenAccept((cursorLongPair) -> {

                if (cursorLongPair == null) {
                    log.warn("KafkaTopicConsumerManager.remove({}) return null for topic {}. "
                            + "Fetch for topic return error.", offset, topicPartition);
                    registerPrepareMetadataFailedEvent(startPrepareMetadataNanos);
                    future.complete(ReadRecordsResult.error(Errors.NOT_LEADER_OR_FOLLOWER, this));
                    return;
                }
                final ManagedCursor cursor = cursorLongPair.getLeft();
                final AtomicLong cursorOffset = new AtomicLong(cursorLongPair.getRight());

                requestStats.getPrepareMetadataStats().registerSuccessfulEvent(
                        MathUtils.elapsedNanos(startPrepareMetadataNanos), TimeUnit.NANOSECONDS);
                long adjustedMaxBytes = Math.min(partitionData.partitionMaxBytes(), limitBytes.get());
                if (readCommitted) {
                    long firstUndecidedOffset = producerStateManager.firstUndecidedOffset().orElse(-1L);
                    if (firstUndecidedOffset >= 0 && firstUndecidedOffset <= offset) {
                        long highWaterMark = MessageMetadataUtils.getHighWatermark(cursor.getManagedLedger());
                        future.complete(
                                ReadRecordsResult.empty(
                                        highWaterMark,
                                        firstUndecidedOffset,
                                        tcm.getManagedLedger().getLastConfirmedEntry(), this
                                )
                        );
                        return;
                    }
                }
                readEntries(cursor, topicPartition, cursorOffset, maxReadEntriesNum, adjustedMaxBytes,
                        fullPartitionName -> {
                            topicManager.invalidateCacheForFencedManagerLedgerOnTopic(fullPartitionName);
                        }).whenComplete((entries, throwable) -> {
                    if (throwable != null) {
                        tcm.deleteOneCursorAsync(cursorLongPair.getLeft(),
                                "cursor.readEntry fail. deleteCursor");
                        if (throwable instanceof ManagedLedgerException.CursorAlreadyClosedException
                                || throwable instanceof ManagedLedgerException.ManagedLedgerFencedException) {
                            future.complete(ReadRecordsResult.error(Errors.NOT_LEADER_OR_FOLLOWER, this));
                            return;
                        }
                        log.error("Read entry error on {}", partitionData, throwable);
                        future.complete(ReadRecordsResult.error(Errors.UNKNOWN_SERVER_ERROR, this));
                        return;
                    }
                    long readSize = entries.stream().mapToLong(Entry::getLength).sum();
                    limitBytes.addAndGet(-1 * readSize);
                    // Add new offset back to TCM after entries are read successfully
                    tcm.add(cursorOffset.get(), Pair.of(cursor, cursorOffset.get()));
                    handleEntries(future, entries, partitionData, tcm, cursor, readCommitted, context);
                });
            }).exceptionally(ex -> {
                registerPrepareMetadataFailedEvent(startPrepareMetadataNanos);
                context.getSharedState()
                        .getKafkaTopicConsumerManagerCache().removeAndCloseByTopic(fullPartitionName);
                future.complete(ReadRecordsResult.error(Errors.NOT_LEADER_OR_FOLLOWER, this));
                return null;
            });
        });

        return future;
    }

    private boolean checkOffsetOutOfRange(KafkaTopicConsumerManager tcm,
                                          long offset,
                                          TopicPartition topicPartition,
                                          long startPrepareMetadataNanos) {
        // handle offset out-of-range exception
        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) tcm.getManagedLedger();
        long logEndOffset = MessageMetadataUtils.getLogEndOffset(managedLedger);
        // TODO: Offset out-of-range checks are still incomplete
        // We only check the case of `offset > logEndOffset` and `offset < LogStartOffset`
        // is currently not handled.
        // Because we found that the operation of obtaining `LogStartOffset`
        // requires reading from disk,
        // and such a time-consuming operation is likely to harm the performance of FETCH request.
        // More discussions please refer to https://github.com/streamnative/kop/pull/531
        if (offset > logEndOffset) {
            log.error("Received request for offset {} for partition {}, "
                            + "but we only have entries less than {}.",
                    offset, topicPartition, logEndOffset);
            if (startPrepareMetadataNanos > 0) {
                registerPrepareMetadataFailedEvent(startPrepareMetadataNanos);
            }
            return true;
        }
        return false;
    }

    private void registerPrepareMetadataFailedEvent(long startPrepareMetadataNanos) {
        this.requestStats.getPrepareMetadataStats().registerFailedEvent(
                MathUtils.elapsedNanos(startPrepareMetadataNanos), TimeUnit.NANOSECONDS);
    }

    private void handleEntries(final CompletableFuture<ReadRecordsResult> future,
                               final List<Entry> entries,
                               final FetchRequestData.FetchPartition partitionData,
                               final KafkaTopicConsumerManager tcm,
                               final ManagedCursor cursor,
                               final boolean readCommitted,
                               final MessageFetchContext context) {
        final long highWatermark = MessageMetadataUtils.getHighWatermark(cursor.getManagedLedger());
        final long lso = (readCommitted
                ? this.firstUndecidedOffset().orElse(highWatermark) : highWatermark);
        final List<Entry> committedEntries = readCommitted ? getCommittedEntries(entries, lso) : entries;

        if (log.isDebugEnabled()) {
            log.debug("Read {} entries but only {} entries are committed, lso {}, highWatermark {}",
                    entries.size(), committedEntries.size(), lso, highWatermark);
        }
        if (committedEntries.isEmpty()) {
            future.complete(ReadRecordsResult.error(tcm.getManagedLedger().getLastConfirmedEntry(), Errors.NONE,
                    this));
            return;
        }

        // use compatible magic value by apiVersion
        final byte magic = getCompatibleMagic(context.getHeader().apiVersion());

        // this part is heavyweight, and we should not execute in the ManagedLedger Ordered executor thread
        final CompletableFuture<String> groupNameFuture = kafkaConfig.isKopEnableGroupLevelConsumerMetrics()
                ? context.getCurrentConnectedGroupNameAsync() : CompletableFuture.completedFuture(null);

        groupNameFuture.whenCompleteAsync((groupName, ex) -> {
            if (ex != null) {
                log.error("Get groupId failed.", ex);
                groupName = "";
            }
            final long startDecodingEntriesNanos = MathUtils.nowInNano();

            // Get the last entry position for delayed fetch.
            Position lastPosition = this.getLastPositionFromEntries(committedEntries);
            final DecodeResult decodeResult = entryFormatter.decode(committedEntries, magic);
            requestStats.getFetchDecodeStats().registerSuccessfulEvent(
                    MathUtils.elapsedNanos(startDecodingEntriesNanos), TimeUnit.NANOSECONDS);

            // collect consumer metrics
            decodeResult.updateConsumerStats(topicPartition, committedEntries.size(), groupName, requestStats);
            List<FetchResponseData.AbortedTransaction> abortedTransactions = null;
            if (readCommitted) {
                abortedTransactions = this.getAbortedIndexList(partitionData.fetchOffset());
            }
            if (log.isDebugEnabled()) {
                log.debug("Partition {} read entry completed in {} ns",
                        topicPartition, MathUtils.nowInNano() - startDecodingEntriesNanos);
            }
            future.complete(ReadRecordsResult
                    .get(decodeResult, abortedTransactions, highWatermark, lso, lastPosition, this));
        }, context.getDecodeExecutor()).exceptionally(ex -> {
            log.error("Partition {} read entry exceptionally. ", topicPartition, ex);
            future.complete(ReadRecordsResult.error(Errors.KAFKA_STORAGE_ERROR, this));
            return null;
        });
    }

    private static byte getCompatibleMagic(short apiVersion) {
        final byte magic;
        if (apiVersion <= 1) {
            magic = RecordBatch.MAGIC_VALUE_V0;
        } else if (apiVersion <= 3) {
            magic = RecordBatch.MAGIC_VALUE_V1;
        } else {
            magic = RecordBatch.CURRENT_MAGIC_VALUE;
        }
        return magic;
    }

    private Position getLastPositionFromEntries(List<Entry> entries) {
        if (entries == null || entries.isEmpty()) {
            return PositionImpl.EARLIEST;
        }
        return entries.get(entries.size() - 1).getPosition();
    }

    private List<Entry> getCommittedEntries(List<Entry> entries, long lso) {
        List<Entry> committedEntries;
        committedEntries = new ArrayList<>();
        for (Entry entry : entries) {
            try {
                if (lso > MessageMetadataUtils.peekBaseOffsetFromEntry(entry)) {
                    committedEntries.add(entry);
                } else {
                    break;
                }
            } catch (MetadataCorruptedException e) {
                log.error("[{}:{}] Failed to peek base offset from entry.",
                        entry.getLedgerId(), entry.getEntryId());
            }
        }
        // Release all the entries that are not in the result
        for (int i = committedEntries.size(); i < entries.size(); i++) {
            entries.get(i).release();
        }
        return committedEntries;
    }

    /**
     * Read Entries by cursor.
     *
     * @return CompletableFuture<List<Entry>>
     *     When the comparable future complete normally, the list of entry's will never be null.
     */
    private CompletableFuture<List<Entry>> readEntries(final ManagedCursor cursor,
                                                       final TopicPartition topicPartition,
                                                       final AtomicLong cursorOffset,
                                                       final int maxReadEntriesNum,
                                                       final long adjustedMaxBytes,
                                                       final Consumer<String> invalidateCacheOnTopic) {
        final OpStatsLogger messageReadStats = requestStats.getMessageReadStats();
        // read readeEntryNum size entry.
        final long startReadingMessagesNanos = MathUtils.nowInNano();

        final CompletableFuture<List<Entry>> readFuture = new CompletableFuture<>();
        if (adjustedMaxBytes <= 0) {
            readFuture.complete(Collections.emptyList());
            return readFuture;
        }

        final long originalOffset = cursorOffset.get();
        cursor.asyncReadEntries(maxReadEntriesNum, adjustedMaxBytes, new AsyncCallbacks.ReadEntriesCallback() {

            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                if (!entries.isEmpty()) {
                    final Entry lastEntry = entries.get(entries.size() - 1);
                    final PositionImpl currentPosition = PositionImpl.get(
                            lastEntry.getLedgerId(), lastEntry.getEntryId());

                    try {
                        final long lastOffset = MessageMetadataUtils.peekOffsetFromEntry(lastEntry);

                        // commit the offset, so backlog not affect by this cursor.
                        commitOffset((NonDurableCursorImpl) cursor, currentPosition);

                        // and add back to TCM when all read complete.
                        cursorOffset.set(lastOffset + 1);

                        if (log.isDebugEnabled()) {
                            log.debug("Topic {} success read entry: ledgerId: {}, entryId: {}, size: {},"
                                            + " ConsumerManager original offset: {}, lastEntryPosition: {}, "
                                            + "nextOffset: {}",
                                    topicPartition, lastEntry.getLedgerId(), lastEntry.getEntryId(),
                                    lastEntry.getLength(), originalOffset, currentPosition,
                                    cursorOffset.get());
                        }
                    } catch (MetadataCorruptedException e) {
                        log.error("[{}] Failed to peekOffsetFromEntry from position {}: {}",
                                topicPartition, currentPosition, e.getMessage());
                        messageReadStats.registerFailedEvent(
                                MathUtils.elapsedNanos(startReadingMessagesNanos), TimeUnit.NANOSECONDS);
                        readFuture.completeExceptionally(e);
                        return;
                    }
                }

                messageReadStats.registerSuccessfulEvent(
                        MathUtils.elapsedNanos(startReadingMessagesNanos), TimeUnit.NANOSECONDS);
                readFuture.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                log.error("Error read entry for topic: {}", fullPartitionName, exception);
                if (exception instanceof ManagedLedgerException.ManagedLedgerFencedException) {
                    invalidateCacheOnTopic.accept(fullPartitionName);
                }
                messageReadStats.registerFailedEvent(
                        MathUtils.elapsedNanos(startReadingMessagesNanos), TimeUnit.NANOSECONDS);
                readFuture.completeExceptionally(exception);
            }
        }, null, PositionImpl.LATEST);

        return readFuture;
    }

    // commit the offset, so backlog not affect by this cursor.
    private static void commitOffset(NonDurableCursorImpl cursor, PositionImpl currentPosition) {
        cursor.asyncMarkDelete(currentPosition, new AsyncCallbacks.MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("Mark delete success for position: {}", currentPosition);
                }
            }

            // this is OK, since this is kind of cumulative ack, following commit will come.
            @Override
            public void markDeleteFailed(ManagedLedgerException e, Object ctx) {
                log.warn("Mark delete failed for position: {} with error:",
                        currentPosition, e);
            }
        }, null);
    }

    private void publishMessages(final CompletableFuture<Long> appendFuture,
                                 final LogAppendInfo appendInfo,
                                 final EncodeResult encodeResult,
                                 final AppendRecordsContext appendRecordsContext) {
        checkAndRecordPublishQuota(persistentTopic, appendInfo.validBytes(),
                appendInfo.numMessages(), appendRecordsContext);
        if (persistentTopic.isSystemTopic()) {
            encodeResult.recycle();
            log.error("Not support producing message to system topic: {}", persistentTopic);
            appendFuture.completeExceptionally(Errors.INVALID_TOPIC_EXCEPTION.exception());
            return;
        }

        appendRecordsContext
                .getTopicManager()
                .registerProducerInPersistentTopic(fullPartitionName, persistentTopic)
                .ifPresent((producer) -> {
                    // collect metrics
                    encodeResult.updateProducerStats(topicPartition, requestStats, producer);
                });

        final int numMessages = encodeResult.getNumMessages();
        final ByteBuf byteBuf = encodeResult.getEncodedByteBuf();
        final int byteBufSize = byteBuf.readableBytes();
        final long beforePublish = time.nanoseconds();

        publishMessage(persistentTopic, byteBuf, appendInfo)
                .whenComplete((offset, e) -> {
            appendRecordsContext.getCompleteSendOperationForThrottling().accept(byteBufSize);

            if (e == null) {
                requestStats.getMessagePublishStats().registerSuccessfulEvent(
                        time.nanoseconds() - beforePublish, TimeUnit.NANOSECONDS);
                final long lastOffset = offset + numMessages - 1;

                AnalyzeResult analyzeResult = analyzeAndValidateProducerState(
                        encodeResult.getRecords(), Optional.of(offset), lastOffset, AppendOrigin.Client);
                updateProducerStateManager(lastOffset, analyzeResult);

                appendFuture.complete(offset);
            } else {
                log.error("publishMessages for topic partition: {} failed when write.", fullPartitionName, e);
                requestStats.getMessagePublishStats().registerFailedEvent(
                        time.nanoseconds() - beforePublish, TimeUnit.NANOSECONDS);
                appendFuture.completeExceptionally(e);
            }
            encodeResult.recycle();
        });
    }

    private void checkAndRecordPublishQuota(Topic topic, int msgSize, int numMessages,
                                            AppendRecordsContext appendRecordsContext) {
        final boolean isPublishRateExceeded;
        if (preciseTopicPublishRateLimitingEnable) {
            boolean isPreciseTopicPublishRateExceeded =
                    topic.isTopicPublishRateExceeded(numMessages, msgSize);
            if (isPreciseTopicPublishRateExceeded) {
                topic.disableCnxAutoRead();
                return;
            }
            isPublishRateExceeded = topic.isBrokerPublishRateExceeded();
        } else {
            if (topic.isResourceGroupRateLimitingEnabled()) {
                final boolean resourceGroupPublishRateExceeded =
                        topic.isResourceGroupPublishRateExceeded(numMessages, msgSize);
                if (resourceGroupPublishRateExceeded) {
                    topic.disableCnxAutoRead();
                    return;
                }
            }
            isPublishRateExceeded = topic.isPublishRateExceeded();
        }

        if (isPublishRateExceeded) {
            ChannelHandlerContext ctx = appendRecordsContext.getCtx();
            if (ctx != null && ctx.channel().config().isAutoRead()) {
                ctx.channel().config().setAutoRead(false);
            }
        }
    }

    /**
     * Publish message to bookkeeper.
     * When the message is control message, then it will not do the message deduplication.
     *
     * @param persistentTopic The persistentTopic, use to publish message and check message deduplication.
     * @param byteBuf Message byteBuf
     * @param appendInfo Pre-analyzed recode info, we can get sequence, message num ...
     * @return offset
     */
    private CompletableFuture<Long> publishMessage(final PersistentTopic persistentTopic,
                                                   final ByteBuf byteBuf,
                                                   final LogAppendInfo appendInfo) {
        final CompletableFuture<Long> offsetFuture = new CompletableFuture<>();

        // This producerName is only used to check the message deduplication.
        // Kafka will reuse pid when transactionId is the same but will increase the producerEpoch.
        // So we need to ensure the producerName is not the same.
        String producerName = new StringJoiner("-")
                .add(PID_PREFIX)
                .add(String.valueOf(appendInfo.producerId().orElse(-1L)))
                .add(String.valueOf(appendInfo.producerEpoch())).toString();


        persistentTopic.publishMessage(byteBuf,
                MessagePublishContext.get(
                        offsetFuture,
                        persistentTopic,
                        producerName,
                        appendInfo.producerId().isPresent() && !appendInfo.isControlBatch(),
                        appendInfo.firstSequence(),
                        appendInfo.lastSequence(),
                        appendInfo.numMessages(),
                        time.nanoseconds()));
        return offsetFuture;
    }

    @VisibleForTesting
    public LogAppendInfo analyzeAndValidateRecords(MemoryRecords records) {
        int numMessages = 0;
        int shallowMessageCount = 0;
        Optional<Long> firstOffset = Optional.empty();
        boolean readFirstMessage = false;
        boolean isTransaction = false;
        boolean isControlBatch = false;
        int validBytesCount = 0;
        int firstSequence = Integer.MAX_VALUE;
        int lastSequence = -1;
        Optional<Long> producerId = Optional.empty();
        short producerEpoch = -1;
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
            isControlBatch = batch.isControlBatch();

            // We assume batches producerId are same.
            if (batch.hasProducerId()) {
                producerId = Optional.of(batch.producerId());
                producerEpoch = batch.producerEpoch();
            }

            if (batch.compressionType().id != CompressionType.NONE.id) {
                CompressionType compressionType = CompressionType.forId(batch.compressionType().id);
                sourceCodec = new KopLogValidator.CompressionCodec(
                        compressionType.name, compressionType.id);
            }
            if (firstSequence > batch.baseSequence()) {
                firstSequence = batch.baseSequence();
            }
            if (lastSequence < batch.lastSequence()) {
                lastSequence = batch.lastSequence();
            }
        }

        if (validBytesCount < 0) {
            throw new CorruptRecordException("Cannot append record batch with illegal length "
                    + validBytesCount + " to log for " + topicPartition
                    + ". A possible cause is corrupted produce request.");
        }

        KopLogValidator.CompressionCodec targetCodec =
                KopLogValidator.getTargetCodec(sourceCodec, kafkaConfig.getKafkaCompressionType());
        return new LogAppendInfo(firstOffset, producerId, producerEpoch, numMessages, shallowMessageCount,
                isTransaction, isControlBatch, validBytesCount, firstSequence, lastSequence, sourceCodec, targetCodec);
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

    /**
     * Remove all the AbortedTxn that are no more referred by existing data on the topic.
     * @return
     */
    public CompletableFuture<?> updatePurgeAbortedTxnsOffset() {
        if (!kafkaConfig.isKafkaTransactionCoordinatorEnabled()) {
            // no need to scan the topic, because transactions are disabled
            return CompletableFuture.completedFuture(null);
        }
        if (!producerStateManager.hasSomeAbortedTransactions()) {
            // nothing to do
            return CompletableFuture.completedFuture(null);
        }
        if (unloaded.get()) {
            // nothing to do
            return CompletableFuture.completedFuture(null);
        }
        return fetchOldestAvailableIndexFromTopic()
                .thenAccept(offset ->
                    producerStateManager.updateAbortedTxnsPurgeOffset(offset));

    }

    public CompletableFuture<Long> fetchOldestAvailableIndexFromTopic() {
        if (unloaded.get()) {
            return FutureUtil.failedFuture(new NotLeaderOrFollowerException());
        }

        final CompletableFuture<Long> future = new CompletableFuture<>();

        // The future that is returned by getTopicConsumerManager is always completed normally
        KafkaTopicConsumerManager tcm = new KafkaTopicConsumerManager("purge-aborted-tx",
                true, persistentTopic);
        future.whenComplete((___, error) -> {
            // release resources in any case
            try {
                tcm.close();
            } catch (Exception err) {
                log.error("Cannot safely close the temporary KafkaTopicConsumerManager for {}",
                        fullPartitionName, err);
            }
        });

        ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
        long numberOfEntries = managedLedger.getNumberOfEntries();
        if (numberOfEntries == 0) {
            long currentOffset = MessageMetadataUtils.getCurrentOffset(managedLedger);
            log.info("First offset for topic {} is {} as the topic is empty (numberOfEntries=0)",
                    fullPartitionName, currentOffset);
            future.complete(currentOffset);

            return future;
        }

        // this is a DUMMY entry with -1
        PositionImpl firstPosition = managedLedger.getFirstPosition();
        // look for the first entry with data
        PositionImpl nextValidPosition = managedLedger.getNextValidPosition(firstPosition);

        fetchOldestAvailableIndexFromTopicReadNext(future, managedLedger, nextValidPosition);

        return future;

    }

    private void fetchOldestAvailableIndexFromTopicReadNext(CompletableFuture<Long> future,
                                                            ManagedLedgerImpl managedLedger, PositionImpl position) {
        managedLedger.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                try {
                    long startOffset = MessageMetadataUtils.peekBaseOffsetFromEntry(entry);
                    log.info("First offset for topic {} is {} - position {}", fullPartitionName,
                            startOffset, entry.getPosition());
                    future.complete(startOffset);
                } catch (MetadataCorruptedException.NoBrokerEntryMetadata noBrokerEntryMetadata) {
                    long currentOffset = MessageMetadataUtils.getCurrentOffset(managedLedger);
                    log.info("Legacy entry for topic {} - position {} - returning current offset {}",
                            fullPartitionName,
                            entry.getPosition(),
                            currentOffset);
                    future.complete(currentOffset);
                } catch (Exception err) {
                    future.completeExceptionally(err);
                } finally {
                    entry.release();
                }
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
    }

    @VisibleForTesting
    public CompletableFuture<?> takeProducerSnapshot() {
        return initFuture.thenCompose((___)  -> {
            // snapshot can be taken only on the same thread that is used for writes
            ManagedLedgerImpl ml = (ManagedLedgerImpl) getPersistentTopic().getManagedLedger();
            Executor executorService = ml.getExecutor();
            return this
                    .getProducerStateManager()
                    .takeSnapshot(executorService);
        });
    }

    @VisibleForTesting
    public CompletableFuture<Long> forcePurgeAbortTx() {
        return initFuture.thenCompose((___)  -> {
            // purge can be taken only on the same thread that is used for writes
            ManagedLedgerImpl ml = (ManagedLedgerImpl) getPersistentTopic().getManagedLedger();
            ExecutorService executorService = ml.getScheduledExecutor().chooseThread(ml.getName());

            return updatePurgeAbortedTxnsOffset()
                    .thenApplyAsync((____) -> {
                        return getProducerStateManager().executePurgeAbortedTx();
                    }, executorService);
        });
    }

    public CompletableFuture<Long> recoverTxEntries(
            long offset,
            Executor executor) {
        if (!kafkaConfig.isKafkaTransactionCoordinatorEnabled()
                || !MessageMetadataUtils.isInterceptorConfigured(persistentTopic.getManagedLedger())) {
            // no need to scan the topic, because transactions are disabled
            return CompletableFuture.completedFuture(Long.valueOf(0));
        }
        return fetchOldestAvailableIndexFromTopic().thenCompose((minOffset -> {
            log.info("start recoverTxEntries for {} at offset {} minOffset {}",
                    fullPartitionName, offset, minOffset);
            final CompletableFuture<Long> future = new CompletableFuture<>();

            // The future that is returned by getTopicConsumerManager is always completed normally
            KafkaTopicConsumerManager tcm = new KafkaTopicConsumerManager("recover-tx",
                    true, persistentTopic);
            future.whenComplete((___, error) -> {
                // release resources in any case
                try {
                    tcm.close();
                } catch (Exception err) {
                    log.error("Cannot safely close the temporary KafkaTopicConsumerManager for {}",
                            fullPartitionName, err);
                }
            });

            final long offsetToStart;
            if (checkOffsetOutOfRange(tcm, offset, topicPartition, -1)) {
                offsetToStart = 0;
                log.info("recoverTxEntries for {}: offset {} is out-of-range, "
                                + "maybe the topic has been deleted/recreated, "
                                + "starting recovery from {}",
                        topicPartition, offset, offsetToStart);
            } else {
                offsetToStart = offset;
            }

            producerStateManager.handleMissingDataBeforeRecovery(minOffset, offset);

            if (log.isDebugEnabled()) {
                log.debug("recoverTxEntries for {}: remove tcm to get cursor for fetch offset: {} .",
                        topicPartition, offsetToStart);
            }


            final CompletableFuture<Pair<ManagedCursor, Long>> cursorFuture = tcm.removeCursorFuture(offsetToStart);

            if (cursorFuture == null) {
                // tcm is closed, just return a NONE error because the channel may be still active
                log.warn("KafkaTopicConsumerManager is closed, remove TCM of {}", fullPartitionName);
                future.completeExceptionally(new NotLeaderOrFollowerException());
                return future;
            }

            cursorFuture.thenAccept((cursorLongPair) -> {

                if (cursorLongPair == null) {
                    log.warn("KafkaTopicConsumerManager.remove({}) return null for topic {}. "
                            + "Fetch for topic return error.", offsetToStart, topicPartition);
                    future.completeExceptionally(new NotLeaderOrFollowerException());
                    return;
                }
                final ManagedCursor cursor = cursorLongPair.getLeft();
                final AtomicLong cursorOffset = new AtomicLong(cursorLongPair.getRight());

                AtomicLong entryCounter = new AtomicLong();
                readNextEntriesForRecovery(cursor, cursorOffset, tcm, entryCounter,
                        future, executor);

            }).exceptionally(ex -> {
                future.completeExceptionally(new NotLeaderOrFollowerException());
                return null;
            });
            return future;
        }));
    }

    private void readNextEntriesForRecovery(ManagedCursor cursor, AtomicLong cursorOffset,
                                            KafkaTopicConsumerManager tcm,
                                            AtomicLong entryCounter,
                                            CompletableFuture<Long> future, Executor executor) {
        if (log.isDebugEnabled()) {
            log.debug("readNextEntriesForRecovery {} cursorOffset {}", fullPartitionName, cursorOffset);
        }
        int maxReadEntriesNum = 200;
        long adjustedMaxBytes = Long.MAX_VALUE;
        readEntries(cursor, topicPartition, cursorOffset, maxReadEntriesNum, adjustedMaxBytes,
                (partitionName) -> {})
                .whenCompleteAsync((entries, throwable) -> {
                    if (throwable != null) {
                        log.error("Read entry error on {}", fullPartitionName, throwable);
                        tcm.deleteOneCursorAsync(cursor,
                                "cursor.readEntry fail. deleteCursor");
                        if (throwable instanceof ManagedLedgerException.CursorAlreadyClosedException
                                || throwable instanceof ManagedLedgerException.ManagedLedgerFencedException) {
                            future.completeExceptionally(new NotLeaderOrFollowerException());
                            return;
                        }
                        future.completeExceptionally(new UnknownServerException(throwable));
                        return;
                    }

                    // Add new offset back to TCM after entries are read successfully
                    tcm.add(cursorOffset.get(), Pair.of(cursor, cursorOffset.get()));

                    if (entries.isEmpty()) {
                        if (log.isDebugEnabled()) {
                            log.debug("No more entries to recover for {}", fullPartitionName);
                        }
                        future.completeAsync(() -> entryCounter.get(), executor);
                        return;
                    }

                    CompletableFuture<DecodeResult> decodedEntries = new CompletableFuture<>();
                    decodeEntriesForRecovery(decodedEntries, entries);

                    decodedEntries.thenAccept((decodeResult) -> {
                        try {
                            MemoryRecords records = decodeResult.getRecords();
                            // When we retrieve many entries, this firstOffset's baseOffset is not necessarily
                            // the base offset for all records.
                            Optional<Long> firstOffset = Optional
                                    .ofNullable(records.firstBatch())
                                    .map(batch -> batch.baseOffset());

                            long[] lastOffSetHolder = {-1L};
                            records.batches().forEach(batch -> {
                                batch.forEach(record -> {
                                    if (lastOffSetHolder[0] < record.offset()) {
                                        lastOffSetHolder[0] = record.offset();
                                    }
                                    entryCounter.incrementAndGet();
                                });
                            });
                            long lastOffset = lastOffSetHolder[0];

                            if (log.isDebugEnabled()) {
                                log.debug("Read some entries while recovering {} firstOffSet {} lastOffset {}",
                                        fullPartitionName,
                                        firstOffset.orElse(null), lastOffset);
                            }

                            // Get the relevant offsets from each record
                            AnalyzeResult analyzeResult = analyzeAndValidateProducerState(records,
                                    Optional.empty(), null, AppendOrigin.Log);

                            updateProducerStateManager(lastOffset, analyzeResult);
                            if (log.isDebugEnabled()) {
                                log.debug("Completed recovery of batch {} {}", analyzeResult, fullPartitionName);
                            }
                        } finally {
                            decodeResult.recycle();
                        }
                        readNextEntriesForRecovery(cursor, cursorOffset, tcm, entryCounter, future, executor);

                    }).exceptionally(error -> {
                        log.error("Bad error while recovering {}", fullPartitionName, error);
                        future.completeExceptionally(error);
                        return null;
                    });
                }, executor);
    }

    private void updateProducerStateManager(long lastOffset, AnalyzeResult analyzeResult) {
        analyzeResult.updatedProducers().forEach((pid, producerAppendInfo) -> {
            if (log.isDebugEnabled()) {
                log.debug("Append pid: [{}], appendInfo: [{}], lastOffset: [{}]",
                        pid, producerAppendInfo, lastOffset);
            }
            producerStateManager.update(producerAppendInfo);
        });
        analyzeResult.completedTxns().forEach(completedTxn -> {
            long lastStableOffset = producerStateManager.lastStableOffset(completedTxn);
            producerStateManager.updateTxnIndex(completedTxn, lastStableOffset);
            producerStateManager.completeTxn(completedTxn);
        });
        producerStateManager.updateMapEndOffset(lastOffset);

        // do system clean up stuff in this thread
        producerStateManager.maybeTakeSnapshot(recoveryExecutor);
        producerStateManager.maybePurgeAbortedTx();
    }

    private void decodeEntriesForRecovery(final CompletableFuture<DecodeResult> future,
                                          final List<Entry> entries) {

        if (log.isDebugEnabled()) {
            log.debug("Read {} entries", entries.size());
        }
        final byte magic = RecordBatch.CURRENT_MAGIC_VALUE;
        final long startDecodingEntriesNanos = MathUtils.nowInNano();
        try {
            DecodeResult decodeResult = entryFormatter.decode(entries, magic);
            requestStats.getFetchDecodeStats().registerSuccessfulEvent(
                    MathUtils.elapsedNanos(startDecodingEntriesNanos), TimeUnit.NANOSECONDS);
            future.complete(decodeResult);
        } catch (Exception error) {
            future.completeExceptionally(error);
        }
    }

    public boolean isUnloaded() {
        return unloaded.get();
    }
}
