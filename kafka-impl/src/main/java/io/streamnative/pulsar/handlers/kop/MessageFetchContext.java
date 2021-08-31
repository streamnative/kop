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

import static io.streamnative.pulsar.handlers.kop.KopServerStats.BYTES_OUT;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.ENTRIES_OUT;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.GROUP_SCOPE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.MESSAGE_OUT;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.PARTITION_SCOPE;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.TOPIC_SCOPE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.exceptions.KoPMessageMetadataNotFoundException;
import io.streamnative.pulsar.handlers.kop.format.DecodeResult;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import io.streamnative.pulsar.handlers.kop.security.auth.Resource;
import io.streamnative.pulsar.handlers.kop.security.auth.ResourceType;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import io.streamnative.pulsar.handlers.kop.utils.ZooKeeperUtils;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.NonDurableCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FetchResponse.PartitionData;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseCallbackWrapper;
import org.apache.pulsar.common.naming.TopicName;

/**
 * MessageFetchContext handling FetchRequest.
 */
@Slf4j
public final class MessageFetchContext {

    private static final Recycler<MessageFetchContext> RECYCLER = new Recycler<MessageFetchContext>() {
        protected MessageFetchContext newObject(Handle<MessageFetchContext> handle) {
            return new MessageFetchContext(handle);
        }
    };

    private final Handle<MessageFetchContext> recyclerHandle;
    private Map<TopicPartition, PartitionData<MemoryRecords>> responseData;
    private ConcurrentLinkedQueue<DecodeResult> decodeResults;
    private KafkaRequestHandler requestHandler;
    private int maxReadEntriesNum;
    private KafkaTopicManager topicManager;
    private RequestStats statsLogger;
    private TransactionCoordinator tc;
    private String clientHost;
    private FetchRequest fetchRequest;
    private RequestHeader header;
    private volatile CompletableFuture<AbstractResponse> resultFuture;
    private AtomicBoolean hasComplete;

    // recycler and get for this object
    public static MessageFetchContext get(KafkaRequestHandler requestHandler,
                                          KafkaHeaderAndRequest kafkaHeaderAndRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        MessageFetchContext context = RECYCLER.get();
        context.responseData = new ConcurrentHashMap<>();
        context.decodeResults = new ConcurrentLinkedQueue<>();
        context.requestHandler = requestHandler;
        context.maxReadEntriesNum = requestHandler.getMaxReadEntriesNum();
        context.topicManager = requestHandler.getTopicManager();
        context.statsLogger = requestHandler.requestStats;
        context.tc = requestHandler.getTransactionCoordinator();
        context.clientHost = kafkaHeaderAndRequest.getClientHost();
        context.fetchRequest = (FetchRequest) kafkaHeaderAndRequest.getRequest();
        context.header = kafkaHeaderAndRequest.getHeader();
        context.resultFuture = resultFuture;
        context.hasComplete = new AtomicBoolean(false);
        return context;
    }

    //only used for unit test
    public static MessageFetchContext getForTest(FetchRequest fetchRequest,
                                          CompletableFuture<AbstractResponse> resultFuture) {
        MessageFetchContext context = RECYCLER.get();
        context.responseData = new ConcurrentHashMap<>();
        context.decodeResults = new ConcurrentLinkedQueue<>();
        context.requestHandler = null;
        context.maxReadEntriesNum = 0;
        context.topicManager = null;
        context.statsLogger = null;
        context.tc = null;
        context.clientHost = null;
        context.fetchRequest = fetchRequest;
        context.header = null;
        context.resultFuture = resultFuture;
        context.hasComplete = new AtomicBoolean(false);
        return context;
    }

    private MessageFetchContext(Handle<MessageFetchContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }


    private void recycle() {
        responseData = null;
        decodeResults = null;
        requestHandler = null;
        maxReadEntriesNum = 0;
        topicManager = null;
        statsLogger = null;
        tc = null;
        clientHost = null;
        fetchRequest = null;
        header = null;
        resultFuture = null;
        hasComplete = null;
        recyclerHandle.recycle(this);
    }

    //only used for unit test
    public void addErrorPartitionResponseForTest(TopicPartition topicPartition, Errors errors) {
        responseData.put(topicPartition, new PartitionData<>(
                errors,
                FetchResponse.INVALID_HIGHWATERMARK,
                FetchResponse.INVALID_LAST_STABLE_OFFSET,
                FetchResponse.INVALID_LOG_START_OFFSET,
                null,
                MemoryRecords.EMPTY));
        tryComplete();
    }

    private void addErrorPartitionResponse(TopicPartition topicPartition, Errors errors) {
        responseData.put(topicPartition, new PartitionData<>(
                errors,
                FetchResponse.INVALID_HIGHWATERMARK,
                FetchResponse.INVALID_LAST_STABLE_OFFSET,
                FetchResponse.INVALID_LOG_START_OFFSET,
                null,
                MemoryRecords.EMPTY));
        tryComplete();
    }

    private void tryComplete() {
        if (resultFuture != null && responseData.size() >= fetchRequest.fetchData().size()
                && hasComplete.compareAndSet(false, true)) {
            complete();
        }
    }

    public void complete() {
        if (resultFuture == null) {
            // the context has been recycled
            return;
        }
        if (resultFuture.isCancelled()) {
            // The request was cancelled by KafkaCommandDecoder when channel is closed or this request is expired,
            // so the Netty buffers should be released.
            decodeResults.forEach(DecodeResult::release);
            return;
        }
        if (resultFuture.isDone()) {
            // It may be triggered again in DelayedProduceAndFetch
            return;
        }

        // Keep the order of TopicPartition
        final LinkedHashMap<TopicPartition, PartitionData<MemoryRecords>> orderedResponseData = new LinkedHashMap<>();
        // add the topicPartition with timeout error if it's not existed in responseData
        fetchRequest.fetchData().keySet().forEach(topicPartition -> {
            final PartitionData<MemoryRecords> partitionData = responseData.remove(topicPartition);
            if (partitionData != null) {
                orderedResponseData.put(topicPartition, partitionData);
            } else {
                orderedResponseData.put(topicPartition, new FetchResponse.PartitionData<>(
                        Errors.REQUEST_TIMED_OUT,
                        FetchResponse.INVALID_HIGHWATERMARK,
                        FetchResponse.INVALID_LAST_STABLE_OFFSET,
                        FetchResponse.INVALID_LOG_START_OFFSET,
                        null,
                        MemoryRecords.EMPTY));
            }
        });

        // Create another reference to this.decodeResults so the lambda expression will capture this local reference
        // because this.decodeResults will be reset to null after resultFuture is completed.
        final ConcurrentLinkedQueue<DecodeResult> decodeResults = this.decodeResults;
        resultFuture.complete(
                new ResponseCallbackWrapper(
                        new FetchResponse<>(
                                Errors.NONE,
                                orderedResponseData,
                                ((Integer) THROTTLE_TIME_MS.defaultValue),
                                fetchRequest.metadata().sessionId()),
                        () -> {
                            // release the batched ByteBuf if necessary
                            decodeResults.forEach(DecodeResult::release);
                        }));
        recycle();
    }

    // handle request
    public void handleFetch() {
        final boolean readCommitted =
                (tc != null && fetchRequest.isolationLevel().equals(IsolationLevel.READ_COMMITTED));

        fetchRequest.fetchData().forEach((topicPartition, partitionData) -> {
            final long startPrepareMetadataNanos = MathUtils.nowInNano();

            final String fullTopicName = KopTopic.toString(topicPartition);

            // Do authorization
            requestHandler.authorize(AclOperation.READ, Resource.of(ResourceType.TOPIC, fullTopicName))
                    .whenComplete((isAuthorized, ex) -> {
                        if (ex != null) {
                            log.error("Read topic authorize failed, topic - {}. {}",
                                    fullTopicName, ex.getMessage());
                            addErrorPartitionResponse(topicPartition, Errors.TOPIC_AUTHORIZATION_FAILED);
                            return;
                        }
                        if (!isAuthorized) {
                            addErrorPartitionResponse(topicPartition, Errors.TOPIC_AUTHORIZATION_FAILED);
                            return;
                        }
                        handlePartitionData(topicPartition,
                                partitionData,
                                fullTopicName,
                                startPrepareMetadataNanos,
                                readCommitted);
                    });
        });
    }

    private void handlePartitionData(final TopicPartition topicPartition,
                                     final FetchRequest.PartitionData partitionData,
                                     final String fullTopicName,
                                     final long startPrepareMetadataNanos,
                                     final boolean readCommitted) {
        final long offset = partitionData.fetchOffset;
        // the future that is returned by getTopicConsumerManager is always completed normally
        topicManager.getTopicConsumerManager(fullTopicName).thenAccept(tcm -> {
            if (tcm == null) {
                statsLogger.getPrepareMetadataStats().registerFailedEvent(
                        MathUtils.elapsedNanos(startPrepareMetadataNanos), TimeUnit.NANOSECONDS);
                // remove null future cache
                KafkaTopicConsumerManagerCache.getInstance().removeAndClose(fullTopicName);
                addErrorPartitionResponse(topicPartition, Errors.NOT_LEADER_FOR_PARTITION);
                return;
            }

            // handle offset out-of-range exception
            ManagedLedgerImpl managedLedger = (ManagedLedgerImpl) tcm.getManagedLedger();
            long logEndOffset = MessageIdUtils.getLogEndOffset(managedLedger);
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
                addErrorPartitionResponse(topicPartition, Errors.OFFSET_OUT_OF_RANGE);
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Fetch for {}: remove tcm to get cursor for fetch offset: {} .",
                        topicPartition, offset);
            }

            final CompletableFuture<Pair<ManagedCursor, Long>> cursorFuture =
                    tcm.removeCursorFuture(offset);
            if (cursorFuture == null) {
                // tcm is closed, just return a NONE error because the channel may be still active
                log.warn("[{}] KafkaTopicConsumerManager is closed, remove TCM of {}",
                        requestHandler.ctx, fullTopicName);
                KafkaTopicConsumerManagerCache.getInstance().removeAndClose(fullTopicName);
                addErrorPartitionResponse(topicPartition, Errors.NONE);
                return;
            }

            // cursorFuture is never completed exceptionally because ManagedLedgerImpl#asyncFindPosition
            // is never completed exceptionally.
            cursorFuture.thenAccept(cursorLongPair -> {
                if (cursorLongPair == null) {
                    log.warn("KafkaTopicConsumerManager.remove({}) return null for topic {}. "
                                    + "Fetch for topic return error.",
                            offset, topicPartition);
                    addErrorPartitionResponse(topicPartition, Errors.NOT_LEADER_FOR_PARTITION);
                    return;
                }

                final ManagedCursor cursor = cursorLongPair.getLeft();
                final AtomicLong cursorOffset = new AtomicLong(cursorLongPair.getRight());

                statsLogger.getPrepareMetadataStats().registerSuccessfulEvent(
                        MathUtils.elapsedNanos(startPrepareMetadataNanos), TimeUnit.NANOSECONDS);
                readEntries(cursor, topicPartition, cursorOffset).whenComplete((entries, throwable) -> {
                    if (throwable != null) {
                        tcm.deleteOneCursorAsync(cursorLongPair.getLeft(),
                                "cursor.readEntry fail. deleteCursor");
                        addErrorPartitionResponse(topicPartition, Errors.forException(throwable));
                        return;
                    }
                    if (entries == null) {
                        addErrorPartitionResponse(topicPartition,
                                Errors.forException(new ApiException("Cursor is null")));
                        return;
                    }

                    handleEntries(
                            entries,
                            topicPartition,
                            partitionData,
                            fullTopicName,
                            tcm,
                            cursor,
                            cursorOffset,
                            readCommitted);
                });
            });
        });
    }

    private void handleEntries(final List<Entry> entries,
                               final TopicPartition topicPartition,
                               final FetchRequest.PartitionData partitionData,
                               final String fullTopicName,
                               final KafkaTopicConsumerManager tcm,
                               final ManagedCursor cursor,
                               final AtomicLong cursorOffset,
                               final boolean readCommitted) {
        final long highWatermark = MessageIdUtils.getHighWatermark(cursor.getManagedLedger());
        // Add new offset back to TCM after entries are read successfully
        tcm.add(cursorOffset.get(), Pair.of(cursor, cursorOffset.get()));

        final long lso = (readCommitted
                ? tc.getLastStableOffset(TopicName.get(fullTopicName), highWatermark) : highWatermark);
        List<Entry> committedEntries = entries;
        if (readCommitted) {
            committedEntries = getCommittedEntries(entries, lso);
            if (log.isDebugEnabled()) {
                log.debug("Request {}: read {} entries but only {} entries are committed",
                        header, entries.size(), committedEntries.size());
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Request {}: read {} entries", header, entries.size());
            }
        }
        if (committedEntries.isEmpty()) {
            addErrorPartitionResponse(topicPartition, Errors.NONE);
            return;
        }

        // use compatible magic value by apiVersion
        short apiVersion = header.apiVersion();
        byte magic = RecordBatch.CURRENT_MAGIC_VALUE;
        if (apiVersion <= 1) {
            magic = RecordBatch.MAGIC_VALUE_V0;
        } else if (apiVersion <= 3) {
            magic = RecordBatch.MAGIC_VALUE_V1;
        }

        // get group and consumer
        final String groupName = requestHandler
                .getCurrentConnectedGroup().computeIfAbsent(clientHost, ignored -> {
                    String zkSubPath = ZooKeeperUtils.groupIdPathFormat(clientHost,
                            header.clientId());
                    String groupId = ZooKeeperUtils.getData(
                            requestHandler.getPulsarService().getZkClient(),
                            requestHandler.getGroupIdStoredPath(),
                            zkSubPath);
                    log.info("get group name from zk for current connection:{} groupId:{}",
                            clientHost, groupId);
                    return groupId;
                });
        final long startDecodingEntriesNanos = MathUtils.nowInNano();
        final DecodeResult decodeResult = requestHandler
                .getEntryFormatter().decode(entries, magic);
        requestHandler.requestStats.getFetchDecodeStats().registerSuccessfulEvent(
                MathUtils.elapsedNanos(startDecodingEntriesNanos), TimeUnit.NANOSECONDS);
        decodeResults.add(decodeResult);

        // collect consumer metrics
        updateConsumerStats(topicPartition,
                decodeResult.getRecords(),
                entries.size(),
                groupName);

        final List<FetchResponse.AbortedTransaction> abortedTransactions =
                (readCommitted ? tc.getAbortedIndexList(partitionData.fetchOffset) : null);
        responseData.put(topicPartition, new PartitionData<>(
                Errors.NONE,
                highWatermark,
                lso,
                highWatermark, // TODO: should it be changed to the logStartOffset?
                abortedTransactions,
                decodeResult.getRecords()));
        tryComplete();
    }

    private List<Entry> getCommittedEntries(List<Entry> entries, long lso) {
        List<Entry> committedEntries;
        committedEntries = new ArrayList<>();
        for (Entry entry : entries) {
            try {
                if (lso >= MessageIdUtils.peekBaseOffsetFromEntry(entry)) {
                    committedEntries.add(entry);
                } else {
                    break;
                }
            } catch (KoPMessageMetadataNotFoundException e) {
                log.error("[{}:{}] Failed to peek base offset from entry.",
                        entry.getLedgerId(), entry.getEntryId());
            }
        }
        return committedEntries;
    }

    private CompletableFuture<List<Entry>> readEntries(final ManagedCursor cursor,
                                                       final TopicPartition topicPartition,
                                                       final AtomicLong cursorOffset) {
        final OpStatsLogger messageReadStats = statsLogger.getMessageReadStats();
        // read readeEntryNum size entry.
        final long startReadingMessagesNanos = MathUtils.nowInNano();

        final CompletableFuture<List<Entry>> readFuture = new CompletableFuture<>();
        final long originalOffset = cursorOffset.get();
        cursor.asyncReadEntries(maxReadEntriesNum, new ReadEntriesCallback() {

            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                if (!entries.isEmpty()) {
                    final Entry lastEntry = entries.get(entries.size() - 1);
                    final PositionImpl currentPosition = PositionImpl.get(
                            lastEntry.getLedgerId(), lastEntry.getEntryId());

                    try {
                        final long lastOffset = MessageIdUtils.peekOffsetFromEntry(lastEntry);

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
                    } catch (Exception e) {
                        log.error("[{}] Failed to peekOffsetFromEntry from position {}",
                                topicPartition, currentPosition);
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
                log.error("Error read entry for topic: {}", KopTopic.toString(topicPartition));
                messageReadStats.registerSuccessfulEvent(
                        MathUtils.elapsedNanos(startReadingMessagesNanos), TimeUnit.NANOSECONDS);
                readFuture.completeExceptionally(exception);
            }
        }, null, PositionImpl.latest);

        return readFuture;
    }

    // commit the offset, so backlog not affect by this cursor.
    private static void commitOffset(NonDurableCursorImpl cursor, PositionImpl currentPosition) {
        cursor.asyncMarkDelete(currentPosition, new MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("Mark delete success for position: {}", currentPosition);
                }
            }

            // this is OK, since this is kind of cumulative ack, following commit will come.
            @Override
            public void markDeleteFailed(ManagedLedgerException e, Object ctx) {
                log.warn("Mark delete success for position: {} with error:",
                        currentPosition, e);
            }
        }, null);
    }

    private void updateConsumerStats(final TopicPartition topicPartition, final MemoryRecords records,
                                     int entrySize, final String groupId) {
        int numMessages = EntryFormatter.parseNumMessages(records);

        statsLogger.getStatsLogger()
                .scopeLabel(TOPIC_SCOPE, topicPartition.topic())
                .scopeLabel(PARTITION_SCOPE, String.valueOf(topicPartition.partition()))
                .scopeLabel(GROUP_SCOPE, groupId)
                .getCounter(BYTES_OUT)
                .add(records.sizeInBytes());

        statsLogger.getStatsLogger()
                .scopeLabel(TOPIC_SCOPE, topicPartition.topic())
                .scopeLabel(PARTITION_SCOPE, String.valueOf(topicPartition.partition()))
                .scopeLabel(GROUP_SCOPE, groupId)
                .getCounter(MESSAGE_OUT)
                .add(numMessages);

        statsLogger.getStatsLogger()
                .scopeLabel(TOPIC_SCOPE, topicPartition.topic())
                .scopeLabel(PARTITION_SCOPE, String.valueOf(topicPartition.partition()))
                .scopeLabel(GROUP_SCOPE, groupId)
                .getCounter(ENTRIES_OUT)
                .add(entrySize);
    }
}
