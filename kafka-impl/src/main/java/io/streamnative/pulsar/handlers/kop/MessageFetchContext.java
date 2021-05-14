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

import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

import com.google.common.collect.Lists;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.format.DecodeResult;
import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import io.streamnative.pulsar.handlers.kop.utils.ZooKeeperUtils;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperation;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationKey;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationPurgatory;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.NonDurableCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FetchResponse.PartitionData;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ResponseCallbackWrapper;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.common.naming.TopicName;

/**
 * MessageFetchContext handling FetchRequest.
 */
@Slf4j
public final class MessageFetchContext {

    private KafkaRequestHandler requestHandler;

    // recycler and get for this object
    public static MessageFetchContext get(KafkaRequestHandler requestHandler) {
        MessageFetchContext context = RECYCLER.get();
        context.requestHandler = requestHandler;
        return context;
    }

    private final Handle<MessageFetchContext> recyclerHandle;

    private MessageFetchContext(Handle<MessageFetchContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<MessageFetchContext> RECYCLER = new Recycler<MessageFetchContext>() {
        protected MessageFetchContext newObject(Handle<MessageFetchContext> handle) {
            return new MessageFetchContext(handle);
        }
    };

    public void recycle() {
        requestHandler = null;
        recyclerHandle.recycle(this);
    }


    // handle request
    public CompletableFuture<AbstractResponse> handleFetch(
            CompletableFuture<AbstractResponse> fetchResponse,
            KafkaHeaderAndRequest fetchRequest,
            TransactionCoordinator transactionCoordinator,
            DelayedOperationPurgatory<DelayedOperation> fetchPurgatory) {
        final long startPreparingMetadataNanos = MathUtils.nowInNano();
        LinkedHashMap<TopicPartition, PartitionData<MemoryRecords>> responseData = new LinkedHashMap<>();

        // Map of partition and related tcm.
        Map<TopicPartition, CompletableFuture<KafkaTopicConsumerManager>> topicsAndCursor =
            ((FetchRequest) fetchRequest.getRequest())
                .fetchData().entrySet().stream()
                .map(entry -> {
                    CompletableFuture<KafkaTopicConsumerManager> consumerManager =
                        requestHandler.getTopicManager().getTopicConsumerManager(KopTopic.toString(entry.getKey()));

                    return Pair.of(
                        entry.getKey(),
                        consumerManager);
                })
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        Map<TopicPartition, Long> highWaterMarkMap = new ConcurrentHashMap<>();
        // wait to get all the cursor, then readMessages
        CompletableFuture
            .allOf(topicsAndCursor.entrySet().stream().map(Map.Entry::getValue).toArray(CompletableFuture<?>[]::new))
            .whenComplete((ignore, ex) -> {
                Map<TopicPartition, Pair<ManagedCursor, Long>> partitionCursor =
                    topicsAndCursor.entrySet().stream()
                        .map(pair -> {
                            final TopicPartition topicPartition = pair.getKey();
                            // The future is completed now
                            final KafkaTopicConsumerManager tcm = pair.getValue().getNow(null);
                            if (tcm == null) {
                                // Current broker is not the owner broker of the partition
                                responseData.put(topicPartition, new FetchResponse.PartitionData<>(
                                        Errors.NOT_LEADER_FOR_PARTITION,
                                        FetchResponse.INVALID_HIGHWATERMARK,
                                        FetchResponse.INVALID_LAST_STABLE_OFFSET,
                                        FetchResponse.INVALID_LOG_START_OFFSET,
                                        null,
                                        MemoryRecords.EMPTY));
                                // remove null future cache from consumerTopicManagers
                                KafkaTopicManager.removeKafkaTopicConsumerManager(KopTopic.toString(topicPartition));
                                // result got. this will be filtered in following filter method.
                                return null;
                            }

                            long offset = ((FetchRequest) fetchRequest.getRequest()).fetchData()
                                .get(topicPartition).fetchOffset;

                            if (log.isDebugEnabled()) {
                                log.debug("Fetch for {}: remove tcm to get cursor for fetch offset: {} .",
                                    topicPartition, offset);
                            }

                            Pair<ManagedCursor, Long> cursorLongPair = tcm.remove(offset);
                            if (cursorLongPair == null) {
                                log.warn("KafkaTopicConsumerManager.remove({}) return null for topic {}. "
                                        + "Fetch for topic return error.",
                                    offset, topicPartition);

                                responseData.put(topicPartition,
                                    new FetchResponse.PartitionData<>(
                                        Errors.NOT_LEADER_FOR_PARTITION,
                                        FetchResponse.INVALID_HIGHWATERMARK,
                                        FetchResponse.INVALID_LAST_STABLE_OFFSET,
                                        FetchResponse.INVALID_LOG_START_OFFSET,
                                        null,
                                        MemoryRecords.EMPTY));
                                // result got. this will be filtered in following filter method.
                                return null;
                            }

                            highWaterMarkMap.put(topicPartition,
                                    MessageIdUtils.getHighWatermark(cursorLongPair.getLeft().getManagedLedger()));

                            return Pair.of(topicPartition, cursorLongPair);
                        })
                        .filter(x -> x != null)
                        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

                requestHandler.requestStats.getPrepareMetadataStats().registerSuccessfulEvent(
                        MathUtils.elapsedNanos(startPreparingMetadataNanos), TimeUnit.NANOSECONDS);

                readMessages(fetchRequest, partitionCursor, fetchResponse, responseData,
                        transactionCoordinator, highWaterMarkMap, fetchPurgatory);
            });

        return fetchResponse;
    }


    // read messages from given cursors.
    // the pair Pair<ManagedCursor, Long> can be used to track the read offset?
    private void readMessages(KafkaHeaderAndRequest fetch,
                              Map<TopicPartition, Pair<ManagedCursor, Long>> cursors,
                              CompletableFuture<AbstractResponse> resultFuture,
                              LinkedHashMap<TopicPartition, PartitionData<MemoryRecords>> responseData,
                              TransactionCoordinator tc,
                              Map<TopicPartition, Long> highWaterMarkMap,
                              DelayedOperationPurgatory<DelayedOperation> fetchPurgatory) {
        AtomicInteger bytesRead = new AtomicInteger(0);
        Map<TopicPartition, List<Entry>> entryValues = new ConcurrentHashMap<>();

        final long startReadingTotalMessagesNanos = MathUtils.nowInNano();
        readMessagesInternal(fetch, cursors, bytesRead, entryValues, resultFuture, responseData, tc, highWaterMarkMap,
                startReadingTotalMessagesNanos, fetchPurgatory);
    }

    private void readMessagesInternal(KafkaHeaderAndRequest fetch,
                                      Map<TopicPartition, Pair<ManagedCursor, Long>> cursors,
                                      AtomicInteger bytesRead,
                                      Map<TopicPartition, List<Entry>> responseValues,
                                      CompletableFuture<AbstractResponse> resultFuture,
                                      LinkedHashMap<TopicPartition, PartitionData<MemoryRecords>> responseData,
                                      TransactionCoordinator tc,
                                      Map<TopicPartition, Long> highWaterMarkMap,
                                      long startReadingTotalMessagesNanos,
                                      DelayedOperationPurgatory<DelayedOperation> fetchPurgatory) {

        AtomicInteger entriesRead = new AtomicInteger(0);
        // here do the real read, and in read callback put cursor back to KafkaTopicConsumerManager.
        Map<TopicPartition, CompletableFuture<List<Entry>>> readFutures = readAllCursorOnce(cursors);
        // delay fetch
        FetchRequest fetchRequestRequest = (FetchRequest) fetch.getRequest();
        List<DecodeResult> decodeResults = new ArrayList<>();
        final AtomicInteger topicPartitionNum = new AtomicInteger(fetchRequestRequest.fetchData().entrySet().size());
        int timeoutMs = fetchRequestRequest.maxWait();
        Runnable complete = () -> {
            topicPartitionNum.set(0);
            // add the topicPartition with timeout error if it's not existed in responseData
            fetchRequestRequest.fetchData().keySet().forEach(topicPartition -> {
                if (!responseData.containsKey(topicPartition)) {
                    responseData.put(topicPartition, new FetchResponse.PartitionData<>(
                            Errors.REQUEST_TIMED_OUT,
                            FetchResponse.INVALID_HIGHWATERMARK,
                            FetchResponse.INVALID_LAST_STABLE_OFFSET,
                            FetchResponse.INVALID_LOG_START_OFFSET,
                            null,
                            MemoryRecords.EMPTY));
                }
            });
            if (resultFuture.isDone()) {
                // This future was completed by KafkaCommandDecoder because the channel is closed or the request
                // timed out, so we need to release the Netty buffers here
                decodeResults.forEach(DecodeResult::release);
            } else {
                resultFuture.complete(
                        new ResponseCallbackWrapper(
                                new FetchResponse(
                                        Errors.NONE,
                                        responseData,
                                        ((Integer) THROTTLE_TIME_MS.defaultValue),
                                        ((FetchRequest) fetch.getRequest()).metadata().sessionId()),
                                () -> {
                                    // release the batched ByteBuf if necessary
                                    decodeResults.forEach(DecodeResult::release);
                                }));
            }
            this.recycle();
        };
        CompletableFuture.allOf(readFutures.values().stream().toArray(CompletableFuture<?>[]::new))
            .whenComplete((ignore, ex) -> {

                FetchRequest request = (FetchRequest) fetch.getRequest();
                IsolationLevel isolationLevel = request.isolationLevel();

                // keep entries since all read completed.
                readFutures.entrySet().parallelStream().forEach(kafkaTopicReadEntry -> {
                    TopicPartition kafkaTopic = kafkaTopicReadEntry.getKey();
                    CompletableFuture<List<Entry>> readEntry = kafkaTopicReadEntry.getValue();
                    List<Entry> entries = null;
                    try {
                        entries = readEntry.get();
                    } catch (Exception e) {
                        // readEntry.get failed because of readEntriesFailed. return error for this partition
                        log.error("Request {}: Failed readEntry.get for topic: {}. ",
                                fetch.getHeader(), kafkaTopic, e);

                        // delete related cursor in TCM
                        requestHandler.getTopicManager()
                                .getTopicConsumerManager(KopTopic.toString(kafkaTopic))
                                .thenAccept(cm -> {
                                    // Notice, channel may be close, then TCM would be null.
                                    if (cm != null) {
                                        cm.deleteOneCursorAsync(
                                                cursors.get(kafkaTopic).getLeft(),
                                                "cursor.readEntry fail. deleteCursor");
                                    } else {
                                        // remove null future cache from consumerTopicManagers
                                        KafkaTopicManager.removeKafkaTopicConsumerManager(
                                                KopTopic.toString(kafkaTopic));
                                        log.warn("Cursor deleted while TCM close.");
                                    }
                                });

                        cursors.remove(kafkaTopic);

                        responseData.put(kafkaTopic,
                                new FetchResponse.PartitionData(
                                        Errors.NONE,
                                        FetchResponse.INVALID_HIGHWATERMARK,
                                        FetchResponse.INVALID_LAST_STABLE_OFFSET,
                                        FetchResponse.INVALID_LOG_START_OFFSET,
                                        null,
                                        MemoryRecords.EMPTY));
                    }
                    List<Entry> entryList = responseValues.computeIfAbsent(kafkaTopic, l -> Lists.newArrayList());

                    if (entries != null && !entries.isEmpty()) {
                        if (requestHandler.getKafkaConfig().isEnableTransactionCoordinator()
                                && isolationLevel.equals(IsolationLevel.READ_COMMITTED)
                                && tc != null) {
                            TopicName topicName = TopicName.get(KopTopic.toString(kafkaTopic));
                            long lso = tc.getLastStableOffset(topicName, highWaterMarkMap.get(kafkaTopic));
                            for (Entry entry : entries) {
                                if (lso >= MessageIdUtils.peekBaseOffsetFromEntry(entry)) {
                                    entryList.add(entry);
                                    entriesRead.incrementAndGet();
                                    bytesRead.addAndGet(entry.getLength());
                                } else {
                                    break;
                                }
                            }
                        } else {
                            entryList.addAll(entries);
                            entriesRead.addAndGet(entries.size());
                            bytesRead.addAndGet(entryList.stream().parallel().map(e ->
                                    e.getLength()).reduce(0, Integer::sum));
                        }

                        if (log.isDebugEnabled()) {
                            log.debug("Request {}: For topic {}, entries in list: {}.",
                                    fetch.getHeader(), kafkaTopic.toString(), entryList.size());
                        }
                    }

                });

                int maxBytes = request.maxBytes();
                int minBytes = request.minBytes();

                int allSize = bytesRead.get();

                if (log.isDebugEnabled()) {
                    log.debug("Request {}: One round read {} entries, "
                            + "allSize/maxBytes/minBytes: {}/{}/{}",
                        fetch.getHeader(), entriesRead.get(),
                        allSize, maxBytes, minBytes);
                }

                // all partitions read no entry, return earlier;
                // reach maxTime, return;
                // reach minBytes if no endTime, return;
                if ((allSize == 0 && entriesRead.get() == 0)
                    || allSize > minBytes
                    || allSize > maxBytes){
                    if (log.isDebugEnabled()) {
                        log.debug(" Request {}: Complete read {} entries with size {}",
                            fetch.getHeader(), entriesRead.get(), allSize);
                    }
                    requestHandler.requestStats.getTotalMessageReadStats().registerSuccessfulEvent(
                            MathUtils.elapsedNanos(startReadingTotalMessagesNanos), TimeUnit.NANOSECONDS);

                    responseValues.entrySet().forEach(responseEntries -> {
                        final PartitionData partitionData;
                        TopicPartition kafkaPartition = responseEntries.getKey();
                        List<Entry> entries = responseEntries.getValue();
                        // Add cursor and offset back to TCM when all the read completed.
                        Pair<ManagedCursor, Long> pair = cursors.get(kafkaPartition);
                        requestHandler.getTopicManager()
                            .getTopicConsumerManager(KopTopic.toString(kafkaPartition))
                            .thenAccept(cm -> {
                                // Notice, channel may be close, then TCM would be null.
                                if (cm != null) {
                                    cm.add(pair.getRight(), pair);
                                } else {
                                    // remove null future cache from consumerTopicManagers
                                    KafkaTopicManager.removeKafkaTopicConsumerManager(
                                            KopTopic.toString(kafkaPartition));
                                    log.warn("Cursor deleted while TCM close, failed to add cursor back to TCM.");
                                }
                            });

                        if (entries.isEmpty()) {
                            partitionData = new FetchResponse.PartitionData(
                                Errors.NONE,
                                FetchResponse.INVALID_HIGHWATERMARK,
                                FetchResponse.INVALID_LAST_STABLE_OFFSET,
                                FetchResponse.INVALID_LOG_START_OFFSET,
                                null,
                                MemoryRecords.EMPTY);
                        } else {
                            long highWatermark = highWaterMarkMap.get(kafkaPartition);

                            // use compatible magic value by apiVersion
                            short apiVersion = fetch.getHeader().apiVersion();
                            byte magic = RecordBatch.CURRENT_MAGIC_VALUE;
                            if (apiVersion <= 1) {
                                magic = RecordBatch.MAGIC_VALUE_V0;
                            } else if (apiVersion <= 3) {
                                magic = RecordBatch.MAGIC_VALUE_V1;
                            }
                            // get group and consumer
                            String clientHost = fetch.getClientHost();
                            String groupName = requestHandler
                                    .getCurrentConnectedGroup().computeIfAbsent(clientHost, ignored -> {
                                String zkSubPath = ZooKeeperUtils.groupIdPathFormat(clientHost,
                                        fetch.getHeader().clientId());
                                String groupId = ZooKeeperUtils.getData(requestHandler.getPulsarService().getZkClient(),
                                        requestHandler.getGroupIdStoredPath(), zkSubPath);
                                log.info("get group name from zk for current connection:{} groupId:{}",
                                        clientHost, groupId);
                                return groupId;
                            });
                            CompletableFuture<Consumer> consumerFuture = requestHandler.getTopicManager()
                                    .getGroupConsumers(groupName, kafkaPartition);
                            final long startDecodingEntriesNanos = MathUtils.nowInNano();
                            final DecodeResult decodeResult = requestHandler.getEntryFormatter().decode(entries, magic);
                            requestHandler.requestStats.getFetchDecodeStats().registerSuccessfulEvent(
                                    MathUtils.elapsedNanos(startDecodingEntriesNanos), TimeUnit.NANOSECONDS);
                            decodeResults.add(decodeResult);
                            // collect consumer metrics
                            EntryFormatter.updateConsumerStats(decodeResult.getRecords(), consumerFuture);

                            List<FetchResponse.AbortedTransaction> abortedTransactions;
                            if (requestHandler.getKafkaConfig().isEnableTransactionCoordinator()
                                    && isolationLevel.equals(IsolationLevel.READ_COMMITTED)
                                    && tc != null) {
                                abortedTransactions = tc.getAbortedIndexList(
                                        request.fetchData().get(kafkaPartition).fetchOffset);
                            } else {
                                abortedTransactions = null;
                            }
                            partitionData = new FetchResponse.PartitionData(
                                Errors.NONE,
                                highWatermark,
                                highWatermark,
                                highWatermark,
                                abortedTransactions,
                                decodeResult.getRecords());
                        }
                        responseData.put(kafkaPartition, partitionData);
                        // reset topicPartitionNum
                        int restTopicPartitionNum = topicPartitionNum.decrementAndGet();
                        if (restTopicPartitionNum < 0) {
                            return;
                        }
                        if (restTopicPartitionNum == 0) {
                            complete.run();
                        }
                    });
                    // delay fetch
                    if (timeoutMs <= 0) {
                        complete.run();
                    } else {
                        List<Object> delayedCreateKeys =
                                fetchRequestRequest.fetchData().keySet().stream()
                                        .map(DelayedOperationKey.TopicPartitionOperationKey::new)
                                        .collect(Collectors.toList());
                        DelayedProduceAndFetch delayedFetch = new DelayedProduceAndFetch(timeoutMs,
                                topicPartitionNum, complete);
                        fetchPurgatory.tryCompleteElseWatch(delayedFetch, delayedCreateKeys);
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Request {}: Read time or size not reach, do another round of read before return.",
                            fetch.getHeader());
                    }
                    // need do another round read
                    readMessagesInternal(fetch, cursors, bytesRead, responseValues, resultFuture, responseData,
                            tc, highWaterMarkMap, startReadingTotalMessagesNanos, fetchPurgatory);
                }
            });
    }

    private Map<TopicPartition, CompletableFuture<List<Entry>>> readAllCursorOnce(
        Map<TopicPartition, Pair<ManagedCursor, Long>> cursors) {
        Map<TopicPartition, CompletableFuture<List<Entry>>> readFutures = new ConcurrentHashMap<>();

        cursors.entrySet().parallelStream().forEach(cursorOffsetPair -> {
            ManagedCursor cursor;
            CompletableFuture<List<Entry>> readFuture = new CompletableFuture<>();
            cursor = cursorOffsetPair.getValue().getLeft();
            long currentOffset = cursorOffsetPair.getValue().getRight();
            int readeEntryNum = requestHandler.getMaxReadEntriesNum();

            // read readeEntryNum size entry.
            long startReadingMessagesNanos = MathUtils.nowInNano();
            final OpStatsLogger messageReadStats = requestHandler.requestStats.getMessageReadStats();
            cursor.asyncReadEntries(readeEntryNum,
                    new ReadEntriesCallback() {
                        @Override
                        public void readEntriesComplete(List<Entry> list, Object o) {
                            String fullPartitionName = KopTopic.toString(cursorOffsetPair.getKey());

                            if (!list.isEmpty()) {
                                StreamSupport.stream(list.spliterator(), true).forEachOrdered(entry -> {
                                    long offset = MessageIdUtils.peekOffsetFromEntry(entry);
                                    PositionImpl currentPosition = PositionImpl
                                            .get(entry.getLedgerId(), entry.getEntryId());

                                    // commit the offset, so backlog not affect by this cursor.
                                    commitOffset((NonDurableCursorImpl) cursor, currentPosition);

                                    long nextOffset = offset + 1;

                                    // put next offset in to passed in cursors map.
                                    // and add back to TCM when all read complete.
                                    cursors.put(cursorOffsetPair.getKey(), Pair.of(cursor, nextOffset));

                                    if (log.isDebugEnabled()) {
                                        log.debug("Topic {} success read entry: ledgerId: {}, entryId: {}, size: {},"
                                                        + " ConsumerManager original offset: {}, entryOffset: {} - {}, "
                                                        + "nextOffset: {}",
                                                fullPartitionName, entry.getLedgerId(), entry.getEntryId(),
                                                entry.getLength(), currentOffset, offset, currentPosition,
                                                nextOffset);
                                    }
                                });
                            }

                            readFuture.complete(list);
                            messageReadStats.registerSuccessfulEvent(
                                    MathUtils.elapsedNanos(startReadingMessagesNanos), TimeUnit.NANOSECONDS);
                        }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException e, Object o) {
                        log.error("Error read entry for topic: {}", KopTopic.toString(cursorOffsetPair.getKey()));

                        readFuture.completeExceptionally(e);
                        messageReadStats.registerFailedEvent(
                                MathUtils.elapsedNanos(startReadingMessagesNanos), TimeUnit.NANOSECONDS);
                    }
                }, null, PositionImpl.latest);

            readFutures.putIfAbsent(cursorOffsetPair.getKey(), readFuture);
        });

        return readFutures;
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

}
