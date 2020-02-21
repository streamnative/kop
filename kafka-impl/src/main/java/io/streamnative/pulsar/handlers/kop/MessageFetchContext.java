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

import static io.streamnative.pulsar.handlers.kop.utils.MessageRecordUtils.entriesToRecords;
import static io.streamnative.pulsar.handlers.kop.utils.TopicNameUtils.pulsarTopicName;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;

import com.google.common.collect.Lists;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.MarkDeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.NonDurableCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.FetchResponse.PartitionData;
import org.apache.pulsar.common.naming.TopicName;

/**
 * MessageFetchContext handling FetchRequest .
 */
@Slf4j
public final class MessageFetchContext {

    private KafkaRequestHandler requestHandler;
    private KafkaHeaderAndRequest fetchRequest;

    // recycler and get for this object
    public static MessageFetchContext get(KafkaRequestHandler requestHandler,
                                          KafkaHeaderAndRequest fetchRequest) {
        MessageFetchContext context = RECYCLER.get();
        context.requestHandler = requestHandler;
        context.fetchRequest = fetchRequest;
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
        fetchRequest = null;
        recyclerHandle.recycle(this);
    }


    // handle request
    public CompletableFuture<AbstractResponse> handleFetch(CompletableFuture<AbstractResponse> fetchResponse) {
        LinkedHashMap<TopicPartition, PartitionData<MemoryRecords>> responseData = new LinkedHashMap<>();

        // Map of partition and related tcm.
        Map<TopicPartition, CompletableFuture<KafkaTopicConsumerManager>> topicsAndCursor =
            ((FetchRequest) fetchRequest.getRequest())
                .fetchData().entrySet().stream()
                .map(entry -> {
                    TopicName topicName = pulsarTopicName(entry.getKey(), requestHandler.getNamespace());

                    CompletableFuture<KafkaTopicConsumerManager> consumerManager =
                        requestHandler.getTopicManager().getTopicConsumerManager(topicName.toString());

                    return Pair.of(
                        entry.getKey(),
                        consumerManager);
                })
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

        // wait to get all the cursor, then readMessages
        CompletableFuture
            .allOf(topicsAndCursor.entrySet().stream().map(Map.Entry::getValue).toArray(CompletableFuture<?>[]::new))
            .whenComplete((ignore, ex) -> {
                Map<TopicPartition, Pair<ManagedCursor, Long>> partitionCursor =
                    topicsAndCursor.entrySet().stream()
                        .map(pair -> {
                            KafkaTopicConsumerManager tcm;
                            try {
                                // all future completed now.
                                tcm = pair.getValue().get();
                                if (tcm == null) {
                                    throw new NullPointerException("topic not owned, and return null TCM in fetch.");
                                }
                            } catch (Exception e) {
                                log.warn("Error for get KafkaTopicConsumerManager.", e);

                                responseData.put(pair.getKey(),
                                    new FetchResponse.PartitionData(
                                        Errors.NOT_LEADER_FOR_PARTITION,
                                        FetchResponse.INVALID_HIGHWATERMARK,
                                        FetchResponse.INVALID_LAST_STABLE_OFFSET,
                                        FetchResponse.INVALID_LOG_START_OFFSET,
                                        null,
                                        MemoryRecords.EMPTY));
                                // result got. this will be filtered in following filter method.
                                return null;
                            }

                            long offset = ((FetchRequest) fetchRequest.getRequest()).fetchData()
                                .get(pair.getKey()).fetchOffset;

                            if (log.isDebugEnabled()) {
                                log.debug("Fetch for {}: remove tcm to get cursor for fetch offset: {} - {}.",
                                    pair.getKey(), offset, MessageIdUtils.getPosition(offset));
                            }

                            Pair<ManagedCursor, Long> cursorLongPair = tcm.remove(offset);
                            if (cursorLongPair == null) {
                                log.warn("KafkaTopicConsumerManager.remove({}) return null for topic {}. "
                                        + "Fetch for topic return error.",
                                    offset, pair.getKey());

                                responseData.put(pair.getKey(),
                                    new FetchResponse.PartitionData(
                                        Errors.NOT_LEADER_FOR_PARTITION,
                                        FetchResponse.INVALID_HIGHWATERMARK,
                                        FetchResponse.INVALID_LAST_STABLE_OFFSET,
                                        FetchResponse.INVALID_LOG_START_OFFSET,
                                        null,
                                        MemoryRecords.EMPTY));
                                // result got. this will be filtered in following filter method.
                                return null;
                            }

                            return Pair.of(pair.getKey(), cursorLongPair);
                        })
                        .filter(x -> x != null)
                        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

                readMessages(fetchRequest, partitionCursor, fetchResponse, responseData);
            });

        return fetchResponse;
    }


    // read messages from given cursors.
    // the pair Pair<ManagedCursor, Long> can be used to track the read offset?
    private void readMessages(KafkaHeaderAndRequest fetch,
                              Map<TopicPartition, Pair<ManagedCursor, Long>> cursors,
                              CompletableFuture<AbstractResponse> resultFuture,
                              LinkedHashMap<TopicPartition, PartitionData<MemoryRecords>> responseData) {
        AtomicInteger bytesRead = new AtomicInteger(0);
        Map<TopicPartition, List<Entry>> entryValues = new ConcurrentHashMap<>();

        readMessagesInternal(fetch, cursors, bytesRead, entryValues, resultFuture, responseData);
    }

    private void readMessagesInternal(KafkaHeaderAndRequest fetch,
                                      Map<TopicPartition, Pair<ManagedCursor, Long>> cursors,
                                      AtomicInteger bytesRead,
                                      Map<TopicPartition, List<Entry>> responseValues,
                                      CompletableFuture<AbstractResponse> resultFuture,
                                      LinkedHashMap<TopicPartition, PartitionData<MemoryRecords>> responseData) {
        AtomicInteger entriesRead = new AtomicInteger(0);
        // here do the real read, and in read callback put cursor back to KafkaTopicConsumerManager.
        Map<TopicPartition, CompletableFuture<Entry>> readFutures = readAllCursorOnce(cursors);
        CompletableFuture.allOf(readFutures.values().stream().toArray(CompletableFuture<?>[]::new))
            .whenComplete((ignore, ex) -> {
                // keep entries since all read completed. currently only read 1 entry each time.
                readFutures.forEach((kafkaTopic, readEntry) -> {
                    try {
                        Entry entry = readEntry.get();
                        List<Entry> entryList = responseValues.computeIfAbsent(kafkaTopic, l -> Lists.newArrayList());

                        if (entry != null) {
                            entryList.add(entry);
                            entriesRead.incrementAndGet();
                            bytesRead.addAndGet(entry.getLength());
                            if (log.isDebugEnabled()) {
                                log.debug("Request {}: For topic {}, entries in list: {}. add new entry {}:{}",
                                    fetch.getHeader(), kafkaTopic.toString(), entryList.size(),
                                    entry.getLedgerId(), entry.getEntryId());
                            }
                        }
                    } catch (Exception e) {
                        // readEntry.get failed because of readEntriesFailed. return error for this partition
                        log.error("Request {}: Failed readEntry.get for topic: {}. ",
                            fetch.getHeader(), kafkaTopic, e);

                        // delete related cursor in TCM
                        TopicName pulsarTopicName = pulsarTopicName(kafkaTopic, requestHandler.getNamespace());
                        requestHandler.getTopicManager()
                                .getTopicConsumerManager(pulsarTopicName.toString())
                                .thenAccept(cm ->
                                    cm.deleteOneCursorAsync(
                                        cursors.get(kafkaTopic).getLeft(),
                                        "cursor.readEntry fail. deleteCursor"));

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
                });

                FetchRequest request = (FetchRequest) fetch.getRequest();
                int maxBytes = request.maxBytes();
                int minBytes = request.minBytes();
                int waitTime = request.maxWait(); // in ms
                // if endTime <= 0, then no time wait, wait for minBytes.
                long endTime = waitTime > 0 ? System.currentTimeMillis() + waitTime : waitTime;

                int allSize = bytesRead.get();

                if (log.isDebugEnabled()) {
                    log.debug("Request {}: One round read {} entries, "
                            + "allSize/maxBytes/minBytes/endTime: {}/{}/{}/{}",
                        fetch.getHeader(), entriesRead.get(),
                        allSize, maxBytes, minBytes, new Date(endTime));
                }

                // all partitions read no entry, return earlier;
                // reach maxTime, return;
                // reach minBytes if no endTime, return;
                if ((allSize == 0 && entriesRead.get() == 0)
                    || (endTime > 0 && endTime <= System.currentTimeMillis())
                    || allSize > minBytes
                    || allSize > maxBytes){
                    if (log.isDebugEnabled()) {
                        log.debug(" Request {}: Complete read {} entries with size {}",
                            fetch.getHeader(), entriesRead.get(), allSize);
                    }

                    AtomicBoolean allPartitionsNoEntry = new AtomicBoolean(true);
                    responseValues.forEach((kafkaPartition, entries) -> {
                        final FetchResponse.PartitionData partitionData;

                        // Add cursor and offset back to TCM when all the read completed.
                        TopicName pulsarTopicName = pulsarTopicName(kafkaPartition, requestHandler.getNamespace());
                        Pair<ManagedCursor, Long> pair = cursors.get(kafkaPartition);
                        requestHandler.getTopicManager()
                            .getTopicConsumerManager(pulsarTopicName.toString())
                            .thenAccept(cm ->
                                cm.add(pair.getRight(), pair));

                        if (entries.isEmpty()) {
                            partitionData = new FetchResponse.PartitionData(
                                Errors.NONE,
                                FetchResponse.INVALID_HIGHWATERMARK,
                                FetchResponse.INVALID_LAST_STABLE_OFFSET,
                                FetchResponse.INVALID_LOG_START_OFFSET,
                                null,
                                MemoryRecords.EMPTY);
                        } else {
                            allPartitionsNoEntry.set(false);
                            Entry entry = entries.get(entries.size() - 1);
                            long entryOffset = MessageIdUtils.getOffset(entry.getLedgerId(), entry.getEntryId());
                            long highWatermark = entryOffset
                                + cursors.get(kafkaPartition).getLeft().getNumberOfEntries();

                            // by default kafka is produced message in batched mode.
                            MemoryRecords records;
                            records = entriesToRecords(entries);

                            partitionData = new FetchResponse.PartitionData(
                                Errors.NONE,
                                highWatermark,
                                highWatermark,
                                highWatermark,
                                null,
                                records);
                        }
                        responseData.put(kafkaPartition, partitionData);
                    });

                    if (allPartitionsNoEntry.get()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Request {}: All partitions for request read 0 entry",
                                fetch.getHeader());
                        }

                        requestHandler.getPulsarService().getExecutor().schedule(() -> {
                            resultFuture.complete(
                                new FetchResponse(Errors.NONE,
                                    responseData,
                                    ((Integer) THROTTLE_TIME_MS.defaultValue),
                                    ((FetchRequest) fetch.getRequest()).metadata().sessionId()));
                            this.recycle();
                        }, waitTime, TimeUnit.MILLISECONDS);
                    } else {
                        resultFuture.complete(
                            new FetchResponse(
                                Errors.NONE,
                                responseData,
                                ((Integer) THROTTLE_TIME_MS.defaultValue),
                                ((FetchRequest) fetch.getRequest()).metadata().sessionId()));
                        this.recycle();
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Request {}: Read time or size not reach, do another round of read before return.",
                            fetch.getHeader());
                    }
                    // need do another round read
                    readMessagesInternal(fetch, cursors, bytesRead, responseValues, resultFuture, responseData);
                }
            });
    }

    private Map<TopicPartition, CompletableFuture<Entry>> readAllCursorOnce(
        Map<TopicPartition, Pair<ManagedCursor, Long>> cursors) {
        Map<TopicPartition, CompletableFuture<Entry>> readFutures = new ConcurrentHashMap<>();

        cursors.entrySet().forEach(cursorOffsetPair -> {
            ManagedCursor cursor;
            CompletableFuture<Entry> readFuture = new CompletableFuture<>();

            cursor = cursorOffsetPair.getValue().getLeft();
            long currentOffset = cursorOffsetPair.getValue().getRight();

            // only read 1 entry currently.
            cursor.asyncReadEntries(1,
                new ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> list, Object o) {
                        TopicName topicName = pulsarTopicName(
                            cursorOffsetPair.getKey(),
                            requestHandler.getNamespace());

                        Entry entry = null;
                        if (!list.isEmpty()) {
                            entry = list.get(0);
                            long offset = MessageIdUtils.getOffset(entry.getLedgerId(), entry.getEntryId());
                            PositionImpl currentPosition = PositionImpl
                                .get(entry.getLedgerId(), entry.getEntryId());

                            // commit the offset, so backlog not affect by this cursor.
                            commitOffset((NonDurableCursorImpl) cursor, currentPosition);

                            // get next offset
                            PositionImpl nextPosition = ((NonDurableCursorImpl) cursor)
                                .getNextAvailablePosition(currentPosition);

                            long nextOffset = MessageIdUtils
                                .getOffset(nextPosition.getLedgerId(), nextPosition.getEntryId());

                            // put next offset in to passed in cursors map. and add back to TCM when all read complete.
                            cursors.put(cursorOffsetPair.getKey(), Pair.of(cursor, nextOffset));

                            if (log.isDebugEnabled()) {
                                log.debug("Topic {} success read entry: ledgerId: {}, entryId: {}, size: {},"
                                        + " ConsumerManager original offset: {}, entryOffset: {} - {}, "
                                        + "nextOffset: {} - {}",
                                    topicName.toString(), entry.getLedgerId(), entry.getEntryId(),
                                    entry.getLength(), currentOffset, offset, currentPosition,
                                    nextOffset, nextPosition);
                            }
                        }

                        readFuture.complete(entry);
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException e, Object o) {
                        log.error("Error read entry for topic: {}",
                            pulsarTopicName(cursorOffsetPair.getKey(), requestHandler.getNamespace()));

                        readFuture.completeExceptionally(e);
                    }
                }, null);

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
