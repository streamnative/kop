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

import static io.streamnative.pulsar.handlers.kop.utils.MessageRecordUtils.messageToByteBuf;
import static io.streamnative.pulsar.handlers.kop.utils.MessageRecordUtils.recordToEntry;
import static io.streamnative.pulsar.handlers.kop.utils.MessageRecordUtils.recordsToByteBuf;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.Topic.PublishContext;

/**
 * Implementation for PublishContext.
 */
@Slf4j
public final class MessagePublishContext implements PublishContext {

    private CompletableFuture<Long> offsetFuture;
    private Topic topic;
    private long startTimeNs;
    public static final boolean MESSAGE_BATCHED = true;

    /**
     * Executed from managed ledger thread when the message is persisted.
     */
    @Override
    public void completed(Exception exception, long ledgerId, long entryId) {

        if (exception != null) {
            log.error("Failed write entry: ledgerId: {}, entryId: {}. triggered send callback.",
                ledgerId, entryId);
            offsetFuture.completeExceptionally(exception);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Success write topic: {}, ledgerId: {}, entryId: {}"
                        + " And triggered send callback.",
                    topic.getName(), ledgerId, entryId);
            }

            topic.recordAddLatency(System.nanoTime() - startTimeNs, TimeUnit.MICROSECONDS);

            offsetFuture.complete(Long.valueOf(MessageIdUtils.getOffset(ledgerId, entryId)));
        }

        recycle();
    }

    // recycler
    public static MessagePublishContext get(CompletableFuture<Long> offsetFuture,
                                            Topic topic,
                                            long startTimeNs) {
        MessagePublishContext callback = RECYCLER.get();
        callback.offsetFuture = offsetFuture;
        callback.topic = topic;
        callback.startTimeNs = startTimeNs;
        return callback;
    }

    private final Handle<MessagePublishContext> recyclerHandle;

    private MessagePublishContext(Handle<MessagePublishContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<MessagePublishContext> RECYCLER = new Recycler<MessagePublishContext>() {
        protected MessagePublishContext newObject(Handle<MessagePublishContext> handle) {
            return new MessagePublishContext(handle);
        }
    };

    public void recycle() {
        offsetFuture = null;
        topic = null;
        startTimeNs = -1;
        recyclerHandle.recycle(this);
    }


    // publish Kafka records to pulsar topic, handle callback in MessagePublishContext.
    public static void publishMessages(MemoryRecords records,
                                       Topic topic,
                                       CompletableFuture<PartitionResponse> future,
                                       ScheduledExecutorService executor) {

        // get records size.
        AtomicInteger size = new AtomicInteger(0);
        records.records().forEach(record -> size.incrementAndGet());
        int rec = size.get();

        if (log.isDebugEnabled()) {
            log.debug("publishMessages for topic partition: {} , records size is {} ", topic.getName(), size.get());
        }

        if (MESSAGE_BATCHED) {
            CompletableFuture<Long> offsetFuture = new CompletableFuture<>();

//            ByteBuf headerAndPayload = recordsToByteBuf(records, rec);
            CompletableFuture<ByteBuf> transFuture = new CompletableFuture<>();
            //put queue
            executor.submit(() -> {
                recordsToByteBuf(records, rec, transFuture);
            });

            transFuture.whenComplete((headerAndPayload, ex) -> {
                if (ex != null) {
                    log.error("record to bytebuf error: ", ex);
                } else {
                    topic.publishMessage(
                            headerAndPayload,
                            MessagePublishContext.get(
                                    offsetFuture, topic, System.nanoTime()));
                }
            });

//            topic.publishMessage(
//                headerAndPayload,
//                MessagePublishContext.get(
//                    offsetFuture, topic, System.nanoTime()));

            offsetFuture.whenComplete((offset, ex) -> {
                if (ex != null) {
                    log.error("publishMessages for topic partition: {} failed when write.",
                        topic.getName(), ex);
                    future.complete(new PartitionResponse(Errors.KAFKA_STORAGE_ERROR));
                } else {
                    future.complete(new PartitionResponse(Errors.NONE));
                }
            });
        } else {
            List<CompletableFuture<Long>> futures = Collections
                .synchronizedList(Lists.newArrayListWithExpectedSize(size.get()));

            records.records().forEach(record -> {
                CompletableFuture<Long> offsetFuture = new CompletableFuture<>();
                futures.add(offsetFuture);
                ByteBuf headerAndPayload = messageToByteBuf(recordToEntry(record));
                topic.publishMessage(
                    headerAndPayload,
                    MessagePublishContext.get(
                        offsetFuture, topic, System.nanoTime()));
            });

            CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[rec])).whenComplete((ignore, ex) -> {
                if (ex != null) {
                    log.error("publishMessages for topic partition: {} failed when write.",
                        topic.getName(), ex);
                    future.complete(new PartitionResponse(Errors.KAFKA_STORAGE_ERROR));
                } else {
                    future.complete(new PartitionResponse(Errors.NONE));
                }
            });
        }
    }
}
