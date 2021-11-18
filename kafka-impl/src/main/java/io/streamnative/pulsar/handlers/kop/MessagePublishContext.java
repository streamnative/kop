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
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.streamnative.pulsar.handlers.kop.exceptions.MetadataCorruptedException;
import io.streamnative.pulsar.handlers.kop.utils.MessageMetadataUtils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.Topic.PublishContext;

/**
 * Implementation for PublishContext.
 */
@Slf4j
public final class MessagePublishContext implements PublishContext {

    public static final long DEFAULT_OFFSET = -1L;

    private CompletableFuture<Long> offsetFuture;
    private Topic topic;
    private long startTimeNs;
    private int numberOfMessages;
    private long baseOffset;
    private MetadataCorruptedException peekOffsetError;

    @Override
    public void setMetadataFromEntryData(ByteBuf entryData) {
        try {
            baseOffset = MessageMetadataUtils.peekBaseOffset(entryData, numberOfMessages);
        } catch (MetadataCorruptedException e) {
            peekOffsetError = e;
        }
    }

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
            // setMetadataFromEntryData() was called before completed() is called so that baseOffset could be set
            if (baseOffset == DEFAULT_OFFSET) {
                log.error("[{}] Failed to get offset for ({}, {}): {}",
                        topic, ledgerId, entryId, peekOffsetError.getMessage());
            }
            offsetFuture.complete(baseOffset);
        }

        recycle();
    }

    // recycler
    public static MessagePublishContext get(CompletableFuture<Long> offsetFuture,
                                            Topic topic,
                                            int numberOfMessages,
                                            long startTimeNs) {
        MessagePublishContext callback = RECYCLER.get();
        callback.offsetFuture = offsetFuture;
        callback.topic = topic;
        callback.numberOfMessages = numberOfMessages;
        callback.startTimeNs = startTimeNs;
        callback.baseOffset = DEFAULT_OFFSET;
        callback.peekOffsetError = null;
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

    @Override
    public long getNumberOfMessages() {
        return numberOfMessages;
    }

    public void recycle() {
        offsetFuture = null;
        topic = null;
        startTimeNs = -1;
        numberOfMessages = 0;
        baseOffset = DEFAULT_OFFSET;
        peekOffsetError = null;
        recyclerHandle.recycle(this);
    }
}
