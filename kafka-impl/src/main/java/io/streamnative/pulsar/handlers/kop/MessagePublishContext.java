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
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.broker.service.persistent.MessageDeduplication;

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
    private long sequenceId;
    private long highestSequenceId;
    private String producerName;
    private boolean enableDeduplication;

    /**
     * On Pulsar side, the replicator marker message will skip the deduplication check,
     * For support produce a regular message in KoP when Pulsar deduplication is enabled,
     * KoP uses this method to support this feature.
     *
     * See: https://github.com/streamnative/kop/issues/1225
     */
    @Override
    public boolean isMarkerMessage() {
        return !this.enableDeduplication;
    }

    @Override
    public long getSequenceId() {
        return this.sequenceId;
    }

    @Override
    public long getHighestSequenceId() {
        return this.highestSequenceId;
    }

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
            if (exception instanceof BrokerServiceException.TopicClosedException
                    || exception instanceof BrokerServiceException.TopicTerminatedException
                    || exception instanceof BrokerServiceException.TopicFencedException) {
                log.warn("[{}] Failed to publish message: {}", topic.getName(), exception.getMessage());
                offsetFuture.completeExceptionally(new NotLeaderForPartitionException());
            } else if (exception instanceof MessageDeduplication.MessageDupUnknownException) {
                log.warn("[{}] Failed to publish message: {}", topic.getName(), exception.getMessage());
                offsetFuture.completeExceptionally(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER.exception());
            } else {
                log.error("[{}] Failed to publish message", topic.getName(), exception);
                offsetFuture.completeExceptionally(new KafkaStorageException(exception));
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Success write topic: {}, producerName {} ledgerId: {}, entryId: {}"
                        + " And triggered send callback.",
                    topic.getName(), producerName, ledgerId, entryId);
            }

            topic.recordAddLatency(System.nanoTime() - startTimeNs, TimeUnit.NANOSECONDS);

            // duplicated message
            if (ledgerId == -1 && entryId == -1) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to write topic: {}, producerName {}, ledgerId: {}, entryId: {}"
                                    + " with duplicated message.",
                            topic.getName(), producerName, ledgerId, entryId);
                }
                offsetFuture.completeExceptionally(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER.exception());
                return;
            }
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
                                            String producerName,
                                            boolean enableDeduplication,
                                            long sequenceId,
                                            long highestSequenceId,
                                            int numberOfMessages,
                                            long startTimeNs) {
        MessagePublishContext callback = RECYCLER.get();
        callback.offsetFuture = offsetFuture;
        callback.topic = topic;
        callback.producerName = producerName;
        callback.numberOfMessages = numberOfMessages;
        callback.startTimeNs = startTimeNs;
        callback.baseOffset = DEFAULT_OFFSET;
        callback.sequenceId = sequenceId;
        callback.highestSequenceId = highestSequenceId;
        callback.peekOffsetError = null;
        callback.enableDeduplication = enableDeduplication;
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
    public String getProducerName() {
        return this.producerName;
    }

    @Override
    public long getNumberOfMessages() {
        return numberOfMessages;
    }

    public void recycle() {
        offsetFuture = null;
        producerName = null;
        topic = null;
        startTimeNs = -1;
        numberOfMessages = 0;
        baseOffset = DEFAULT_OFFSET;
        sequenceId = -1;
        highestSequenceId = -1;
        peekOffsetError = null;
        enableDeduplication = false;
        recyclerHandle.recycle(this);
    }
}
