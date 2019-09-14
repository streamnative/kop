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
package io.streamnative.kop.utils;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;

/**
 * Pulsar Message and Kafka Record utils.
 */
@UtilityClass
@Slf4j
public final class MessageRecordUtils {
    private static final int DEFAULT_FETCH_BUFFER_SIZE = 1024 * 1024;
    private static final int MAX_RECORDS_BUFFER_SIZE = 100 * 1024 * 1024;
    private static final String FAKE_KOP_PRODUCER_NAME = "fake_kop_producer_name";

    private static final Clock clock = Clock.systemDefaultZone();

    // convert kafka Record to Pulsar Message.
    public static MessageImpl<byte[]> recordToEntry(Record record) {
        @SuppressWarnings("unchecked")
        TypedMessageBuilderImpl<byte[]> builder = new TypedMessageBuilderImpl(null, Schema.BYTES);

        // key
        if (record.hasKey()) {
            byte[] key = new byte[record.keySize()];
            record.key().get(key);
            builder.keyBytes(key);
        }

        // value
        if (record.hasValue()) {
            byte[] value = new byte[record.valueSize()];
            record.value().get(value);
            builder.value(value);
        } else {
            builder.value(new byte[0]);
        }

        // sequence
        if (record.sequence() >= 0) {
            builder.sequenceId(record.sequence());
        }

        // timestamp
        if (record.timestamp() >= 0) {
            builder.eventTime(record.timestamp());
        }

        // header
        for (Header h : record.headers()) {
            builder.property(h.key(),
                Base64.getEncoder().encodeToString(h.value()));
        }

        return (MessageImpl<byte[]>) builder.getMessage();
    }

    // convert message to ByteBuf payload for ledger.addEntry.
    public static ByteBuf messageToByteBuf(Message<byte[]> message) {
        checkArgument(message instanceof MessageImpl);

        MessageImpl<byte[]> msg = (MessageImpl<byte[]>) message;
        MessageMetadata.Builder msgMetadataBuilder = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();

        // filled in required fields
        if (!msgMetadataBuilder.hasSequenceId()) {
            msgMetadataBuilder.setSequenceId(-1);
        }
        if (!msgMetadataBuilder.hasPublishTime()) {
            msgMetadataBuilder.setPublishTime(clock.millis());
        }
        if (!msgMetadataBuilder.hasProducerName()) {
            msgMetadataBuilder.setProducerName(FAKE_KOP_PRODUCER_NAME);
        }

        msgMetadataBuilder.setCompression(
            CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        msgMetadataBuilder.setUncompressedSize(payload.readableBytes());
        MessageMetadata msgMetadata = msgMetadataBuilder.build();

        ByteBuf buf = Commands.serializeMetadataAndPayload(ChecksumType.Crc32c, msgMetadata, payload);

        msgMetadataBuilder.recycle();
        msgMetadata.recycle();

        return buf;
    }


    // convert entries read from BookKeeper into Kafka Records
    public static MemoryRecords entriesToRecords(List<org.apache.bookkeeper.mledger.Entry> entries) {
        try (ByteBufferOutputStream outputStream = new ByteBufferOutputStream(DEFAULT_FETCH_BUFFER_SIZE)) {
            MemoryRecordsBuilder builder = new MemoryRecordsBuilder(outputStream, RecordBatch.CURRENT_MAGIC_VALUE,
                org.apache.kafka.common.record.CompressionType.NONE,
                TimestampType.CREATE_TIME,
                MessageIdUtils.getOffset(entries.get(0).getLedgerId(), 0),
                RecordBatch.NO_TIMESTAMP,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                MAX_RECORDS_BUFFER_SIZE);

            for (Entry entry : entries) {
                ByteBuf metadataAndPayload = entry.getDataBuffer();
                MessageMetadata msgMetadata = Commands.parseMessageMetadata(metadataAndPayload);
                ByteBuf payload = metadataAndPayload.retain();

                byte[] data = new byte[payload.readableBytes()];
                payload.readBytes(data);

                builder.appendWithOffset(
                    MessageIdUtils.getOffset(entry.getLedgerId(), entry.getEntryId()),
                    msgMetadata.getEventTime(),
                    Base64.getDecoder().decode(msgMetadata.getPartitionKey()),
                    data);
            }
            return builder.build();
        } catch (IOException ioe) {
            log.error("Meet IOException: {}", ioe);
            throw new UncheckedIOException(ioe);
        } catch (Exception e) {
            log.error("Meet exception: {}", e);
            throw e;
        }
    }


    //// for Batch messages
    protected static final int INITIAL_BATCH_BUFFER_SIZE = 1024;
    protected static final int MAX_MESSAGE_BATCH_SIZE_BYTES = 128 * 1024;
    public static ByteBuf recordsToByteBuf(MemoryRecords records, int size) {
        long currentBatchSizeBytes = 0;
        int numMessagesInBatch = 0;

        long sequenceId = -1;
        PulsarApi.CompressionType compressionType = PulsarApi.CompressionType.NONE;

        ByteBuf batchedMessageMetadataAndPayload = PulsarByteBufAllocator.DEFAULT
            .buffer(Math.min(INITIAL_BATCH_BUFFER_SIZE, MAX_MESSAGE_BATCH_SIZE_BYTES));
        List<MessageImpl<byte[]>> messages = Lists.newArrayListWithExpectedSize(size);
        MessageMetadata.Builder messageMetaBuilder = MessageMetadata.newBuilder();

        Iterator<Record> iterator = records.records().iterator();
        while (iterator.hasNext()) {
            MessageImpl<byte[]> message = recordToEntry(iterator.next());
            if (++numMessagesInBatch == 1) {
                sequenceId = Commands.initBatchMessageMetadata(messageMetaBuilder, message.getMessageBuilder());
            }
            messages.add(message);
            currentBatchSizeBytes += message.getDataBuffer().readableBytes();

            if (log.isDebugEnabled()) {
                log.debug("recordsToByteBuf , sequenceId: {}, numMessagesInBatch: {}, currentBatchSizeBytes: {} ",
                    sequenceId, numMessagesInBatch, currentBatchSizeBytes);
            }
        }

        for (MessageImpl<byte[]> msg : messages) {
            PulsarApi.MessageMetadata.Builder msgBuilder = msg.getMessageBuilder();
            batchedMessageMetadataAndPayload = Commands.serializeSingleMessageInBatchWithPayload(msgBuilder,
                msg.getDataBuffer(), batchedMessageMetadataAndPayload);
            msgBuilder.recycle();
        }
        int uncompressedSize = batchedMessageMetadataAndPayload.readableBytes();

        messageMetaBuilder.setCompression(compressionType);
        messageMetaBuilder.setUncompressedSize(uncompressedSize);
        messageMetaBuilder.setNumMessagesInBatch(numMessagesInBatch);

        MessageMetadata msgMetadata = messageMetaBuilder.build();

        ByteBuf buf = Commands.serializeMetadataAndPayload(ChecksumType.Crc32c, msgMetadata, batchedMessageMetadataAndPayload);

        messageMetaBuilder.recycle();
        msgMetadata.recycle();

        return buf;
    }
}
