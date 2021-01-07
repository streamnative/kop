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
package io.streamnative.pulsar.handlers.kop.format;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.kop.utils.ByteBufUtils;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.api.proto.PulsarApi.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;


/**
 * The entry formatter that uses Pulsar's format.
 */
@Slf4j
public class PulsarEntryFormatter implements EntryFormatter {
    private static final int DEFAULT_FETCH_BUFFER_SIZE = 1024 * 1024;
    private static final int MAX_RECORDS_BUFFER_SIZE = 100 * 1024 * 1024;
    //// for Batch messages
    private static final int INITIAL_BATCH_BUFFER_SIZE = 1024;
    private static final int MAX_MESSAGE_BATCH_SIZE_BYTES = 128 * 1024;

    @Override
    public ByteBuf encode(final MemoryRecords records, final int numMessages) {
        long currentBatchSizeBytes = 0;
        int numMessagesInBatch = 0;

        long sequenceId = -1;
        // TODO: handle different compression type
        PulsarApi.CompressionType compressionType = PulsarApi.CompressionType.NONE;

        ByteBuf batchedMessageMetadataAndPayload = PulsarByteBufAllocator.DEFAULT
                .buffer(Math.min(INITIAL_BATCH_BUFFER_SIZE, MAX_MESSAGE_BATCH_SIZE_BYTES));
        List<MessageImpl<byte[]>> messages = Lists.newArrayListWithExpectedSize(numMessages);
        MessageMetadata.Builder messageMetaBuilder = MessageMetadata.newBuilder();

        StreamSupport.stream(records.records().spliterator(), true).forEachOrdered(record -> {
            MessageImpl<byte[]> message = recordToEntry(record);
            messages.add(message);
            if (messageMetaBuilder.getPublishTime() <= 0) {
                messageMetaBuilder.setPublishTime(message.getPublishTime());
            }
        });

        for (MessageImpl<byte[]> message : messages) {
            if (++numMessagesInBatch == 1) {
                sequenceId = Commands.initBatchMessageMetadata(messageMetaBuilder, message.getMessageBuilder());
            }
            currentBatchSizeBytes += message.getDataBuffer().readableBytes();
            if (log.isDebugEnabled()) {
                log.debug("recordsToByteBuf , sequenceId: {}, numMessagesInBatch: {}, currentBatchSizeBytes: {} ",
                        sequenceId, numMessagesInBatch, currentBatchSizeBytes);
            }

            PulsarApi.MessageMetadata.Builder msgBuilder = message.getMessageBuilder();
            batchedMessageMetadataAndPayload = Commands.serializeSingleMessageInBatchWithPayload(msgBuilder,
                    message.getDataBuffer(), batchedMessageMetadataAndPayload);
            msgBuilder.recycle();
        }

        int uncompressedSize = batchedMessageMetadataAndPayload.readableBytes();

        if (PulsarApi.CompressionType.NONE != compressionType) {
            messageMetaBuilder.setCompression(compressionType);
            messageMetaBuilder.setUncompressedSize(uncompressedSize);
        }

        messageMetaBuilder.setNumMessagesInBatch(numMessagesInBatch);

        MessageMetadata msgMetadata = messageMetaBuilder.build();

        ByteBuf buf = Commands.serializeMetadataAndPayload(ChecksumType.Crc32c,
                msgMetadata,
                batchedMessageMetadataAndPayload);

        messageMetaBuilder.recycle();
        msgMetadata.recycle();
        batchedMessageMetadataAndPayload.release();

        return buf;
    }

    @Override
    public MemoryRecords decode(final List<Entry> entries, final byte magic) {
        try (ByteBufferOutputStream outputStream = new ByteBufferOutputStream(DEFAULT_FETCH_BUFFER_SIZE)) {
            MemoryRecordsBuilder builder = new MemoryRecordsBuilder(outputStream, magic,
                    org.apache.kafka.common.record.CompressionType.NONE,
                    TimestampType.CREATE_TIME,
                    // using the first entry, index 0 as base offset
                    MessageIdUtils.getOffset(entries.get(0).getLedgerId(), entries.get(0).getEntryId(), 0),
                    RecordBatch.NO_TIMESTAMP,
                    RecordBatch.NO_PRODUCER_ID,
                    RecordBatch.NO_PRODUCER_EPOCH,
                    RecordBatch.NO_SEQUENCE,
                    false, false,
                    RecordBatch.NO_PARTITION_LEADER_EPOCH,
                    MAX_RECORDS_BUFFER_SIZE);

            entries.parallelStream().forEachOrdered(entry -> {
                // each entry is a batched message
                ByteBuf metadataAndPayload = entry.getDataBuffer();

                // Uncompress the payload if necessary
                MessageMetadata msgMetadata = Commands.parseMessageMetadata(metadataAndPayload);
                CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(msgMetadata.getCompression());
                int uncompressedSize = msgMetadata.getUncompressedSize();
                ByteBuf payload;
                try {
                    payload = codec.decode(metadataAndPayload, uncompressedSize);
                } catch (IOException ioe) {
                    log.error("Meet IOException: {}", ioe);
                    throw new UncheckedIOException(ioe);
                }
                int numMessages = msgMetadata.getNumMessagesInBatch();
                boolean notBatchMessage = (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch());

                if (log.isDebugEnabled()) {
                    log.debug("entriesToRecords.  NumMessagesInBatch: {}, isBatchMessage: {}, entries in list: {}."
                                    + " new entryId {}:{}, readerIndex: {},  writerIndex: {}",
                            numMessages, !notBatchMessage, entries.size(), entry.getLedgerId(),
                            entry.getEntryId(), payload.readerIndex(), payload.writerIndex());
                }

                // need handle encryption
                checkState(msgMetadata.getEncryptionKeysCount() == 0);

                if (!notBatchMessage) {
                    IntStream.range(0, numMessages).parallel().forEachOrdered(i -> {
                        if (log.isDebugEnabled()) {
                            log.debug(" processing message num - {} in batch", i);
                        }
                        try {
                            SingleMessageMetadata.Builder singleMessageMetadataBuilder = SingleMessageMetadata
                                    .newBuilder();
                            ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(payload,
                                    singleMessageMetadataBuilder, i, numMessages);

                            SingleMessageMetadata singleMessageMetadata = singleMessageMetadataBuilder.build();
                            Header[] headers = getHeadersFromMetadata(singleMessageMetadata.getPropertiesList());

                            final ByteBuffer value = (singleMessageMetadata.getNullValue())
                                    ? null
                                    : ByteBufUtils.getNioBuffer(singleMessagePayload);
                            builder.appendWithOffset(
                                    MessageIdUtils.getOffset(entry.getLedgerId(), entry.getEntryId(), i),
                                    msgMetadata.getEventTime() > 0
                                            ? msgMetadata.getEventTime() : msgMetadata.getPublishTime(),
                                    ByteBufUtils.getKeyByteBuffer(singleMessageMetadata),
                                    value,
                                    headers);
                            singleMessagePayload.release();
                            singleMessageMetadataBuilder.recycle();
                        } catch (IOException e) {
                            log.error("Meet IOException: {}", e);
                            throw new UncheckedIOException(e);
                        }
                    });
                } else {
                    Header[] headers = getHeadersFromMetadata(msgMetadata.getPropertiesList());

                    builder.appendWithOffset(
                            MessageIdUtils.getOffset(entry.getLedgerId(), entry.getEntryId()),
                            msgMetadata.getEventTime() > 0 ? msgMetadata.getEventTime() : msgMetadata.getPublishTime(),
                            ByteBufUtils.getKeyByteBuffer(msgMetadata),
                            ByteBufUtils.getNioBuffer(payload),
                            headers);
                }

                msgMetadata.recycle();
                payload.release();
                entry.release();
            });
            return builder.build();
        } catch (IOException ioe){
            log.error("Meet IOException: {}", ioe);
            throw new UncheckedIOException(ioe);
        } catch (Exception e){
            log.error("Meet exception: {}", e);
            throw e;
        }
    }

    // convert kafka Record to Pulsar Message.
    // convert kafka Record to Pulsar Message.
    // called when publish received Kafka Record into Pulsar.
    private static MessageImpl<byte[]> recordToEntry(Record record) {
        @SuppressWarnings("unchecked")
        TypedMessageBuilderImpl<byte[]> builder = new TypedMessageBuilderImpl(null, Schema.BYTES);

        // key
        if (record.hasKey()) {
            byte[] key = new byte[record.keySize()];
            record.key().get(key);
            builder.keyBytes(key);
            // reuse ordering key to avoid converting string < > bytes
            builder.orderingKey(key);
        }

        // value
        if (record.hasValue()) {
            byte[] value = new byte[record.valueSize()];
            record.value().get(value);
            builder.value(value);
        } else {
            builder.value(null);
        }

        // sequence
        if (record.sequence() >= 0) {
            builder.sequenceId(record.sequence());
        }

        // timestamp
        if (record.timestamp() >= 0) {
            builder.eventTime(record.timestamp());
            builder.getMetadataBuilder().setPublishTime(record.timestamp());
        } else {
            builder.getMetadataBuilder().setPublishTime(System.currentTimeMillis());
        }

        // header
        for (Header h : record.headers()) {
            builder.property(h.key(),
                    new String(h.value(), UTF_8));
        }

        return (MessageImpl<byte[]>) builder.getMessage();
    }

    private Header[] getHeadersFromMetadata(List<KeyValue> properties) {
        Header[] headers = new Header[properties.size()];

        if (log.isDebugEnabled()) {
            log.debug("getHeadersFromMetadata. Header size: {}",
                    properties.size());
        }

        int index = 0;
        for (KeyValue kv: properties) {
            headers[index] = new RecordHeader(kv.getKey(), kv.getValue().getBytes(UTF_8));

            if (log.isDebugEnabled()) {
                log.debug("index: {} kv.getKey: {}. kv.getValue: {}",
                        index, kv.getKey(), kv.getValue());
            }
            index++;
        }

        return headers;
    }


}
