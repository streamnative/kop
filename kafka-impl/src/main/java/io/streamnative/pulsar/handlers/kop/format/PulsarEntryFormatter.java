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
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
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

        ByteBuf batchedMessageMetadataAndPayload = PulsarByteBufAllocator.DEFAULT
                .buffer(Math.min(INITIAL_BATCH_BUFFER_SIZE, MAX_MESSAGE_BATCH_SIZE_BYTES));
        List<MessageImpl<byte[]>> messages = Lists.newArrayListWithExpectedSize(numMessages);
        final MessageMetadata msgMetadata = new MessageMetadata();

        records.batches().forEach(recordBatch -> {
            StreamSupport.stream(recordBatch.spliterator(), true).forEachOrdered(record -> {
                MessageImpl<byte[]> message = recordToEntry(record);
                messages.add(message);
                if (recordBatch.isTransactional()) {
                    msgMetadata.setTxnidMostBits(recordBatch.producerId());
                    msgMetadata.setTxnidLeastBits(recordBatch.producerEpoch());
                }
            });
        });

        for (MessageImpl<byte[]> message : messages) {
            if (++numMessagesInBatch == 1) {
                // msgMetadata will set publish time here
                sequenceId = Commands.initBatchMessageMetadata(msgMetadata, message.getMessageBuilder());
            }
            currentBatchSizeBytes += message.getDataBuffer().readableBytes();
            if (log.isDebugEnabled()) {
                log.debug("recordsToByteBuf , sequenceId: {}, numMessagesInBatch: {}, currentBatchSizeBytes: {} ",
                        sequenceId, numMessagesInBatch, currentBatchSizeBytes);
            }

            final MessageMetadata msgBuilder = message.getMessageBuilder();
            batchedMessageMetadataAndPayload = Commands.serializeSingleMessageInBatchWithPayload(msgBuilder,
                    message.getDataBuffer(), batchedMessageMetadataAndPayload);
        }

        msgMetadata.setNumMessagesInBatch(numMessagesInBatch);

        ByteBuf buf = Commands.serializeMetadataAndPayload(ChecksumType.Crc32c,
                msgMetadata,
                batchedMessageMetadataAndPayload);

        batchedMessageMetadataAndPayload.release();

        return buf;
    }

    @Override
    public MemoryRecords decode(final List<Entry> entries, final byte magic) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(DEFAULT_FETCH_BUFFER_SIZE);

        entries.parallelStream().forEachOrdered(entry -> {
            long baseOffset = MessageIdUtils.peekBaseOffsetFromEntry(entry);
            // each entry is a batched message
            ByteBuf metadataAndPayload = entry.getDataBuffer();

            Commands.skipBrokerEntryMetadataIfExist(metadataAndPayload);
            MessageMetadata msgMetadata = Commands.parseMessageMetadata(metadataAndPayload);

            if (msgMetadata.hasMarkerType()
                    && (msgMetadata.getMarkerType() == MarkerType.TXN_COMMIT_VALUE
                    || msgMetadata.getMarkerType() == MarkerType.TXN_ABORT_VALUE)) {
                MemoryRecords memoryRecords = MemoryRecords.withEndTransactionMarker(
                        baseOffset,
                        msgMetadata.getPublishTime(),
                        0,
                        msgMetadata.getTxnidMostBits(),
                        (short) msgMetadata.getTxnidLeastBits(),
                        new EndTransactionMarker(
                                msgMetadata.getMarkerType() == MarkerType.TXN_COMMIT_VALUE
                                        ? ControlRecordType.COMMIT : ControlRecordType.ABORT, 0));
                byteBuffer.put(memoryRecords.buffer());
                return;
            }

            MemoryRecordsBuilder builder = new MemoryRecordsBuilder(byteBuffer, magic,
                    org.apache.kafka.common.record.CompressionType.NONE,
                    TimestampType.CREATE_TIME,
                    // using the first entry, index 0 as base offset
                    baseOffset,
                    msgMetadata.getPublishTime(),
                    RecordBatch.NO_PRODUCER_ID,
                    RecordBatch.NO_PRODUCER_EPOCH,
                    RecordBatch.NO_SEQUENCE,
                    msgMetadata.hasTxnidMostBits() && msgMetadata.hasTxnidLeastBits(),
                    false,
                    RecordBatch.NO_PARTITION_LEADER_EPOCH,
                    MAX_RECORDS_BUFFER_SIZE);

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

            if (msgMetadata.hasTxnidMostBits()) {
                builder.setProducerState(
                        msgMetadata.getTxnidMostBits(),
                        (short) msgMetadata.getTxnidLeastBits(), 0, true);
            }

            if (!notBatchMessage) {
                IntStream.range(0, numMessages).parallel().forEachOrdered(i -> {
                    if (log.isDebugEnabled()) {
                        log.debug(" processing message num - {} in batch", i);
                    }
                    try {
                        final SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
                        ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(payload,
                                singleMessageMetadata, i, numMessages);

                        Header[] headers = getHeadersFromMetadata(singleMessageMetadata.getPropertiesList());

                        final ByteBuffer value = (singleMessageMetadata.isNullValue())
                                ? null
                                : ByteBufUtils.getNioBuffer(singleMessagePayload);
                        builder.appendWithOffset(
                                baseOffset + i,
                                msgMetadata.getEventTime() > 0
                                        ? msgMetadata.getEventTime() : msgMetadata.getPublishTime(),
                                ByteBufUtils.getKeyByteBuffer(singleMessageMetadata),
                                value,
                                headers);
                        singleMessagePayload.release();
                    } catch (IOException e) {
                        log.error("Meet IOException: {}", e);
                        throw new UncheckedIOException(e);
                    }
                });
            } else {
                Header[] headers = getHeadersFromMetadata(msgMetadata.getPropertiesList());

                builder.appendWithOffset(
                        baseOffset,
                        msgMetadata.getEventTime() > 0 ? msgMetadata.getEventTime() : msgMetadata.getPublishTime(),
                        ByteBufUtils.getKeyByteBuffer(msgMetadata),
                        ByteBufUtils.getNioBuffer(payload),
                        headers);
            }

            payload.release();
            entry.release();

            builder.close();
        });

        byteBuffer.flip();
        return MemoryRecords.readableRecords(byteBuffer);
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

        // Following fields are required in `Commands.initBatchMessageMetadata`, but since we write to
        // bookie directly, broker won't make use of them. So here we just set trivial values.
        builder.getMetadataBuilder().setProducerName("");
        if (record.sequence() >= 0) {
            builder.sequenceId(record.sequence());
        } else {
            builder.sequenceId(0L);
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
