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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.kop.exceptions.KoPMessageMetadataNotFoundException;
import io.streamnative.pulsar.handlers.kop.utils.ByteBufUtils;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;


/**
 * The entry formatter that uses Pulsar's format.
 */
@Slf4j
public class PulsarEntryFormatter implements EntryFormatter {
    //// for Batch messages
    private static final int INITIAL_BATCH_BUFFER_SIZE = 1024;
    private static final int MAX_MESSAGE_BATCH_SIZE_BYTES = 128 * 1024;

    private static final DecodeResult EMPTY_DECODE_RESULT = new DecodeResult(
            MemoryRecords.readableRecords(ByteBuffer.allocate(0)));

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
    public DecodeResult decode(final List<Entry> entries, final byte magic) {
        final List<DecodeResult> decodeResults = new ArrayList<>();

        entries.parallelStream().forEachOrdered(entry -> {
            try {
                long baseOffset = MessageIdUtils.peekBaseOffsetFromEntry(entry);
                // each entry is a batched message
                ByteBuf metadataAndPayload = entry.getDataBuffer();

                MessageMetadata msgMetadata = Commands.parseMessageMetadata(metadataAndPayload);

                decodeResults.add(ByteBufUtils.decodePulsarEntryToKafkaRecords(
                        msgMetadata, metadataAndPayload, baseOffset, magic));
            } catch (KoPMessageMetadataNotFoundException | IOException e) { // skip failed decode entry
                log.error("[{}:{}] Failed to decode entry", entry.getLedgerId(), entry.getEntryId());
            } finally {
                entry.release();
            }
        });

        if (decodeResults.isEmpty()) {
            return EMPTY_DECODE_RESULT;
        } else if (decodeResults.size() == 1) {
            return decodeResults.get(0);
        } else {
            final int totalSize = decodeResults.stream()
                    .mapToInt(decodeResult -> decodeResult.getRecords().sizeInBytes())
                    .sum();
            final ByteBuf mergedBuffer = PulsarByteBufAllocator.DEFAULT.directBuffer(totalSize);
            decodeResults.forEach(decodeResult -> mergedBuffer.writeBytes(decodeResult.getRecords().buffer()));
            decodeResults.forEach(DecodeResult::release);
            return new DecodeResult(MemoryRecords.readableRecords(mergedBuffer.nioBuffer()), mergedBuffer);
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

}
