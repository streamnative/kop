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
package io.streamnative.pulsar.handlers.kop.utils;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;


/**
 * Utils for ByteBuf operations.
 */
@Slf4j
public class ByteBufUtils {

    public static ByteBuffer getKeyByteBuffer(SingleMessageMetadata messageMetadata) {
        if (messageMetadata.hasOrderingKey()) {
            return ByteBuffer.wrap(messageMetadata.getOrderingKey()).asReadOnlyBuffer();
        }

        if (messageMetadata.hasPartitionKey()) {
            final String key = messageMetadata.getPartitionKey();
            if (messageMetadata.hasPartitionKeyB64Encoded()) {
                return ByteBuffer.wrap(Base64.getDecoder().decode(key)).asReadOnlyBuffer();
            } else {
                // for Base64 not encoded string, convert to UTF_8 chars
                return ByteBuffer.wrap(key.getBytes(UTF_8));
            }
        } else {
            return ByteBuffer.allocate(0);
        }
    }

    public static ByteBuffer getKeyByteBuffer(MessageMetadata messageMetadata) {
        if (messageMetadata.hasOrderingKey()) {
            return ByteBuffer.wrap(messageMetadata.getOrderingKey()).asReadOnlyBuffer();
        }

        String key = messageMetadata.getPartitionKey();
        if (messageMetadata.hasPartitionKeyB64Encoded()) {
            return ByteBuffer.wrap(Base64.getDecoder().decode(key));
        } else {
            // for Base64 not encoded string, convert to UTF_8 chars
            return ByteBuffer.wrap(key.getBytes(UTF_8));
        }
    }

    public static ByteBuffer getNioBuffer(ByteBuf buffer) {
        if (buffer.isDirect()) {
            return buffer.nioBuffer();
        }
        final byte[] bytes = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), bytes);
        return ByteBuffer.wrap(bytes);
    }

    public MemoryRecords decodePulsarEntryToKafkaRecords(final MessageMetadata msgMetadata,
                                                         final ByteBuf payload,
                                                         final long baseOffset,
                                                         final byte magic,
                                                         final long ledgerId,
                                                         final long entryId) {

        if (msgMetadata.hasMarkerType()
                && (msgMetadata.getMarkerType() == MarkerType.TXN_COMMIT_VALUE
                || msgMetadata.getMarkerType() == MarkerType.TXN_ABORT_VALUE)) {
            return MemoryRecords.withEndTransactionMarker(
                    baseOffset,
                    msgMetadata.getPublishTime(),
                    0,
                    msgMetadata.getTxnidMostBits(),
                    (short) msgMetadata.getTxnidLeastBits(),
                    new EndTransactionMarker(
                            msgMetadata.getMarkerType() == MarkerType.TXN_COMMIT_VALUE
                                    ? ControlRecordType.COMMIT : ControlRecordType.ABORT, 0));
        }

        final CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(msgMetadata.getCompression());
        final int uncompressedSize = msgMetadata.getUncompressedSize();
        ByteBuf uncompressedPayload;
        try {
            uncompressedPayload = codec.decode(payload, uncompressedSize);
        } catch (IOException e) {
            log.error("Failed to uncompress payload of offset: {} and skip it", baseOffset, e);
            return null;
        }

        final ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024);
        final MemoryRecordsBuilder builder = new MemoryRecordsBuilder(byteBuffer,
                magic,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                baseOffset,
                msgMetadata.getPublishTime(),
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                msgMetadata.hasTxnidMostBits() && msgMetadata.hasTxnidLeastBits(),
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                100 * 1024 * 1024);
        if (msgMetadata.hasTxnidMostBits()) {
            builder.setProducerState(msgMetadata.getTxnidMostBits(), (short) msgMetadata.getTxnidLeastBits(), 0, true);
        }

        final int numMessages = msgMetadata.getNumMessagesInBatch();
        final boolean isBatched = (msgMetadata.hasNumMessagesInBatch() || numMessages != 1);

        if (log.isDebugEnabled()) {
            log.debug("Entry to MemoryRecords, numMessages: {}, isBatched: {}, ledgerId: {}, entryId: {}",
                    numMessages, isBatched, ledgerId, entryId);
        }

        if (isBatched) {
            IntStream.range(0, numMessages).parallel().forEachOrdered(i -> {
                if (log.isDebugEnabled()) {
                    log.debug(" processing message num - {} in batch", i);
                }
                final SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
                try {
                    final ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(
                            uncompressedPayload, singleMessageMetadata, i, numMessages);
                    final ByteBuffer value = (singleMessageMetadata.isNullValue()
                            ? null
                            : getNioBuffer(singleMessagePayload));
                    builder.appendWithOffset(
                            baseOffset + i,
                            msgMetadata.getEventTime() > 0 ? msgMetadata.getEventTime() : msgMetadata.getPublishTime(),
                            ByteBufUtils.getKeyByteBuffer(singleMessageMetadata),
                            value,
                            getHeadersFromMetadata(singleMessageMetadata.getPropertiesList()));
                    singleMessagePayload.release();
                } catch (IOException e) {
                    // TODO: how to handle the corrupted single message?
                    log.error("Failed to deserialize single message in batch {}", i, e);
                }
            });
        } else {
            builder.appendWithOffset(
                    baseOffset,
                    msgMetadata.getEventTime() > 0 ? msgMetadata.getEventTime() : msgMetadata.getPublishTime(),
                    getKeyByteBuffer(msgMetadata),
                    getNioBuffer(uncompressedPayload),
                    getHeadersFromMetadata(msgMetadata.getPropertiesList()));
        }

        uncompressedPayload.release();
        builder.close();
        byteBuffer.flip();
        return MemoryRecords.readableRecords(byteBuffer);
    }

    private static Header[] getHeadersFromMetadata(final List<KeyValue> properties) {
        return properties.stream()
                .map(property -> new RecordHeader(
                        property.getKey(),
                        property.getValue().getBytes(StandardCharsets.UTF_8))
                ).toArray(Header[]::new);
    }
}
