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
import io.streamnative.pulsar.handlers.kop.format.DecodeResult;
import io.streamnative.pulsar.handlers.kop.format.DirectBufferOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import lombok.NonNull;
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

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private static final int MAX_RECORDS_BUFFER_SIZE = 10 * 1024 * 1024;

    public static ByteBuffer getKeyByteBuffer(SingleMessageMetadata messageMetadata) {
        if (messageMetadata.hasOrderingKey()) {
            return ByteBuffer.wrap(messageMetadata.getOrderingKey()).asReadOnlyBuffer();
        }

        if (!messageMetadata.hasPartitionKey()) {
            return null;
        }

        final String key = messageMetadata.getPartitionKey();
        if (messageMetadata.isPartitionKeyB64Encoded()) {
            return ByteBuffer.wrap(Base64.getDecoder().decode(key)).asReadOnlyBuffer();
        } else {
            // for Base64 not encoded string, convert to UTF_8 chars
            return ByteBuffer.wrap(key.getBytes(UTF_8));
        }
    }

    public static ByteBuffer getKeyByteBuffer(MessageMetadata messageMetadata) {
        if (messageMetadata.hasOrderingKey()) {
            return ByteBuffer.wrap(messageMetadata.getOrderingKey()).asReadOnlyBuffer();
        }

        if (!messageMetadata.hasPartitionKey()) {
            return null;
        }
        String key = messageMetadata.getPartitionKey();
        if (messageMetadata.isPartitionKeyB64Encoded()) {
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

    public static DecodeResult decodePulsarEntryToKafkaRecords(final MessageMetadata metadata,
                                                               final ByteBuf payload,
                                                               final long baseOffset,
                                                               final byte magic) throws IOException {
        if (metadata.hasMarkerType()
                && (metadata.getMarkerType() == MarkerType.TXN_COMMIT_VALUE
                || metadata.getMarkerType() == MarkerType.TXN_ABORT_VALUE)) {
            return DecodeResult.get(MemoryRecords.withEndTransactionMarker(
                    baseOffset,
                    metadata.getPublishTime(),
                    0,
                    metadata.getTxnidMostBits(),
                    (short) metadata.getTxnidLeastBits(),
                    new EndTransactionMarker(metadata.getMarkerType() == MarkerType.TXN_COMMIT_VALUE
                            ? ControlRecordType.COMMIT : ControlRecordType.ABORT, 0)));
        }
        final int uncompressedSize = metadata.getUncompressedSize();
        final CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(metadata.getCompression());
        final ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);

        final DirectBufferOutputStream directBufferOutputStream = new DirectBufferOutputStream(DEFAULT_BUFFER_SIZE);
        final MemoryRecordsBuilder builder = new MemoryRecordsBuilder(directBufferOutputStream,
                magic,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                baseOffset,
                metadata.getPublishTime(),
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                metadata.hasTxnidMostBits() && metadata.hasTxnidLeastBits(),
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                MAX_RECORDS_BUFFER_SIZE);
        if (metadata.hasTxnidMostBits()) {
            builder.setProducerState(metadata.getTxnidMostBits(), (short) metadata.getTxnidLeastBits(), 0, true);
        }

        int conversionCount = 0;
        if (metadata.hasNumMessagesInBatch()) {
            final int numMessages = metadata.getNumMessagesInBatch();
            conversionCount += numMessages;
            for (int i = 0; i < numMessages; i++) {
                final SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
                final ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(
                        uncompressedPayload, singleMessageMetadata, i, numMessages);

                final long timestamp = (metadata.getEventTime() > 0)
                        ? metadata.getEventTime()
                        : metadata.getPublishTime();
                final ByteBuffer value = singleMessageMetadata.isNullValue()
                        ? null
                        : getNioBuffer(singleMessagePayload);
                if (magic >= RecordBatch.MAGIC_VALUE_V2) {
                    final Header[] headers = getHeadersFromMetadata(singleMessageMetadata.getPropertiesList());
                    builder.appendWithOffset(baseOffset + i,
                            timestamp,
                            getKeyByteBuffer(singleMessageMetadata),
                            value,
                            headers);
                } else {
                    // record less than magic=2, no header attribute
                    builder.appendWithOffset(baseOffset + i,
                            timestamp,
                            getKeyByteBuffer(singleMessageMetadata),
                            value);
                }
                singleMessagePayload.release();
            }
        } else {
            conversionCount += 1;
            final long timestamp = (metadata.getEventTime() > 0)
                    ? metadata.getEventTime()
                    : metadata.getPublishTime();
            if (magic >= RecordBatch.MAGIC_VALUE_V2) {
                final Header[] headers = getHeadersFromMetadata(metadata.getPropertiesList());
                builder.appendWithOffset(baseOffset,
                        timestamp,
                        getKeyByteBuffer(metadata),
                        getNioBuffer(uncompressedPayload),
                        headers);
            } else {
                builder.appendWithOffset(baseOffset,
                        timestamp,
                        getKeyByteBuffer(metadata),
                        getNioBuffer(uncompressedPayload));
            }
        }

        final MemoryRecords records = builder.build();
        uncompressedPayload.release();
        return DecodeResult.get(records, directBufferOutputStream.getByteBuf(), conversionCount);
    }

    @NonNull
    private static Header[] getHeadersFromMetadata(final List<KeyValue> properties) {
        return properties.stream()
                .map(property -> new RecordHeader(
                        property.getKey(),
                        property.getValue().getBytes(UTF_8))
                ).toArray(Header[]::new);
    }
}
