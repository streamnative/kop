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

import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.OFFSET_OFFSET;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import io.streamnative.pulsar.handlers.kop.exceptions.KoPMessageMetadataNotFoundException;
import io.streamnative.pulsar.handlers.kop.utils.ByteBufUtils;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodec;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;

/**
 * The entry formatter that uses Kafka's format.
 */
@Slf4j
public class KafkaEntryFormatter implements EntryFormatter {

    // These key-value identifies the entry's format as kafka
    private static final String IDENTITY_KEY = "entry.format";
    private static final String IDENTITY_VALUE = EntryFormatterFactory.EntryFormat.KAFKA.name().toLowerCase();

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private static final int MAX_RECORDS_BUFFER_SIZE = 100 * 1024 * 1024;

    @Override
    public ByteBuf encode(MemoryRecords records, int numMessages) {
        final ByteBuf recordsWrapper = Unpooled.wrappedBuffer(records.buffer());
        final ByteBuf buf = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.None,
                getMessageMetadataWithNumberMessages(numMessages),
                recordsWrapper);
        recordsWrapper.release();
        return buf;
    }

    @Override
    public DecodeResult decode(List<Entry> entries, byte magic) {
        Optional<List<ByteBuf>> optionalByteBufs = Optional.empty();

        // reset header information
        final List<ByteBuf> orderedByteBuf = new ArrayList<>();
        for (Entry entry : entries) {
            try {
                long startOffset = MessageIdUtils.peekBaseOffsetFromEntry(entry);
                final ByteBuf byteBuf = entry.getDataBuffer();
                final MessageMetadata metadata = Commands.parseMessageMetadata(byteBuf);
                if (isKafkaEntryFormat(metadata)) {
                    byteBuf.setLong(byteBuf.readerIndex() + OFFSET_OFFSET, startOffset);
                    byteBuf.setByte(byteBuf.readerIndex() + MAGIC_OFFSET, magic);
                    orderedByteBuf.add(byteBuf.slice(byteBuf.readerIndex(), byteBuf.readableBytes()));
                } else {
                    final ByteBuf kafkaBuffer = createKafkaBuffer(startOffset, magic, metadata, byteBuf);
                    orderedByteBuf.add(kafkaBuffer);
                    if (!optionalByteBufs.isPresent()) {
                        optionalByteBufs = Optional.of(new ArrayList<>());
                    }
                    optionalByteBufs.ifPresent(byteBufs -> byteBufs.add(kafkaBuffer));
                }
            } catch (KoPMessageMetadataNotFoundException | IOException e) { // skip failed decode entry
                log.error("[{}:{}] Failed to decode entry. ", entry.getLedgerId(), entry.getEntryId(), e);
                entry.release();
                return null;
            }
        }

        // batched ByteBuf should be released after sending to client
        int totalSize = orderedByteBuf.stream().mapToInt(ByteBuf::readableBytes).sum();
        ByteBuf batchedByteBuf = PulsarByteBufAllocator.DEFAULT.directBuffer(totalSize);

        for (ByteBuf byteBuf : orderedByteBuf) {
            batchedByteBuf.writeBytes(byteBuf);
        }
        optionalByteBufs.ifPresent(byteBufs -> byteBufs.forEach(ReferenceCounted::release));

        // release entries
        entries.forEach(Entry::release);
        return new DecodeResult(
                MemoryRecords.readableRecords(ByteBufUtils.getNioBuffer(batchedByteBuf)), batchedByteBuf);
    }

    private static MessageMetadata getMessageMetadataWithNumberMessages(int numMessages) {
        final MessageMetadata metadata = new MessageMetadata();
        metadata.addProperty()
                .setKey(IDENTITY_KEY)
                .setValue(IDENTITY_VALUE);
        metadata.setProducerName("");
        metadata.setSequenceId(0L);
        metadata.setPublishTime(System.currentTimeMillis());
        metadata.setNumMessagesInBatch(numMessages);
        return metadata;
    }

    private static boolean isKafkaEntryFormat(final MessageMetadata messageMetadata) {
        final List<KeyValue> keyValues = messageMetadata.getPropertiesList();
        for (KeyValue keyValue : keyValues) {
            if (keyValue.hasKey()
                    && keyValue.getKey().equals(IDENTITY_KEY)
                    && keyValue.getValue().equals(IDENTITY_VALUE)) {
                return true;
            }
        }
        return false;
    }

    private ByteBuf createKafkaBuffer(final long offset,
                                      final byte magic,
                                      final MessageMetadata metadata,
                                      final ByteBuf payload) throws IOException {
        if (metadata.hasMarkerType()
                && (metadata.getMarkerType() == MarkerType.TXN_COMMIT_VALUE
                || metadata.getMarkerType() == MarkerType.TXN_ABORT_VALUE)) {
            final MemoryRecords records = MemoryRecords.withEndTransactionMarker(
                    offset,
                    metadata.getPublishTime(),
                    0,
                    metadata.getTxnidMostBits(),
                    (short) metadata.getTxnidLeastBits(),
                    new EndTransactionMarker(metadata.getMarkerType() == MarkerType.TXN_COMMIT_VALUE
                            ? ControlRecordType.COMMIT : ControlRecordType.ABORT, 0));
            return Unpooled.wrappedBuffer(records.buffer());
        }

        final ByteBuffer byteBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        final MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                byteBuffer,
                magic,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                offset,
                metadata.getPublishTime(),
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                metadata.hasTxnidMostBits() && metadata.hasTxnidLeastBits(),
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                MAX_RECORDS_BUFFER_SIZE);

        final int uncompressedSize = metadata.getUncompressedSize();
        final CompressionCodec codec = CompressionCodecProvider.getCompressionCodec(metadata.getCompression());
        final ByteBuf uncompressedPayload = codec.decode(payload, uncompressedSize);

        if (metadata.hasNumMessagesInBatch()) {
            final int numMessages = metadata.getNumMessagesInBatch();
            for (int i = 0; i < numMessages; i++) {
                final SingleMessageMetadata singleMessageMetadata = new SingleMessageMetadata();
                final ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(
                        uncompressedPayload, singleMessageMetadata, i, numMessages);

                final long timestamp = (metadata.getEventTime() > 0)
                        ? metadata.getEventTime()
                        : metadata.getPublishTime();
                final ByteBuffer value = (singleMessageMetadata.isNullValue())
                        ? null
                        : ByteBufUtils.getNioBuffer(singleMessagePayload);
                final Header[] headers = getHeadersFromMetadata(metadata.getPropertiesList());
                builder.appendWithOffset(offset + i,
                        timestamp,
                        ByteBufUtils.getKeyByteBuffer(singleMessageMetadata),
                        value,
                        headers);
                singleMessagePayload.release();
            }
        } else {
            final long timestamp = (metadata.getEventTime() > 0)
                    ? metadata.getEventTime()
                    : metadata.getPublishTime();
            final Header[] headers = getHeadersFromMetadata(metadata.getPropertiesList());
            builder.appendWithOffset(offset,
                    timestamp,
                    ByteBufUtils.getKeyByteBuffer(metadata),
                    ByteBufUtils.getNioBuffer(uncompressedPayload),
                    headers);
        }

        final MemoryRecords records = builder.build();
        uncompressedPayload.release();
        byteBuffer.flip();

        return Unpooled.wrappedBuffer(records.buffer());
    }

    @NonNull
    private Header[] getHeadersFromMetadata(final List<KeyValue> properties) {
        final Header[] headers = new Header[properties.size()];

        int index = 0;
        for (KeyValue keyValue : properties) {
            headers[index] = new RecordHeader(keyValue.getKey(), keyValue.getValue().getBytes(StandardCharsets.UTF_8));
            index++;
        }

        return headers;
    }
}
