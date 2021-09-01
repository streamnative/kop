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
import io.streamnative.pulsar.handlers.kop.utils.KopRecordsUtil;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.ConvertedRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

/**
 * The entry formatter that uses Kafka's format.
 */
@Slf4j
public class KafkaEntryFormatter implements EntryFormatter {

    // These key-value identifies the entry's format as kafka
    private static final String IDENTITY_KEY = "entry.format";
    private static final String IDENTITY_VALUE = EntryFormatterFactory.EntryFormat.KAFKA.name().toLowerCase();

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
                    byte batchMagic = byteBuf.getByte(byteBuf.readerIndex() + MAGIC_OFFSET);
                    byteBuf.setLong(byteBuf.readerIndex() + OFFSET_OFFSET, startOffset);

                    // batch magic greater than the magic corresponding to the version requested by the client
                    // need down converted
                    if (batchMagic > magic || batchMagic != RecordBatch.MAGIC_VALUE_V2) {
                        MemoryRecords memoryRecords = MemoryRecords.readableRecords(ByteBufUtils.getNioBuffer(byteBuf));
                        //down converted, batch magic will be set to client magic
                        ConvertedRecords<MemoryRecords> convertedRecords =
                                KopRecordsUtil.convertAndAssignOffsets(memoryRecords.batches(), magic, startOffset);

                        final ByteBuf kafkaBuffer = Unpooled.wrappedBuffer(convertedRecords.records().buffer());
                        orderedByteBuf.add(kafkaBuffer);
                        if (!optionalByteBufs.isPresent()) {
                            optionalByteBufs = Optional.of(new ArrayList<>());
                        }
                        optionalByteBufs.ifPresent(byteBufs -> byteBufs.add(kafkaBuffer));

                        if (log.isTraceEnabled()) {
                            log.trace("[{}:{}] convertAndAssignOffsets record for down converted"
                                            + " or assign offsets with v0 and v1 magic, start offset {},"
                                            + " entry magic: {}, client magic: {}",
                                    entry.getLedgerId(), entry.getEntryId(), startOffset, batchMagic, magic);
                        }

                    } else {
                        //not need down converted, batch magic retains the magic value written in production
                        orderedByteBuf.add(byteBuf.slice(byteBuf.readerIndex(), byteBuf.readableBytes()));
                    }
                } else {
                    final DecodeResult decodeResult =
                            ByteBufUtils.decodePulsarEntryToKafkaRecords(metadata, byteBuf, startOffset, magic);
                    final ByteBuf kafkaBuffer = decodeResult.getOrCreateByteBuf();
                    orderedByteBuf.add(kafkaBuffer);
                    if (!optionalByteBufs.isPresent()) {
                        optionalByteBufs = Optional.of(new ArrayList<>());
                    }
                    optionalByteBufs.ifPresent(byteBufs -> byteBufs.add(kafkaBuffer));
                }
                // Almost all exceptions in Kafka inherit from KafkaException and will be captured
                // and processed in KafkaApis. Here, whether it is down-conversion or the IOException
                // in builder.appendWithOffset in decodePulsarEntryToKafkaRecords will be caught by Kafka
                // and the KafkaException will be thrown. So we need to catch KafkaException here.
            } catch (KoPMessageMetadataNotFoundException | IOException | KafkaException e) { // skip failed decode entry
                log.error("[{}:{}] Failed to decode entry. ", entry.getLedgerId(), entry.getEntryId(), e);
                entry.release();
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
}
