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
import io.streamnative.pulsar.handlers.kop.exceptions.MetadataCorruptedException;
import io.streamnative.pulsar.handlers.kop.utils.ByteBufUtils;
import io.streamnative.pulsar.handlers.kop.utils.MessageMetadataUtils;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.ConvertedRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;

@Slf4j
public abstract class AbstractEntryFormatter implements EntryFormatter {

    // These key-value identifies the entry's format as kafka
    public static final String IDENTITY_KEY = "entry.format";
    public static final String IDENTITY_VALUE = EntryFormatterFactory.EntryFormat.KAFKA.name().toLowerCase();
    private final Time time = Time.SYSTEM;

    @Override
    public DecodeResult decode(List<Entry> entries, byte magic) {
        int totalSize = 0;
        int conversionCount = 0;
        long conversionTimeNanos = 0L;
        // batched ByteBuf should be released after sending to client
        ByteBuf batchedByteBuf = PulsarByteBufAllocator.DEFAULT.directBuffer(totalSize);
        for (Entry entry : entries) {
            try {
                long startOffset = MessageMetadataUtils.peekBaseOffsetFromEntry(entry);
                final ByteBuf byteBuf = entry.getDataBuffer();
                final MessageMetadata metadata = MessageMetadataUtils.parseMessageMetadata(byteBuf);
                if (isKafkaEntryFormat(metadata)) {
                    byte batchMagic = byteBuf.getByte(byteBuf.readerIndex() + MAGIC_OFFSET);
                    byteBuf.setLong(byteBuf.readerIndex() + OFFSET_OFFSET, startOffset);

                    // batch magic greater than the magic corresponding to the version requested by the client
                    // need down converted
                    if (batchMagic > magic) {
                        long startConversionNanos = MathUtils.nowInNano();
                        MemoryRecords memoryRecords = MemoryRecords.readableRecords(ByteBufUtils.getNioBuffer(byteBuf));
                        // down converted, batch magic will be set to client magic
                        ConvertedRecords<MemoryRecords> convertedRecords =
                                memoryRecords.downConvert(magic, startOffset, time);
                        conversionCount += convertedRecords.recordConversionStats().numRecordsConverted();
                        conversionTimeNanos += MathUtils.elapsedNanos(startConversionNanos);

                        final ByteBuf kafkaBuffer = Unpooled.wrappedBuffer(convertedRecords.records().buffer());
                        totalSize += kafkaBuffer.readableBytes();
                        batchedByteBuf.writeBytes(kafkaBuffer);
                        kafkaBuffer.release();
                        if (log.isTraceEnabled()) {
                            log.trace("[{}:{}] MemoryRecords down converted, start offset {},"
                                            + " entry magic: {}, client magic: {}",
                                    entry.getLedgerId(), entry.getEntryId(), startOffset, batchMagic, magic);
                        }

                    } else {
                        // not need down converted, batch magic retains the magic value written in production
                        ByteBuf buf = byteBuf.slice(byteBuf.readerIndex(), byteBuf.readableBytes());
                        totalSize += buf.readableBytes();
                        batchedByteBuf.writeBytes(buf);
                    }
                } else {
                    final DecodeResult decodeResult =
                            ByteBufUtils.decodePulsarEntryToKafkaRecords(metadata, byteBuf, startOffset, magic);
                    conversionCount += decodeResult.getConversionCount();
                    conversionTimeNanos += decodeResult.getConversionTimeNanos();
                    final ByteBuf kafkaBuffer = decodeResult.getOrCreateByteBuf();
                    totalSize += kafkaBuffer.readableBytes();
                    batchedByteBuf.writeBytes(kafkaBuffer);
                    decodeResult.recycle();
                }

                // Almost all exceptions in Kafka inherit from KafkaException and will be captured
                // and processed in KafkaApis. Here, whether it is down-conversion or the IOException
                // in builder.appendWithOffset in decodePulsarEntryToKafkaRecords will be caught by Kafka
                // and the KafkaException will be thrown. So we need to catch KafkaException here.
            } catch (MetadataCorruptedException | IOException | KafkaException e) { // skip failed decode entry
                log.error("[{}:{}] Failed to decode entry. ", entry.getLedgerId(), entry.getEntryId(), e);
            } finally {
                entry.release();
            }
        }

        return DecodeResult.get(
                MemoryRecords.readableRecords(ByteBufUtils.getNioBuffer(batchedByteBuf)),
                batchedByteBuf,
                conversionCount,
                conversionTimeNanos);
    }

    protected static boolean isKafkaEntryFormat(final MessageMetadata messageMetadata) {
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
