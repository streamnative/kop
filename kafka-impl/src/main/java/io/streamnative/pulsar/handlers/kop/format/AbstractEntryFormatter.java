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

import static org.apache.kafka.common.record.Records.OFFSET_OFFSET;

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.kop.exceptions.KoPMessageMetadataNotFoundException;
import io.streamnative.pulsar.handlers.kop.utils.ByteBufUtils;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

@Slf4j
public abstract class AbstractEntryFormatter implements EntryFormatter {

    // These key-value identifies the entry's format as kafka
    public static final String IDENTITY_KEY = "entry.format";
    public static final String IDENTITY_VALUE = EntryFormatterFactory.EntryFormat.KAFKA.name().toLowerCase();

    @Override
    public DecodeResult decode(List<Entry> entries, byte magic) {
        int totalSize = 0;
        // batched ByteBuf should be released after sending to client
        ByteBuf batchedByteBuf = PulsarByteBufAllocator.DEFAULT.directBuffer(totalSize);
        for (Entry entry : entries) {
            try {
                long startOffset = MessageIdUtils.peekBaseOffsetFromEntry(entry);
                final ByteBuf byteBuf = entry.getDataBuffer();
                final MessageMetadata metadata = Commands.parseMessageMetadata(byteBuf);
                if (isKafkaEntryFormat(metadata)) {
                    byteBuf.setLong(byteBuf.readerIndex() + OFFSET_OFFSET, startOffset);
                    ByteBuf buf = byteBuf.slice(byteBuf.readerIndex(), byteBuf.readableBytes());
                    totalSize += buf.readableBytes();
                    batchedByteBuf.writeBytes(buf);
                } else {
                    final DecodeResult decodeResult =
                            ByteBufUtils.decodePulsarEntryToKafkaRecords(metadata, byteBuf, startOffset, magic);
                    final ByteBuf kafkaBuffer = decodeResult.getOrCreateByteBuf();
                    totalSize += kafkaBuffer.readableBytes();
                    batchedByteBuf.writeBytes(kafkaBuffer);
                    kafkaBuffer.release();
                }

                // Almost all exceptions in Kafka inherit from KafkaException and will be captured
                // and processed in KafkaApis. Here, whether it is down-conversion or the IOException
                // in builder.appendWithOffset in decodePulsarEntryToKafkaRecords will be caught by Kafka
                // and the KafkaException will be thrown. So we need to catch KafkaException here.
            } catch (KoPMessageMetadataNotFoundException | IOException | KafkaException e) { // skip failed decode entry
                log.error("[{}:{}] Failed to decode entry. ", entry.getLedgerId(), entry.getEntryId(), e);
            } finally {
                entry.release();
            }
        }

        return new DecodeResult(
                MemoryRecords.readableRecords(ByteBufUtils.getNioBuffer(batchedByteBuf)), batchedByteBuf);
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
