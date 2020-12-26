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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.streamnative.pulsar.handlers.kop.utils.ByteBufUtils;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.apache.pulsar.common.protocol.Commands;


/**
 * The entry formatter that uses Kafka's format.
 */
public class KafkaEntryFormatter implements EntryFormatter {
    private final KafkaEntryFormatterHeader header = new KafkaEntryFormatterHeader();

    @Override
    public ByteBuf encode(MemoryRecords records, int numMessages) {
        return Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.None,
                header.getMessageMetadataWithNumberMessages(numMessages),
                Unpooled.wrappedBuffer(records.buffer())
        );
    }

    @Override
    public MemoryRecords decode(List<Entry> entries, byte magic) {
        int size = 0;
        for (Entry entry : entries) {
            size += entry.getLength();
        }
        final MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(size),
                magic,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                MessageIdUtils.peekBaseOffsetFromEntry(entries.get(0)));
        entries.forEach(entry -> {
            long startOffset = MessageIdUtils.peekBaseOffsetFromEntry(entry);
            final ByteBuf byteBuf = entry.getDataBuffer();
            Commands.skipMessageMetadata(byteBuf);
            final MemoryRecords records = MemoryRecords.readableRecords(ByteBufUtils.getNioBuffer(byteBuf));
            for (Record record : records.records()) {
                builder.appendWithOffset(startOffset, record);
                startOffset++;
            }
            entry.release();
        });
        return builder.build();
    }
}
