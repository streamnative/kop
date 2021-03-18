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
import io.streamnative.pulsar.handlers.kop.utils.ByteBufUtils;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

/**
 * The entry formatter that uses Kafka's format.
 */
public class KafkaEntryFormatter implements EntryFormatter {

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
    public MemoryRecords decode(List<Entry> entries, byte magic) {
        // TODO The memory records should maintain multiple batches, one entry present one batch,
        //  this is necessary, because one entry only belongs to one transaction.
        List<MemoryRecords> orderedRecord = entries.stream().parallel().map(entry -> {
            long startOffset = MessageIdUtils.peekBaseOffsetFromEntry(entry);
            final ByteBuf byteBuf = entry.getDataBuffer();
            Commands.skipMessageMetadata(byteBuf);
            final MemoryRecords records = MemoryRecords.readableRecords(ByteBufUtils.getNioBuffer(byteBuf));

            int p = records.buffer().position();
            records.buffer().putLong(p + OFFSET_OFFSET, startOffset);
            records.buffer().put(p + MAGIC_OFFSET, magic);

            return records;
        }).collect(Collectors.toList());

        // Concatenate multiple batch into one single MemoryRecords object
        // In this mode, batch and entry have a one-to-one correspondence
        int totalSize = orderedRecord.stream().mapToInt(MemoryRecords::sizeInBytes).sum();
        ByteBuffer batchedBuffer = ByteBuffer.allocate(totalSize);
        for (MemoryRecords records : orderedRecord) {
            batchedBuffer.put(records.buffer());
        }
        batchedBuffer.flip();

        MemoryRecords batchedMemoryRecords = MemoryRecords.readableRecords(batchedBuffer);
        entries.forEach(Entry::release);
        return batchedMemoryRecords;
    }

    private static MessageMetadata getMessageMetadataWithNumberMessages(int numMessages) {
        final MessageMetadata metadata = new MessageMetadata();
        metadata.addProperty()
                .setKey("entry.format")
                .setValue(EntryFormatterFactory.EntryFormat.KAFKA.name().toLowerCase());
        metadata.setProducerName("");
        metadata.setSequenceId(0L);
        metadata.setPublishTime(System.currentTimeMillis());
        metadata.setNumMessagesInBatch(numMessages);
        return metadata;
    }

}
