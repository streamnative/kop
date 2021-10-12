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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.streamnative.pulsar.handlers.kop.exceptions.KoPMessageMetadataNotFoundException;
import io.streamnative.pulsar.handlers.kop.utils.ByteBufUtils;
import io.streamnative.pulsar.handlers.kop.utils.KopLogValidator;
import io.streamnative.pulsar.handlers.kop.utils.LongRef;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ConvertedRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

/**
 * The entry formatter that uses Kafka's mixed versions format.
 * This formatter support all kafka magic version records,
 * so call it Mixed entry formatter.
 */
@Slf4j
public class MixedKafkaEntryFormatter extends AbstractEntryFormatter {

    private final String brokerCompressionType;
    private final Time time = Time.SYSTEM;

    public MixedKafkaEntryFormatter(String brokerCompressionType) {
        this.brokerCompressionType = brokerCompressionType;
    }

    @Override
    public EncodeResult encode(final EncodeRequest encodeRequest) {
        final MemoryRecords records = encodeRequest.getRecords();
        final long baseOffset = encodeRequest.getBaseOffset();
        final LongRef offset = new LongRef(baseOffset);

        final KopLogValidator.CompressionCodec sourceCodec = getSourceCodec(records);
        final KopLogValidator.CompressionCodec targetCodec = getTargetCodec(sourceCodec);

        final MemoryRecords validRecords = KopLogValidator.validateMessagesAndAssignOffsets(records,
                offset,
                System.currentTimeMillis(),
                sourceCodec,
                targetCodec,
                false,
                RecordBatch.MAGIC_VALUE_V2,
                TimestampType.CREATE_TIME,
                Long.MAX_VALUE);

        final int numMessages = EntryFormatter.parseNumMessages(validRecords);
        final ByteBuf recordsWrapper = Unpooled.wrappedBuffer(validRecords.buffer());
        final ByteBuf buf = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.None,
                getMessageMetadataWithNumberMessages(numMessages),
                recordsWrapper);
        recordsWrapper.release();

        return new EncodeResult(validRecords, buf, numMessages);
    }

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
                    byte batchMagic = byteBuf.getByte(byteBuf.readerIndex() + MAGIC_OFFSET);
                    byteBuf.setLong(byteBuf.readerIndex() + OFFSET_OFFSET, startOffset);

                    // batch magic greater than the magic corresponding to the version requested by the client
                    // need down converted
                    if (batchMagic > magic) {
                        MemoryRecords memoryRecords = MemoryRecords.readableRecords(ByteBufUtils.getNioBuffer(byteBuf));
                        // down converted, batch magic will be set to client magic
                        ConvertedRecords<MemoryRecords> convertedRecords =
                                memoryRecords.downConvert(magic, startOffset, time);

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

    @VisibleForTesting
    public KopLogValidator.CompressionCodec getSourceCodec(MemoryRecords records) {
        KopLogValidator.CompressionCodec sourceCodec = new KopLogValidator.CompressionCodec(
                CompressionType.NONE.name, CompressionType.NONE.id);
        for (RecordBatch batch : records.batches()) {
            CompressionType compressionType = CompressionType.forId(batch.compressionType().id);
            KopLogValidator.CompressionCodec messageCodec = new KopLogValidator.CompressionCodec(
                    compressionType.name, compressionType.id);
            if (!messageCodec.name().equals(CompressionType.NONE.name)) {
                sourceCodec = messageCodec;
            }
        }
        return sourceCodec;
    }

    @VisibleForTesting
    public KopLogValidator.CompressionCodec getTargetCodec(KopLogValidator.CompressionCodec sourceCodec) {
        String lowerCaseBrokerCompressionType = brokerCompressionType.toLowerCase(Locale.ROOT);
        if (lowerCaseBrokerCompressionType.equals(CompressionType.NONE.name)) {
            return sourceCodec;
        } else {
            CompressionType compressionType = CompressionType.forName(lowerCaseBrokerCompressionType);
            return new KopLogValidator.CompressionCodec(compressionType.name, compressionType.id);
        }
    }

}
