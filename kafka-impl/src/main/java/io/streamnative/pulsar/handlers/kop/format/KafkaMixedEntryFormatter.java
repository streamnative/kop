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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.streamnative.pulsar.handlers.kop.utils.KopLogValidator;
import io.streamnative.pulsar.handlers.kop.utils.LongRef;
import java.util.List;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;

/**
 * The entry formatter that uses Kafka's mixed versions format.
 * This formatter support all kafka magic version records,
 * so call it Mixed entry formatter.
 */
@Slf4j
public class KafkaMixedEntryFormatter extends AbstractEntryFormatter {

    private final String brokerCompressionType;

    public KafkaMixedEntryFormatter(String brokerCompressionType) {
        this.brokerCompressionType = brokerCompressionType;
    }

    @Override
    public EncodeResult encode(final EncodeRequest encodeRequest) {
        final MemoryRecords records = encodeRequest.getRecords();
        final long baseOffset = encodeRequest.getBaseOffset();
        final LongRef offset = new LongRef(baseOffset);

        final KopLogValidator.CompressionCodec sourceCodec = getSourceCodec(records);
        final KopLogValidator.CompressionCodec targetCodec = getTargetCodec(sourceCodec);

        final ValidationAndOffsetAssignResult validationAndOffsetAssignResult =
                KopLogValidator.validateMessagesAndAssignOffsets(records,
                        offset,
                        System.currentTimeMillis(),
                        sourceCodec,
                        targetCodec,
                        false,
                        RecordBatch.MAGIC_VALUE_V2,
                        TimestampType.CREATE_TIME,
                        Long.MAX_VALUE);

        MemoryRecords validRecords = validationAndOffsetAssignResult.getRecords();
        int conversionCount = validationAndOffsetAssignResult.getConversionCount();

        final int numMessages = EntryFormatter.parseNumMessages(validRecords);
        final ByteBuf recordsWrapper = Unpooled.wrappedBuffer(validRecords.buffer());
        final ByteBuf buf = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.None,
                getMessageMetadataWithNumberMessages(numMessages),
                recordsWrapper);
        recordsWrapper.release();
        validationAndOffsetAssignResult.recycle();

        return EncodeResult.get(validRecords, buf, numMessages, conversionCount);
    }

    @Override
    public DecodeResult decode(List<Entry> entries, byte magic) {
        return super.decode(entries, magic);
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
            if (messageCodec.codec() != CompressionType.NONE.id) {
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
