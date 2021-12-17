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

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.pulsar.handlers.kop.format.ValidationAndOffsetAssignResult;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Locale;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

public class KopLogValidator {

    /**
     * Update the offsets for this message set and do further validation on messages including:
     * 1. Messages for compacted topics must have keys
     * 2. When magic value >= 1, inner messages of a compressed message set must have monotonically increasing offsets
     *    starting from 0.
     * 3. When magic value >= 1, validate and maybe overwrite timestamps of messages.
     * 4. Declared count of records in DefaultRecordBatch must match number of valid records contained therein.
     *
     * This method will convert messages as necessary to the topic's configured message format version.
     * If no format conversion or value overwriting is required for messages,
     * this method will perform in-place operations to avoid expensive re-compression.
     *
     * Returns a ValidationAndOffsetAssignResult containing the validated message set, maximum timestamp,
     * the offset of the shallow message with the max timestamp and a boolean indicating
     * whether the message sizes may have changed.
     */
    public static ValidationAndOffsetAssignResult validateMessagesAndAssignOffsets(MemoryRecords records,
                                                                 LongRef offsetCounter,
                                                                 long now,
                                                                 CompressionCodec sourceCodec,
                                                                 CompressionCodec targetCodec,
                                                                 boolean compactedTopic,
                                                                 byte magic,
                                                                 TimestampType timestampType,
                                                                 long timestampDiffMaxMs) {
        if (sourceCodec.codec() == CompressionType.NONE.id
                && targetCodec.codec() == CompressionType.NONE.id) {
            // check the magic value
            if (!records.hasMatchingMagic(magic)) {
                return convertAndAssignOffsetsNonCompressed(records,
                        offsetCounter,
                        compactedTopic,
                        now,
                        timestampType,
                        timestampDiffMaxMs,
                        magic);
            } else {
                // Do in-place validation, offset assignment and maybe set timestamp
                return assignOffsetsNonCompressed(records,
                        offsetCounter,
                        now,
                        compactedTopic,
                        timestampType,
                        timestampDiffMaxMs,
                        magic);
            }
        } else {
            return validateMessagesAndAssignOffsetsCompressed(records,
                    offsetCounter,
                    now,
                    sourceCodec,
                    targetCodec,
                    compactedTopic,
                    magic,
                    timestampType,
                    timestampDiffMaxMs);
        }
    }

    private static ValidationAndOffsetAssignResult convertAndAssignOffsetsNonCompressed(MemoryRecords records,
                                                                                        LongRef offsetCounter,
                                                                                        boolean compactedTopic,
                                                                                        long now,
                                                                                        TimestampType timestampType,
                                                                                        long timestampDiffMaxMs,
                                                                                        byte toMagicValue) {
        int sizeInBytesAfterConversion = AbstractRecords.estimateSizeInBytes(toMagicValue, offsetCounter.value(),
                CompressionType.NONE, records.records());

        Iterator<MutableRecordBatch> batchIterator = records.batches().iterator();
        MutableRecordBatch first = batchIterator.next();

        long producerId = first.producerId();
        short producerEpoch = first.producerEpoch();
        int sequence = first.baseSequence();
        boolean isTransactional = first.isTransactional();

        ByteBuffer newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion);
        MemoryRecordsBuilder builder = MemoryRecords.builder(newBuffer,
                toMagicValue,
                CompressionType.NONE,
                timestampType,
                offsetCounter.value(),
                now,
                producerId,
                producerEpoch,
                sequence,
                isTransactional,
                RecordBatch.NO_PARTITION_LEADER_EPOCH);


        records.batches().forEach(batch -> {
            validateBatch(batch, toMagicValue);

            for (Record record : batch) {
                validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic);
                builder.appendWithOffset(offsetCounter.getAndIncrement(), record);
            }
        });

        MemoryRecords memoryRecords = builder.build();
        int conversionCount = builder.numRecords();

        return ValidationAndOffsetAssignResult.get(memoryRecords, conversionCount);
    }

    private static ValidationAndOffsetAssignResult assignOffsetsNonCompressed(MemoryRecords records,
                                                                              LongRef offsetCounter,
                                                                              long now,
                                                                              boolean compactedTopic,
                                                                              TimestampType timestampType,
                                                                              long timestampDiffMaxMs,
                                                                              byte magic) {
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        for (MutableRecordBatch batch : records.batches()) {
            validateBatch(batch, magic);

            long maxBatchTimestamp = RecordBatch.NO_TIMESTAMP;

            for (Record record : batch) {
                validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic);
                if (batch.magic() > RecordBatch.MAGIC_VALUE_V0 && record.timestamp() > maxBatchTimestamp) {
                    maxBatchTimestamp = record.timestamp();
                }
            }

            if (batch.magic() > RecordBatch.MAGIC_VALUE_V0 && maxBatchTimestamp > maxTimestamp) {
                maxTimestamp = maxBatchTimestamp;
            }

            batch.setLastOffset(offsetCounter.value() - 1);

            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                batch.setPartitionLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH);
            }

            if (batch.magic() > RecordBatch.MAGIC_VALUE_V0) {
                if (timestampType == TimestampType.LOG_APPEND_TIME) {
                    batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, now);
                } else {
                    batch.setMaxTimestamp(timestampType, maxBatchTimestamp);
                }
            }
        }

        return ValidationAndOffsetAssignResult.get(records, 0);
    }

    /**
     * We cannot do in place assignment in one of the following situations:
     * 1. Source and target compression codec are different
     * 2. When magic value to use is 0 because offsets need to be overwritten
     * 3. When magic value to use is above 0, but some fields of inner messages need to be overwritten.
     * 4. Message format conversion is needed.
     */
    private static ValidationAndOffsetAssignResult validateMessagesAndAssignOffsetsCompressed(
            MemoryRecords records,
            LongRef offsetCounter,
            long now,
            CompressionCodec sourceCodec,
            CompressionCodec targetCodec,
            boolean compactedTopic,
            byte toMagic,
            TimestampType timestampType,
            long timestampDiffMaxMs) {

        // No in place assignment situation 1 and 2
        boolean inPlaceAssignment = sourceCodec == targetCodec && toMagic > RecordBatch.MAGIC_VALUE_V0;

        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        LongRef expectedInnerOffset = new LongRef(0);
        ArrayList<Record> validatedRecords = new ArrayList<>();

        for (MutableRecordBatch batch : records.batches()) {
            validateBatch(batch, toMagic);

            // Do not compress control records unless they are written compressed
            if (sourceCodec.codec() == CompressionType.NONE.id && batch.isControlBatch()) {
                inPlaceAssignment = true;
            }

            for (Record record : batch) {
                validateRecord(batch, record, now, timestampType, timestampDiffMaxMs, compactedTopic);
                if (sourceCodec.codec() != CompressionType.NONE.id && record.isCompressed()) {
                    throw new InvalidRecordException(String.format(
                            "Compressed outer record should not have an inner record with a "
                            + "compression attribute set: %s", record));
                }

                if (batch.magic() > RecordBatch.MAGIC_VALUE_V0 && toMagic > RecordBatch.MAGIC_VALUE_V0) {
                    // Check if we need to overwrite offset
                    // No in place assignment situation 3
                    if (record.offset() != expectedInnerOffset.getAndIncrement()) {
                        inPlaceAssignment = false;
                    }
                    if (record.timestamp() > maxTimestamp) {
                        maxTimestamp = record.timestamp();
                    }
                }

                // No in place assignment situation 4
                if (!record.hasMagic(toMagic)) {
                    inPlaceAssignment = false;
                }

                validatedRecords.add(record);
            }
        }

        return buildIfPlaceAssignment(inPlaceAssignment,
                records,
                validatedRecords,
                offsetCounter,
                now,
                toMagic,
                timestampType,
                maxTimestamp,
                targetCodec);

    }

    private static ValidationAndOffsetAssignResult buildIfPlaceAssignment(boolean inPlaceAssignment,
                                                                          MemoryRecords records,
                                                                          ArrayList<Record> validatedRecords,
                                                                          LongRef offsetCounter,
                                                                          long now,
                                                                          byte toMagic,
                                                                          TimestampType timestampType,
                                                                          long maxTimestamp,
                                                                          CompressionCodec targetCodec) {
        if (inPlaceAssignment) {
            return buildInPlaceAssignment(records,
                    validatedRecords,
                    offsetCounter,
                    now,
                    toMagic,
                    timestampType,
                    maxTimestamp);
        } else {
            return buildNoInPlaceAssignment(records,
                    validatedRecords,
                    offsetCounter,
                    now,
                    targetCodec,
                    toMagic,
                    timestampType);
        }
    }

    private static ValidationAndOffsetAssignResult buildNoInPlaceAssignment(MemoryRecords records,
                                                                            ArrayList<Record> validatedRecords,
                                                                            LongRef offsetCounter,
                                                                            long now,
                                                                            CompressionCodec targetCodec,
                                                                            byte toMagic,
                                                                            TimestampType timestampType) {
        // note that we only reassign offsets for requests coming straight from a producer.
        // For records with magic V2, there should be exactly one RecordBatch per request,
        // so the following is all we need to do. For Records
        // with older magic versions, there will never be a producer id, etc.
        MutableRecordBatch first = records.batches().iterator().next();

        return buildRecordsAndAssignOffsets(toMagic,
                offsetCounter,
                timestampType,
                CompressionType.forId(targetCodec.codec()),
                now,
                validatedRecords,
                first);
    }

    private static ValidationAndOffsetAssignResult buildInPlaceAssignment(MemoryRecords records,
                                                                          ArrayList<Record> validatedRecords,
                                                                          LongRef offsetCounter,
                                                                          long now,
                                                                          byte toMagic,
                                                                          TimestampType timestampType,
                                                                          long maxTimestamp) {
        long currentMaxTimestamp = maxTimestamp;
        // we can update the batch only and write the compressed payload as is
        MutableRecordBatch batch = records.batches().iterator().next();
        long lastOffset = offsetCounter.addAndGet(validatedRecords.size()) - 1;

        batch.setLastOffset(lastOffset);

        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            currentMaxTimestamp = now;
        }

        if (toMagic >= RecordBatch.MAGIC_VALUE_V1) {
            batch.setMaxTimestamp(timestampType, currentMaxTimestamp);
        }

        if (toMagic >= RecordBatch.MAGIC_VALUE_V2) {
            batch.setPartitionLeaderEpoch(RecordBatch.NO_PARTITION_LEADER_EPOCH);
        }

        return ValidationAndOffsetAssignResult.get(records, 0);
    }

    private static ValidationAndOffsetAssignResult buildRecordsAndAssignOffsets(byte magic,
                                                                                LongRef offsetCounter,
                                                                                TimestampType timestampType,
                                                                                CompressionType compressionType,
                                                                                long logAppendTime,
                                                                                ArrayList<Record> validatedRecords,
                                                                                MutableRecordBatch first) {
        long producerId = first.producerId();
        short producerEpoch = first.producerEpoch();
        int baseSequence = first.baseSequence();
        boolean isTransactional = first.isTransactional();

        int estimatedSize = AbstractRecords.estimateSizeInBytes(magic, offsetCounter.value(), compressionType,
                validatedRecords);
        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer,
                magic,
                compressionType,
                timestampType,
                offsetCounter.value(),
                logAppendTime,
                producerId,
                producerEpoch,
                baseSequence,
                isTransactional,
                RecordBatch.NO_PARTITION_LEADER_EPOCH);

        validatedRecords.forEach(record -> {
            builder.appendWithOffset(offsetCounter.getAndIncrement(), record);
        });

        MemoryRecords memoryRecords = builder.build();
        int conversionCount = builder.numRecords();

        return ValidationAndOffsetAssignResult.get(memoryRecords, conversionCount);
    }

    private static void validateBatch(RecordBatch batch, byte toMagic) {
        if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
            long countFromOffsets = batch.lastOffset() - batch.baseOffset() + 1;
            if (countFromOffsets <= 0) {
                final String exceptionMsg = String.format(
                        "Batch has an invalid offset range: [%d, %d]", batch.baseOffset(), batch.lastOffset());
                throw new InvalidRecordException(exceptionMsg);
            }

            // v2 and above messages always have a non-null count
            int count = batch.countOrNull();
            if (count <= 0) {
                throw new InvalidRecordException(String.format(
                        "Invalid reported count for record batch: %d", count));
            }

            if (countFromOffsets != batch.countOrNull()) {
                final String exceptionMsg = String.format("Inconsistent batch offset range [%d, %d] "
                        + "and count of records %d", batch.baseOffset(), batch.lastOffset(), count);
                throw new InvalidRecordException(exceptionMsg);
            }
        }

        if (batch.hasProducerId() && batch.baseSequence() < 0) {
            final String exceptionMsg = String.format("Invalid sequence number %d in record batch "
                    + "with producerId %d", batch.baseSequence(), batch.producerId());
            throw new InvalidRecordException(exceptionMsg);
        }

        if (batch.isControlBatch()) {
            throw new InvalidRecordException("Clients are not allowed to write control records");
        }

        checkUnsupportedForMessageFormat(batch, toMagic);

    }

    private static void checkUnsupportedForMessageFormat(RecordBatch batch, byte toMagic) {
        if (batch.isTransactional() && toMagic < RecordBatch.MAGIC_VALUE_V2) {
            throw new UnsupportedForMessageFormatException(String.format(
                    "Transactional records cannot be used with magic version %s", toMagic));
        }

        if (batch.hasProducerId() && toMagic < RecordBatch.MAGIC_VALUE_V2) {
            throw new UnsupportedForMessageFormatException(String.format(
                    "Idempotent records cannot be used with magic version %s", toMagic));
        }
    }

    private static void validateRecord(RecordBatch batch, Record record, long now, TimestampType timestampType,
                                       long timestampDiffMaxMs, boolean compactedTopic) {
        if (!record.hasMagic(batch.magic())) {
            throw new InvalidRecordException(String.format(
                    "Log record magic does not match outer magic %s", batch.magic()));
        }

        // verify the record-level CRC only if this is one of the deep entries of a compressed message
        // set for magic v0 and v1. For non-compressed messages, there is no inner record for magic v0 and v1,
        // so we depend on the batch-level CRC check in Log.analyzeAndValidateRecords(). For magic v2 and above,
        // there is no record-level CRC to check.
        if (batch.magic() <= RecordBatch.MAGIC_VALUE_V1 && batch.isCompressed()) {
            record.ensureValid();
        }

        validateKey(record, compactedTopic);
        validateTimestamp(batch, record, now, timestampType, timestampDiffMaxMs);
    }

    private static void validateKey(Record record, boolean compactedTopic) {
        if (compactedTopic && !record.hasKey()) {
            throw new InvalidRecordException("Compacted topic cannot accept message without key.");
        }
    }

    /**
     * This method validates the timestamps of a message.
     * If the message is using create time, this method checks if it is within acceptable range.
     */
    private static void validateTimestamp(RecordBatch batch,
                                          Record record,
                                          long now,
                                          TimestampType timestampType,
                                          long timestampDiffMaxMs) {
        if (timestampType == TimestampType.CREATE_TIME
                && record.timestamp() != RecordBatch.NO_TIMESTAMP
                && Math.abs(record.timestamp() - now) > timestampDiffMaxMs) {
            final String exceptionMsg = String.format("Timestamp %d of message with offset %d is out of range. "
                            + "The timestamp should be within [%d, %d]",
                    record.timestamp(), record.offset(), now - timestampDiffMaxMs, now + timestampDiffMaxMs);
            throw new InvalidTimestampException(exceptionMsg);
        }
        if (batch.timestampType() == TimestampType.LOG_APPEND_TIME) {
            final String exceptionMsg = String.format("Invalid timestamp type in message %s. Producer should not set"
                   + " timestamp type to LogAppendTime.", record);
            throw new InvalidTimestampException(exceptionMsg);
        }
    }

    public static class CompressionCodec {
        private final String name;
        private final int codec;

        public CompressionCodec(String name, int codec) {
            this.name = name;
            this.codec = codec;
        }

        public String name() {
            return name;
        }

        public int codec() {
            return codec;
        }
    }

    @VisibleForTesting
    public static KopLogValidator.CompressionCodec getSourceCodec(MemoryRecords records) {
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
    public static KopLogValidator.CompressionCodec getTargetCodec(KopLogValidator.CompressionCodec sourceCodec,
                                                                  String brokerCompressionType) {
        String lowerCaseBrokerCompressionType = brokerCompressionType.toLowerCase(Locale.ROOT);
        if (lowerCaseBrokerCompressionType.equals(CompressionType.NONE.name)) {
            return sourceCodec;
        } else {
            CompressionType compressionType = CompressionType.forName(lowerCaseBrokerCompressionType);
            return new KopLogValidator.CompressionCodec(compressionType.name, compressionType.id);
        }
    }

}
