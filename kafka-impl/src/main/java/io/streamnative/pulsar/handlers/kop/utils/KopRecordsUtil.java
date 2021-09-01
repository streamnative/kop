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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.ConvertedRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;

/**
 * Utils for DownConverted and ReAssignOffset operations.
 */
@Slf4j
public class KopRecordsUtil {

    public static ConvertedRecords<MemoryRecords> convertAndAssignOffsets(Iterable<? extends RecordBatch> batches,
                                                              byte toMagic,
                                                              long firstOffset) throws IOException {
        // maintain the batch along with the decompressed records to avoid the need to decompress again
        List<RecordBatchAndRecords> recordBatchAndRecordsList = new ArrayList<>();
        int totalSizeEstimate = 0;

        long batchStartOffset = firstOffset;
        for (RecordBatch batch : batches) {
            byte toBatchMagic = toMagic;
            if (toMagic < RecordBatch.MAGIC_VALUE_V2) {
                if (batch.isControlBatch()) {
                    continue;
                }

                if (batch.compressionType().name.equals("zstd")) {
                    throw new IOException("Down-conversion of zstandard-compressed batches "
                            + "is not supported");
                }
            }

            List<Record> records = new ArrayList<>();
            long batchIndex = 0;
            for (Record record : batch) {
                records.add(record);
                batchIndex++;
            }

            if (records.isEmpty()) {
                continue;
            }

            // handle the batch.magic() <= toMagic case
            // Since the internal message set of magic 0 and magic 1 has an offset,
            // the internal offset may be destroyed, so we still need
            // to deal with the message of batch.magic <= toMagic.
            // The only thing that remains unchanged is to ensure that the magic remains unchanged.
            if (batch.magic() < toMagic) {
                toBatchMagic = batch.magic();
            }

            totalSizeEstimate += AbstractRecords.estimateSizeInBytes(
                    toBatchMagic, batchStartOffset, batch.compressionType(), records);
            recordBatchAndRecordsList.add(new RecordBatchAndRecords(batch, records, batchStartOffset, toBatchMagic));
            batchStartOffset += batchIndex;
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSizeEstimate);
        for (RecordBatchAndRecords recordBatchAndRecords : recordBatchAndRecordsList) {
            MemoryRecordsBuilder builder = convertRecordBatch(buffer, recordBatchAndRecords);
            buffer = builder.buffer();
        }

        buffer.flip();
        recordBatchAndRecordsList.clear();
        return new ConvertedRecords<>(MemoryRecords.readableRecords(buffer), null);
    }

    private static MemoryRecordsBuilder convertRecordBatch(ByteBuffer buffer,
                                                           RecordBatchAndRecords recordBatchAndRecords) {
        RecordBatch batch = recordBatchAndRecords.batch;
        byte toBatchMagic = recordBatchAndRecords.toBatchMagic;
        final TimestampType timestampType = batch.timestampType();
        long logAppendTime = timestampType
                == TimestampType.LOG_APPEND_TIME ? batch.maxTimestamp() : RecordBatch.NO_TIMESTAMP;

        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, toBatchMagic, batch.compressionType(),
                timestampType, recordBatchAndRecords.baseOffset, logAppendTime);

        long startOffset = recordBatchAndRecords.baseOffset;
        for (Record record : recordBatchAndRecords.records) {
            if (toBatchMagic > RecordBatch.MAGIC_VALUE_V1) {
                builder.appendWithOffset(startOffset++,
                        record.timestamp(),
                        record.key(),
                        record.value(),
                        record.headers());
            } else {
                builder.appendWithOffset(startOffset++,
                        record.timestamp(),
                        record.key(),
                        record.value());
            }
        }

        builder.close();
        return builder;
    }

    private static class RecordBatchAndRecords {
        private final RecordBatch batch;
        private final List<Record> records;
        private final Long baseOffset;
        private final byte toBatchMagic;

        private RecordBatchAndRecords(RecordBatch batch,
                                      List<Record> records,
                                      Long baseOffset,
                                      byte toBatchMagic) {
            this.batch = batch;
            this.records = records;
            this.baseOffset = baseOffset;
            this.toBatchMagic = toBatchMagic;
        }
    }
}
