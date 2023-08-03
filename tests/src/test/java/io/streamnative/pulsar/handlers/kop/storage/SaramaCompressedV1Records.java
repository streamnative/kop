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
package io.streamnative.pulsar.handlers.kop.storage;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

/**
 * Simulate the behavior of Golang Sarama library for v0 and v1 message set with compression enabled.
 *
 * See https://github.com/IBM/sarama/blob/c10bd1e5709a7b47729b445bc98f2e41bc7cc0a8/produce_set.go#L181-L184 and
 * https://github.com/IBM/sarama/blob/c10bd1e5709a7b47729b445bc98f2e41bc7cc0a8/message.go#L82. The `Message.encode`
 * method does not set the last offset field.
 */
public class SaramaCompressedV1Records {

    // See https://github.com/IBM/sarama/blob/c10bd1e5709a7b47729b445bc98f2e41bc7cc0a8/async_producer.go#L446
    private static final byte MAGIC = RecordBatch.MAGIC_VALUE_V1;
    private static final TimestampType TIMESTAMP_TYPE = TimestampType.LOG_APPEND_TIME;
    private final ByteBufferOutputStream bufferStream = new ByteBufferOutputStream(ByteBuffer.allocate(1024 * 10));
    private final DataOutputStream appendStream;
    private final CompressionType compressionType;
    private long timestamp;

    public SaramaCompressedV1Records(CompressionType compressionType) {
        if (compressionType == CompressionType.NONE) {
            throw new IllegalArgumentException("CompressionType should not be NONE");
        }
        this.compressionType = compressionType;
        this.bufferStream.position(AbstractRecords.recordBatchHeaderSizeInBytes(MAGIC, compressionType));
        this.appendStream = new DataOutputStream(compressionType.wrapForOutput(this.bufferStream, MAGIC));
    }

    public void appendLegacyRecord(long offset, String value) throws IOException {
        // Only test null key and current timestamp
        timestamp = System.currentTimeMillis();
        int size = LegacyRecord.recordSize(MAGIC, 0, value.getBytes().length);
        appendStream.writeLong(offset);
        appendStream.writeInt(size);
        LegacyRecord.write(appendStream, MAGIC, timestamp, null, ByteBuffer.wrap(value.getBytes()),
                CompressionType.NONE, TimestampType.LOG_APPEND_TIME);
    }

    public MemoryRecords build() throws IOException {
        close();
        ByteBuffer buffer = bufferStream.buffer().duplicate();
        buffer.flip();
        buffer.position(0);
        return MemoryRecords.readableRecords(buffer.slice());
    }

    private void close() throws IOException {
        appendStream.close();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(0);

        // NOTE: This is the core difference between Sarama and Kafka official Java client. Sarama does not write the
        // last offset.
        buffer.putLong(0L);

        int wrapperSize = pos - Records.LOG_OVERHEAD;
        buffer.putInt(wrapperSize);
        LegacyRecord.writeCompressedRecordHeader(buffer, MAGIC, wrapperSize, timestamp, compressionType,
                TIMESTAMP_TYPE);
        buffer.position(pos);
    }
}
