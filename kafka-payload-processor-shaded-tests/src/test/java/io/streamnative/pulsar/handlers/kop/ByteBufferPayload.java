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
package io.streamnative.pulsar.handlers.kop;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.pulsar.client.api.MessagePayload;

public class ByteBufferPayload implements MessagePayload {

    private final ByteBuffer buffer;

    public ByteBufferPayload(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public byte[] copiedBuffer() {
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public static ByteBufferPayload newKafkaPayload(final List<String> values) {
        final MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024),
                RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0L);
        for (String value : values) {
            builder.append(System.currentTimeMillis(), null,
                    (value != null) ? value.getBytes(StandardCharsets.UTF_8) : null);
        }
        return new ByteBufferPayload(builder.build().buffer());
    }
}
