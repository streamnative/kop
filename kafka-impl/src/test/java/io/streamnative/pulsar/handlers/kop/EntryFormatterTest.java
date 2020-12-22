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

import static org.testng.Assert.assertEquals;

import io.streamnative.pulsar.handlers.kop.format.EntryFormatter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.IntStream;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for EntryFormatter.
 */
public class EntryFormatterTest {

    @DataProvider(name = "compressionTypes")
    Object[] allCompressionTypes() {
        return Arrays.stream(CompressionType.values()).map(x -> (Object) x).toArray();
    }

    @Test(dataProvider = "compressionTypes")
    public void testParseNumMessages(CompressionType compressionType) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int[] batchSizes = {3, 2, 5};

        int baseOffset = 0;
        for (int batchSize : batchSizes) {
            MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                    compressionType, TimestampType.LOG_APPEND_TIME, baseOffset);
            for (int i = 0; i < batchSize; i++) {
                builder.append(0L, "a".getBytes(), "1".getBytes());
            }
            baseOffset += batchSize;
            // Normally the offsets of batches are continuous, here we add an extra interval just for robustness.
            baseOffset += 1;
            builder.close();
        }

        buffer.flip();
        final MemoryRecords records = MemoryRecords.readableRecords(buffer);
        assertEquals(EntryFormatter.parseNumMessages(records), IntStream.of(batchSizes).sum());
    }
}
