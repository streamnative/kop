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

import io.streamnative.pulsar.handlers.kop.utils.KopLogValidator;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for KafkaEntryFormatter.
 */
public class KafkaEntryFormatTest {

    @Test
    public void testCompressionType() {
        final MemoryRecords recordsNone = newMemoryRecordsBuilder(CompressionType.NONE);
        final MemoryRecords recordsGzip = newMemoryRecordsBuilder(CompressionType.GZIP);
        final MemoryRecords recordsSnappy = newMemoryRecordsBuilder(CompressionType.SNAPPY);
        final MemoryRecords recordsLz4 = newMemoryRecordsBuilder(CompressionType.LZ4);

        // test brokerCompressionType=producer
        final KafkaEntryFormatter producerEntryFormatter =
                new KafkaEntryFormatter("producer");
        Assert.assertEquals(CompressionType.NONE.name,
                checkRecordsCodec(producerEntryFormatter, recordsNone).name());
        Assert.assertEquals(CompressionType.GZIP.name,
                checkRecordsCodec(producerEntryFormatter, recordsGzip).name());
        Assert.assertEquals(CompressionType.SNAPPY.name,
                checkRecordsCodec(producerEntryFormatter, recordsSnappy).name());
        Assert.assertEquals(CompressionType.LZ4.name,
                checkRecordsCodec(producerEntryFormatter, recordsLz4).name());

        // test brokerCompressionType=gzip
        final KafkaEntryFormatter gzipEntryFormatter =
                new KafkaEntryFormatter("gzip");
        Assert.assertEquals(CompressionType.GZIP.name,
                checkRecordsCodec(gzipEntryFormatter, recordsNone).name());
        Assert.assertEquals(CompressionType.GZIP.name,
                checkRecordsCodec(gzipEntryFormatter, recordsGzip).name());
        Assert.assertEquals(CompressionType.GZIP.name,
                checkRecordsCodec(gzipEntryFormatter, recordsSnappy).name());
        Assert.assertEquals(CompressionType.GZIP.name,
                checkRecordsCodec(gzipEntryFormatter, recordsLz4).name());

        // test brokerCompressionType=snappy
        final KafkaEntryFormatter snappyEntryFormatter =
                new KafkaEntryFormatter("snappy");
        Assert.assertEquals(CompressionType.SNAPPY.name,
                checkRecordsCodec(snappyEntryFormatter, recordsNone).name());
        Assert.assertEquals(CompressionType.SNAPPY.name,
                checkRecordsCodec(snappyEntryFormatter, recordsGzip).name());
        Assert.assertEquals(CompressionType.SNAPPY.name,
                checkRecordsCodec(snappyEntryFormatter, recordsSnappy).name());
        Assert.assertEquals(CompressionType.SNAPPY.name,
                checkRecordsCodec(snappyEntryFormatter, recordsLz4).name());

        // test brokerCompressionType=lz4
        final KafkaEntryFormatter lz4EntryFormatter =
                new KafkaEntryFormatter("lz4");
        Assert.assertEquals(CompressionType.LZ4.name,
                checkRecordsCodec(lz4EntryFormatter, recordsNone).name());
        Assert.assertEquals(CompressionType.LZ4.name,
                checkRecordsCodec(lz4EntryFormatter, recordsGzip).name());
        Assert.assertEquals(CompressionType.LZ4.name,
                checkRecordsCodec(lz4EntryFormatter, recordsSnappy).name());
        Assert.assertEquals(CompressionType.LZ4.name,
                checkRecordsCodec(lz4EntryFormatter, recordsLz4).name());
    }

    private static KopLogValidator.CompressionCodec checkRecordsCodec(final KafkaEntryFormatter entryFormatter,
                                          final MemoryRecords records) {
        final KopLogValidator.CompressionCodec sourceCodec = entryFormatter.getSourceCodec(records);
        return entryFormatter.getTargetCodec(sourceCodec);
    }

    private static MemoryRecords newMemoryRecordsBuilder(final CompressionType type) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024 * 1024 * 5),
                RecordBatch.CURRENT_MAGIC_VALUE,
                type,
                TimestampType.CREATE_TIME,
                0L);

        for (int i = 0; i < 10; i++) {
            final byte[] value = new byte[10];
            Arrays.fill(value, (byte) 'a');
            builder.append(new SimpleRecord(System.currentTimeMillis(), "key".getBytes(), value));
        }
        return builder.build();
    }
}
