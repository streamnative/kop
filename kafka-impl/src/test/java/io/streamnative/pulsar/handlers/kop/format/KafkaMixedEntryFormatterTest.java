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
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test for KafkaMixedEntryFormatter.
 */
@Slf4j
public class KafkaMixedEntryFormatterTest {

    @DataProvider(name = "compressionTypesAndMagic")
    public static Object[][] compressionTypesAndMagic() {
        return new Object[][] {
                {CompressionType.NONE, (byte) 0},
                {CompressionType.NONE, (byte) 1},
                {CompressionType.NONE, (byte) 2},
                {CompressionType.LZ4, (byte) 0},
                {CompressionType.LZ4, (byte) 1},
                {CompressionType.LZ4, (byte) 2},
                {CompressionType.GZIP, (byte) 0},
                {CompressionType.GZIP, (byte) 1},
                {CompressionType.GZIP, (byte) 2},
                {CompressionType.SNAPPY, (byte) 0},
                {CompressionType.SNAPPY, (byte) 1},
                {CompressionType.SNAPPY, (byte) 2},
        };
    }

    @Test(dataProvider = "compressionTypesAndMagic")
    public void testCompressionType(CompressionType compressionType, byte magic) {
        final MemoryRecords records = newMemoryRecordsBuilder(compressionType, magic);

        // test brokerCompressionType=producer
        Assert.assertEquals(compressionType.name,
                checkRecordsCodec("none", records).name());

        // test brokerCompressionType=gzip
        Assert.assertEquals(CompressionType.GZIP.name,
                checkRecordsCodec("gzip", records).name());

        // test brokerCompressionType=snappy
        Assert.assertEquals(CompressionType.SNAPPY.name,
                checkRecordsCodec("snappy", records).name());

        // test brokerCompressionType=lz4
        Assert.assertEquals(CompressionType.LZ4.name,
                checkRecordsCodec("lz4", records).name());
    }

    private KopLogValidator.CompressionCodec checkRecordsCodec(
            final String brokerCompressionType,
            final MemoryRecords records) {
        final KopLogValidator.CompressionCodec sourceCodec = KopLogValidator.getSourceCodec(records);
        return KopLogValidator.getTargetCodec(sourceCodec, brokerCompressionType);
    }

    private static MemoryRecords newMemoryRecordsBuilder(final CompressionType type, byte magic) {
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                ByteBuffer.allocate(1024 * 1024 * 5),
                magic,
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