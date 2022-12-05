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

import static org.mockito.Mockito.mock;

import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.KafkaTopicLookupService;
import java.nio.ByteBuffer;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.EndTransactionMarker;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test {@link PartitionLog}.
 */
@Slf4j
public class PartitionLogTest {

    private static final KafkaServiceConfiguration kafkaConfig = new KafkaServiceConfiguration();

    private static final PartitionLog PARTITION_LOG = new PartitionLog(
            kafkaConfig,
            null,
            Time.SYSTEM,
            new TopicPartition("test", 1),
            "test",
            null,
            mock(KafkaTopicLookupService.class),
            new MemoryProducerStateManagerSnapshotBuffer());

    @DataProvider(name = "compressionTypes")
    Object[] allCompressionTypes() {
        return Arrays.stream(CompressionType.values()).map(x -> (Object) x).toArray();
    }

    @Test(dataProvider = "compressionTypes")
    public void testAnalyzeAndValidateRecords(CompressionType compressionType) {
        MemoryRecords memoryRecords = buildMemoryRecords(new int[]{3, 2, 5}, compressionType, 100);
        PartitionLog.LogAppendInfo appendInfo =
                PARTITION_LOG.analyzeAndValidateRecords(memoryRecords);
        Assert.assertEquals(appendInfo.shallowCount(), 3);
        Assert.assertEquals(appendInfo.numMessages(), 10);
        Assert.assertEquals(appendInfo.validBytes(), memoryRecords.validBytes());
        Assert.assertEquals(appendInfo.sourceCodec().codec(), compressionType.id);
        Assert.assertEquals(appendInfo.targetCodec().codec(), compressionType.id);
        Assert.assertTrue(appendInfo.firstOffset().isPresent());
        Assert.assertEquals(appendInfo.firstOffset().get().longValue(), 0);
        Assert.assertFalse(appendInfo.isTransaction());
    }

    @Test
    public void testAnalyzeAndValidateEmptyRecords() {
        MemoryRecords memoryRecords = buildMemoryRecords(new int[]{}, CompressionType.NONE, 0);
        PartitionLog.LogAppendInfo appendInfo =
                PARTITION_LOG.analyzeAndValidateRecords(memoryRecords);
        Assert.assertEquals(appendInfo.shallowCount(), 0);
        Assert.assertEquals(appendInfo.numMessages(), 0);
        Assert.assertEquals(appendInfo.validBytes(), memoryRecords.validBytes());
        Assert.assertEquals(appendInfo.sourceCodec().codec(), CompressionType.NONE.id);
        Assert.assertEquals(appendInfo.targetCodec().codec(), CompressionType.NONE.id);
        Assert.assertFalse(appendInfo.firstOffset().isPresent());
        Assert.assertFalse(appendInfo.isTransaction());
    }

    @Test(expectedExceptions = RecordTooLargeException.class)
    public void testAnalyzeAndValidateTooLargeRecords() {
        MemoryRecords memoryRecords =
                buildMemoryRecords(new int[]{1}, CompressionType.NONE, kafkaConfig.getMaxMessageSize() + 1);
        PARTITION_LOG.analyzeAndValidateRecords(memoryRecords);
    }

    @Test
    public void testAnalyzeAndValidateControlRecords() {
        EndTransactionMarker marker = new EndTransactionMarker(ControlRecordType.ABORT, 0);
        MemoryRecords memoryRecords = MemoryRecords.withEndTransactionMarker(0, (short) 0, marker);
        PartitionLog.LogAppendInfo appendInfo =
                PARTITION_LOG.analyzeAndValidateRecords(memoryRecords);
        Assert.assertEquals(appendInfo.shallowCount(), 1);
        Assert.assertEquals(appendInfo.numMessages(), 1);
        Assert.assertEquals(appendInfo.validBytes(), memoryRecords.validBytes());
        Assert.assertEquals(appendInfo.sourceCodec().codec(), CompressionType.NONE.id);
        Assert.assertEquals(appendInfo.targetCodec().codec(), CompressionType.NONE.id);
        Assert.assertTrue(appendInfo.firstOffset().isPresent());
        Assert.assertEquals(appendInfo.firstOffset().get().longValue(), 0);
        Assert.assertTrue(appendInfo.isTransaction());
    }

    @Test
    public void testAnalyzeAndValidateIdempotentRecords() {
        MemoryRecords memoryRecords = buildIdempotentRecords(new int[]{3, 2, 5});
        PartitionLog.LogAppendInfo appendInfo =
                PARTITION_LOG.analyzeAndValidateRecords(memoryRecords);
        Assert.assertEquals(appendInfo.shallowCount(), 3);
        Assert.assertEquals(appendInfo.numMessages(), 10);
        Assert.assertEquals(appendInfo.validBytes(), memoryRecords.validBytes());
        Assert.assertEquals(appendInfo.sourceCodec().codec(), CompressionType.NONE.id);
        Assert.assertEquals(appendInfo.targetCodec().codec(), CompressionType.NONE.id);
        Assert.assertTrue(appendInfo.firstOffset().isPresent());
        Assert.assertEquals(appendInfo.firstOffset().get().longValue(), 0);
        Assert.assertFalse(appendInfo.isTransaction());
    }

    private MemoryRecords buildMemoryRecords(int[] batchSizes, CompressionType compressionType, int valueSize) {
        int totalBatchSize = 0;
        for (int batchSize : batchSizes) {
            totalBatchSize += batchSize;
        }
        ByteBuffer buffer = ByteBuffer.allocate(valueSize * totalBatchSize * 2);
        int baseOffset = 0;
        for (int batchSize : batchSizes) {
            MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                    compressionType, TimestampType.LOG_APPEND_TIME, baseOffset);
            for (int i = 0; i < batchSize; i++) {
                builder.append(0L, "a".getBytes(), new byte[valueSize]);
            }
            builder.close();
        }

        buffer.flip();

        return MemoryRecords.readableRecords(buffer);
    }

    private MemoryRecords buildIdempotentRecords(int[] batchSizes) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        int baseOffset = 0;
        for (int batchSize : batchSizes) {
            MemoryRecordsBuilder builder = MemoryRecords.idempotentBuilder(buffer, CompressionType.NONE, baseOffset,
                    0, (short) 0, 0);
            for (int i = 0; i < batchSize; i++) {
                builder.append(0L, "a".getBytes(), "1".getBytes());
            }
            builder.close();
        }

        buffer.flip();

        return MemoryRecords.readableRecords(buffer);
    }
}