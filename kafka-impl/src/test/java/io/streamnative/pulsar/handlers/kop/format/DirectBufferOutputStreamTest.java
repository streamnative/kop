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

import java.nio.ByteBuffer;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test for {@link DirectBufferOutputStream}.
 */
public class DirectBufferOutputStreamTest {

    private static final int WRITE_LIMIT = 100 * 1024 * 1024;
    private static final long LOG_APPEND_TIME = System.currentTimeMillis();

    @DataProvider
    public static Object[][] initialCapacityAndNumRecords() {
        return new Object[][]{
                { 1, 10 },
                { 1024 * 1024, 10 },
                { 1024 * 1024, 1024 },
        };
    }

    @Test(dataProvider = "initialCapacityAndNumRecords")
    public void testBuildMemoryRecords(int initialCapacity, int numRecords) {
        final MemoryRecordsBuilder heapMemoryRecordsBuilder =
                newMemoryRecordsBuilder(new ByteBufferOutputStream(initialCapacity));
        // We must expose the DirectBufferOutputStream because we need to release the internal ByteBuf later
        final DirectBufferOutputStream directBufferOutputStream = new DirectBufferOutputStream(initialCapacity);
        final MemoryRecordsBuilder directMemoryRecordsBuilder = newMemoryRecordsBuilder(directBufferOutputStream);
        final ByteBuffer valueBuffer = ByteBuffer.allocate(1024);

        for (int i = 0; i < numRecords; i++) {
            heapMemoryRecordsBuilder.appendWithOffset(i, LOG_APPEND_TIME + i, null, valueBuffer.duplicate());
            directMemoryRecordsBuilder.appendWithOffset(i, LOG_APPEND_TIME + i, null, valueBuffer.duplicate());
        }

        final ByteBuffer heapBuffer = heapMemoryRecordsBuilder.build().buffer();
        final ByteBuffer directBuffer = directMemoryRecordsBuilder.build().buffer();
        System.out.println("heapBuffer size: " + heapBuffer.limit() + ", directBuffer size: " + directBuffer.limit());
        Assert.assertEquals(heapBuffer, directBuffer);

        Assert.assertEquals(directBufferOutputStream.getByteBuf().refCnt(), 1);
        directBufferOutputStream.getByteBuf().release();
    }

    private static MemoryRecordsBuilder newMemoryRecordsBuilder(final ByteBufferOutputStream bufferStream) {
        return new MemoryRecordsBuilder(bufferStream,
                RecordBatch.MAGIC_VALUE_V2,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0,
                LOG_APPEND_TIME,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false,
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                WRITE_LIMIT);
    }
}
