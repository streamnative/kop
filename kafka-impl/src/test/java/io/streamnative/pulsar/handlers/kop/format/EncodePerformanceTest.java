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
import java.util.Arrays;
import java.util.Random;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;


/**
 * The performance test for {@link EntryFormatter#encode(MemoryRecords, int)}.
 */
public class EncodePerformanceTest {

    private static final int NUM_MESSAGES = 2048;
    private static final int MESSAGE_SIZE = 1024;

    public static void main(String[] args) {
        // The first time to run PulsarEntryFormatter a warn log will be printed that could take a lot of time.
        runSingleTest(prepareFixedRecords(), "fixed records", 1);

        runSingleTest(prepareFixedRecords(), "fixed records", 100);
        runSingleTest(prepareRandomRecords(), "random records", 100);

        runSingleTest(prepareFixedRecords(), "fixed records", 1000);
        runSingleTest(prepareRandomRecords(), "random records", 1000);
    }

    private static void runSingleTest(final MemoryRecords records, final String description, final int repeatTimes) {
        final EntryFormatter pulsarFormatter = EntryFormatterFactory.create("pulsar");
        final EntryFormatter kafkaFormatter = EntryFormatterFactory.create("kafka");
        // Here we also add a comparison with NoHeaderKafkaEntryFormatter to measure the overhead of adding a header
        // and copy the ByteBuffer of MemoryRecords that are done by KafkaEntryFormatter.
        final EntryFormatter noHeaderKafkaFormatter = new NoHeaderKafkaEntryFormatter();

        System.out.println("--- " + description + " for " + repeatTimes + " times ---");

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < repeatTimes; i++) {
            pulsarFormatter.encode(records, NUM_MESSAGES).release();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("PulsarEntryFormatter encode time: " + (t2 - t1) + " ms");

        t1 = System.currentTimeMillis();
        for (int i = 0; i < repeatTimes; i++) {
            kafkaFormatter.encode(records, NUM_MESSAGES).release();
        }
        t2 = System.currentTimeMillis();
        System.out.println("KafkaEntryFormatter encode time: " + (t2 - t1) + " ms");

        t1 = System.currentTimeMillis();
        for (int i = 0; i < repeatTimes; i++) {
            noHeaderKafkaFormatter.encode(records, NUM_MESSAGES).release();
        }
        t2 = System.currentTimeMillis();
        System.out.println("NoHeaderKafkaEntryFormatter encode time: " + (t2 - t1) + " ms");
    }

    private static MemoryRecordsBuilder newMemoryRecordsBuilder() {
        return MemoryRecords.builder(
                ByteBuffer.allocate(1024 * 1024 * 5),
                RecordBatch.CURRENT_MAGIC_VALUE,
                CompressionType.NONE,
                TimestampType.CREATE_TIME,
                0L);
    }

    private static MemoryRecords prepareFixedRecords() {
        final MemoryRecordsBuilder builder = newMemoryRecordsBuilder();
        for (int i = 0; i < NUM_MESSAGES; i++) {
            final byte[] value = new byte[MESSAGE_SIZE];
            Arrays.fill(value, (byte) 'a');
            builder.append(new SimpleRecord(System.currentTimeMillis(), "key".getBytes(), value));
        }
        return builder.build();
    }

    private static MemoryRecords prepareRandomRecords() {
        final MemoryRecordsBuilder builder = newMemoryRecordsBuilder();
        final Random random = new Random();
        for (int i = 0; i < NUM_MESSAGES; i++) {
            final ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_SIZE);
            for (int j = 0; j < MESSAGE_SIZE / 4; j++) {
                buffer.putInt(random.nextInt());
            }
            builder.append(new SimpleRecord(System.currentTimeMillis(), "key".getBytes(), buffer.array()));
        }
        return builder.build();
    }
}
