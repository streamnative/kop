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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;


/**
 * The formatter for conversion between Kafka records and Bookie entries.
 */
public interface EntryFormatter {
    int MAX_BATCH_MESSAGE_NUM = 4096;
    int DEFAULT_FETCH_BUFFER_SIZE = 1024 * 1024;
    int MAX_RECORDS_BUFFER_SIZE = 100 * 1024 * 1024;

    /**
     * Encode Kafka records to a ByteBuf.
     *
     * @param records messages with Kafka's format
     * @return the ByteBuf of an entry that is to be written to Bookie
     */
    ByteBuf encode(final MemoryRecords records);

    /**
     * Decode a stream of entries to Kafka records.
     * It should be noted that this method is responsible for releasing the entries.
     *
     * @param entries the list of entries
     * @param magic the Kafka record batch's magic value
     * @return the Kafka records
     */
    MemoryRecords decode(final List<Entry> entries, final byte magic);

    /**
     * Get the number of messages from MemoryRecords.
     * Since MemoryRecords doesn't provide a way to get the number of messages. We need to iterate over the whole
     * MemoryRecords object. So we use a helper method to get the number of messages that can be passed to
     * {@link EntryFormatter#encode(MemoryRecords)} and metrics related methods as well.
     *
     * @param records messages with Kafka's format
     * @return the number of messages
     */
    static int parseNumMessages(final MemoryRecords records) {
        int numMessages = 0;
        for (MutableRecordBatch batch : records.batches()) {
            numMessages += (batch.lastOffset() - batch.baseOffset() + 1);
        }
        return numMessages;
    }

    /**
     * Split large batch records.
     *
     * @param records messages with Kafka's format
     * @param numMessages the number of messages
     * @return the list of split records
     */
    static List<MemoryRecords> splitLargeBatchRecords(MemoryRecords records, int numMessages) {
        List<MemoryRecords> splitResultList = new ArrayList<>();
        if (numMessages < MAX_BATCH_MESSAGE_NUM){
            splitResultList.add(records);
        } else {
            Map<Integer, MemoryRecordsBuilder> recordsHashMap = new HashMap<>();
            int splitTotalNum = numMessages / MAX_BATCH_MESSAGE_NUM;
            for (int i = 0; i <= splitTotalNum; i++) {
                recordsHashMap.put(i, null);
            }
            final AtomicInteger splitCounter = new AtomicInteger(1);
            records.records().forEach(record -> {
                int recordsNum = splitCounter.getAndIncrement();
                int chunkIndex = recordsNum / MAX_BATCH_MESSAGE_NUM;
                if (recordsHashMap.get(chunkIndex) == null) {
                    try (ByteBufferOutputStream outputStream = new ByteBufferOutputStream(DEFAULT_FETCH_BUFFER_SIZE)) {
                        MemoryRecordsBuilder builder = new MemoryRecordsBuilder(
                                outputStream, RecordBatch.CURRENT_MAGIC_VALUE,
                                org.apache.kafka.common.record.CompressionType.NONE,
                                TimestampType.CREATE_TIME,
                                record.offset(),
                                RecordBatch.NO_TIMESTAMP,
                                RecordBatch.NO_PRODUCER_ID,
                                RecordBatch.NO_PRODUCER_EPOCH,
                                RecordBatch.NO_SEQUENCE,
                                false, false,
                                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                                MAX_RECORDS_BUFFER_SIZE);
                        builder.append(record);
                        recordsHashMap.put(chunkIndex, builder);
                    } catch (IOException ioe){
                        throw new UncheckedIOException(ioe);
                    } catch (Exception e){
                        throw e;
                    }
                } else {
                    recordsHashMap.get(chunkIndex).append(record);
                }
            });
            recordsHashMap.forEach((key, value) -> {
                splitResultList.add(value.build());
            });
        }
        return splitResultList;
    }
}
