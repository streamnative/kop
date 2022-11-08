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

import static org.apache.kafka.common.record.LegacyRecord.RECORD_OVERHEAD_V0;
import static org.apache.kafka.common.record.LegacyRecord.RECORD_OVERHEAD_V1;
import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.storage.PartitionLog;
import io.streamnative.pulsar.handlers.kop.storage.ProducerStateManager;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.LegacyRecord;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Crc32C;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.EntryFilterWithClassLoader;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class EntryFormatterTest {

    private static final int NUM_MESSAGES = 10;
    private static final int MESSAGE_SIZE = 1024;
    private static final KafkaServiceConfiguration pulsarServiceConfiguration = new KafkaServiceConfiguration();
    private static final KafkaServiceConfiguration KafkaV1ServiceConfiguration = new KafkaServiceConfiguration();
    private static final KafkaServiceConfiguration kafkaMixedServiceConfiguration = new KafkaServiceConfiguration();
    private static EntryFormatter pulsarFormatter;
    private static EntryFormatter kafkaV1Formatter;
    private static EntryFormatter kafkaMixedFormatter;
    // Kafka's absolute message offset is allocated by the Kafka server,
    // so each batch of messages on the client always starts from 0.
    // If it starts from non-zero, KopLogValidator will think that
    // the internal offset of these messages is damaged and will reconstruct the message.
    // I found this problem after adding the message conversion metrics,
    // so here I changed it to 0.
    private static final long baseOffset = 0;

    private static final PartitionLog PARTITION_LOG = new PartitionLog(
            pulsarServiceConfiguration,
            null,
            Time.SYSTEM,
            new TopicPartition("test", 1),
            "test",
            null,
            new ProducerStateManager("test"));

    private void init() {
        pulsarServiceConfiguration.setEntryFormat("pulsar");
        KafkaV1ServiceConfiguration.setEntryFormat("kafka");
        kafkaMixedServiceConfiguration.setEntryFormat("mixed_kafka");
        pulsarFormatter = EntryFormatterFactory.create(pulsarServiceConfiguration, null);
        kafkaV1Formatter = EntryFormatterFactory.create(KafkaV1ServiceConfiguration, null);
        kafkaMixedFormatter = EntryFormatterFactory.create(kafkaMixedServiceConfiguration, null);
    }

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

    @DataProvider(name = "entryFormatters")
    public Object[] entryFormatters() {
        init();
        return new Object[] {
                pulsarFormatter,
                kafkaV1Formatter,
                kafkaMixedFormatter
        };
    }

    /**
     * 1. When magic=0, magic=0 has no concept of relative offset.
     * 2. When magic=1, simulate the internal offset is not set.
     * 3. When magic=2, magic=2 uses relative offset from the beginning of its birth,
     * so keep the correctness of magic=2 messages
     * 4. When compressionType = NONE, both magic=0 and magic=1 do not use relative offset.
     * @param compressionType
     * @param magic
     */
    @Test(dataProvider = "compressionTypesAndMagic")
    public void testEntryFormatterEncode(CompressionType compressionType, byte magic) {
        init();

        MemoryRecords records = prepareRecords(compressionType, magic);
        PartitionLog.LogAppendInfo appendInfo = PARTITION_LOG.analyzeAndValidateRecords(records);
        // Verify the error offset generated by the simulation
        checkWrongOffset(records, compressionType, magic);

        EncodeResult encodeResult;
        // Verify that KafkaV1EntryFormatter cannot fix the wrong relative offset.
        encodeResult = kafkaV1Formatter.encode(EncodeRequest.get(records, appendInfo));
        Assert.assertEquals(0, encodeResult.getConversionCount());
        checkWrongOffset(encodeResult.getRecords(), compressionType, magic);

        // Verify that PulsarEntryFormatter cannot fix the wrong relative offset.
        encodeResult = pulsarFormatter.encode(EncodeRequest.get(records, appendInfo));
        Assert.assertEquals(NUM_MESSAGES, encodeResult.getConversionCount());
        checkWrongOffset(encodeResult.getRecords(), compressionType, magic);

        // Verify that KafkaMixedEntryFormatter can fix incorrect relative offset.
        encodeResult = kafkaMixedFormatter.encode(EncodeRequest.get(records, appendInfo));
        if (magic == RecordBatch.MAGIC_VALUE_V2) {
            // After changing baseOffset to 0,
            // KafkaMixedFormatter will not reconstruct the message with magic=2,
            // so the conversion count here is always 0.
            Assert.assertEquals(0, encodeResult.getConversionCount());
        } else {
            Assert.assertEquals(NUM_MESSAGES, encodeResult.getConversionCount());
        }
        checkCorrectOffset(encodeResult.getRecords());

    }
    @Test(dataProvider = "entryFormatters")
    public void testEntryFormatterDecode(AbstractEntryFormatter entryFormatter) {
        //Mock entries
        Entry entry1 = mock(Entry.class);
        Entry entry2 = mock(Entry.class);

        // Filter the entries
        ImmutableMap.Builder<String, EntryFilterWithClassLoader> builder = ImmutableMap.builder();
        EntryFilter mockEntryFilter = new EntryFilter() {
            @Override
            public FilterResult filterEntry(Entry entry, FilterContext context) {
                MessageMetadata msgMetadata = context.getMsgMetadata();
                // Filter with replicatedFrom in MessageMetadata
                if (msgMetadata != null && msgMetadata.hasReplicatedFrom()) {
                    return EntryFilter.FilterResult.REJECT;
                }
                return EntryFilter.FilterResult.ACCEPT;
            }

            @Override
            public void close() {
                // Ignore
            }
        };
        builder.put("mockEntryFilter", new EntryFilterWithClassLoader(mockEntryFilter, null));
        Assert.assertEquals(
                entryFormatter.filterOnlyByMsgMetadata(new MessageMetadata().setReplicatedFrom("cluster-1"),
                        entry1,
                        builder.build().values().asList()), EntryFilter.FilterResult.REJECT);
        Assert.assertEquals(
                entryFormatter.filterOnlyByMsgMetadata(new MessageMetadata(),
                        entry2,
                        builder.build().values().asList()), EntryFilter.FilterResult.ACCEPT);
    }

    private static void checkWrongOffset(MemoryRecords records,
                                         CompressionType compressionType,
                                         byte magic) {
        AtomicLong lastOffset = new AtomicLong();
        records.batches().forEach(batch -> {
            Iterator<Record> recordIterator = batch.iterator();
            // skip first record for firstOffset
            Record firstRecord = recordIterator.next();
            lastOffset.set(firstRecord.offset());
            while (recordIterator.hasNext()) {
                Record record = recordIterator.next();
                // Corresponding to 1/3/4 in the method comments,
                // the offset maintains the correctness of monotonically increasing.
                if (compressionType == CompressionType.NONE
                        || magic == RecordBatch.MAGIC_VALUE_V0
                        || magic == RecordBatch.MAGIC_VALUE_V2) {
                    Assert.assertTrue(lastOffset.get() < record.offset());
                } else {
                    // Corresponding to the case two in the method comments
                    Assert.assertEquals(record.offset(), lastOffset.get());
                }
                lastOffset.set(record.offset());
            }
        });
    }

    private static void checkCorrectOffset(MemoryRecords records) {
        AtomicLong lastOffset = new AtomicLong();
        records.batches().forEach(batch -> {
            Iterator<Record> recordIterator = batch.iterator();
            // skip first record for firstOffset
            Record firstRecord = recordIterator.next();
            lastOffset.set(firstRecord.offset());
            while (recordIterator.hasNext()) {
                Record record = recordIterator.next();
                // KafkaMixedEntryFormatter will fix all wrong relative offsets,
                // So no matter what the situation, we will always get the correct message offset.
                Assert.assertTrue(lastOffset.get() < record.offset());
                lastOffset.set(record.offset());
            }
        });
    }

    private static MineMemoryRecordsBuilder newMemoryRecordsBuilder(CompressionType compressionType,
                                                                    byte magic) {
        return new MineMemoryRecordsBuilder(
                new ByteBufferOutputStream(ByteBuffer.allocate(1024 * 1024 * 5)),
                magic,
                compressionType,
                baseOffset);
    }

    private static MemoryRecords prepareRecords(CompressionType compressionType,
                                                byte magic) {
        MineMemoryRecordsBuilder builder = newMemoryRecordsBuilder(compressionType, magic);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            final byte[] value = new byte[MESSAGE_SIZE];
            Arrays.fill(value, (byte) 'a');
            builder.appendWithOffset(baseOffset + i,
                    new SimpleRecord(System.currentTimeMillis(), "key".getBytes(), value));
        }
        return builder.build();
    }

    static class MineMemoryRecordsBuilder extends MemoryRecordsBuilder {
        private static final int BASE_OFFSET_OFFSET = 0;
        private static final int BASE_OFFSET_LENGTH = 8;
        private static final int LENGTH_OFFSET = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
        private static final int LENGTH_LENGTH = 4;
        private static final int PARTITION_LEADER_EPOCH_OFFSET = LENGTH_OFFSET + LENGTH_LENGTH;
        private static final int PARTITION_LEADER_EPOCH_LENGTH = 4;
        private static final int MAGIC_OFFSET = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
        private static final int MAGIC_LENGTH = 1;
        private static final int CRC_OFFSET = MAGIC_OFFSET + MAGIC_LENGTH;
        private static final int CRC_LENGTH = 4;
        private static final int ATTRIBUTES_OFFSET = CRC_OFFSET + CRC_LENGTH;
        private static final int ATTRIBUTE_LENGTH = 2;
        private static final int LAST_OFFSET_DELTA_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
        private static final int LAST_OFFSET_DELTA_LENGTH = 4;
        private static final int FIRST_TIMESTAMP_OFFSET = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
        private static final int FIRST_TIMESTAMP_LENGTH = 8;
        private static final int MAX_TIMESTAMP_OFFSET = FIRST_TIMESTAMP_OFFSET + FIRST_TIMESTAMP_LENGTH;
        private static final int MAX_TIMESTAMP_LENGTH = 8;
        private static final int PRODUCER_ID_OFFSET = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
        private static final int PRODUCER_ID_LENGTH = 8;
        private static final int PRODUCER_EPOCH_OFFSET = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH;
        private static final int PRODUCER_EPOCH_LENGTH = 2;
        private static final int BASE_SEQUENCE_OFFSET = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH;
        private static final int BASE_SEQUENCE_LENGTH = 4;
        private static final int RECORDS_COUNT_OFFSET = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
        private static final int RECORDS_COUNT_LENGTH = 4;
        private static final int RECORDS_OFFSET = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
        private static final int RECORD_BATCH_OVERHEAD = RECORDS_OFFSET;

        private static final byte COMPRESSION_CODEC_MASK = 0x07;
        private static final byte TRANSACTIONAL_FLAG_MASK = 0x10;
        private static final int CONTROL_FLAG_MASK = 0x20;
        private static final byte TIMESTAMP_TYPE_MASK = 0x08;

        private static final DataOutputStream CLOSED_STREAM = new DataOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IllegalStateException("MemoryRecordsBuilder is closed for record appends");
            }
        });
        private final ByteBufferOutputStream bufferStream;
        private DataOutputStream appendStream;
        private final byte magic;
        private final CompressionType compressionType;
        private final long baseOffset;
        private Long firstTimestamp = null;
        private final TimestampType timestampType;
        private final int initialPosition;
        private final int batchHeaderSizeInBytes;
        private Long lastOffset = null;
        private int numRecords = 0;

        public MineMemoryRecordsBuilder(ByteBufferOutputStream bufferStream,
                                        byte magic,
                                        CompressionType compressionType,
                                        long baseOffset) {
            super(bufferStream, magic, compressionType, TimestampType.CREATE_TIME, baseOffset,
                    RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH,
                    RecordBatch.NO_SEQUENCE, false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH,
                    bufferStream.remaining());

            this.magic = magic;
            this.compressionType = compressionType;
            this.baseOffset = baseOffset;
            this.timestampType = TimestampType.CREATE_TIME;
            this.initialPosition = bufferStream.position();
            this.batchHeaderSizeInBytes = AbstractRecords.recordBatchHeaderSizeInBytes(magic, compressionType);

            bufferStream.position(initialPosition + batchHeaderSizeInBytes);
            this.bufferStream = bufferStream;
            this.appendStream = new DataOutputStream(compressionType.wrapForOutput(this.bufferStream, magic));
        }

        @Override
        public Long appendWithOffset(long offset, SimpleRecord record) {
            return appendWithOffset(offset,
                    record.timestamp(),
                    record.key(),
                    record.value(),
                    record.headers());
        }

        public Long appendWithOffset(long offset,
                                     long timestamp,
                                     ByteBuffer key,
                                     ByteBuffer value,
                                     Header[] headers) {
            try {
                if (firstTimestamp == null) {
                    firstTimestamp = timestamp;
                }

                if (magic > RecordBatch.MAGIC_VALUE_V1) {
                    appendDefaultRecord(offset, timestamp, key, value, headers);
                    return null;
                } else {
                    return appendLegacyRecord(offset, timestamp, key, value);
                }
            } catch (IOException e) {
                throw new KafkaException("I/O exception when writing to the append stream, closing", e);
            }
        }

        private void appendDefaultRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value,
                                         Header[] headers) throws IOException {
            int offsetDelta = (int) (offset - baseOffset);
            long timestampDelta = timestamp - firstTimestamp;
            DefaultRecord.writeTo(appendStream, offsetDelta, timestampDelta, key, value, headers);
            lastOffset = offset;
            numRecords += 1;
        }

        private long appendLegacyRecord(long offset,
                                        long timestamp,
                                        ByteBuffer key,
                                        ByteBuffer value) throws IOException {
            int size = recordSize(magic, key, value);
            writeHeader(appendStream, toInnerOffset(offset), size);
            lastOffset = offset;
            numRecords += 1;
            return LegacyRecord.write(appendStream, magic, timestamp, key, value, CompressionType.NONE, timestampType);
        }

        private int recordSize(byte magic, ByteBuffer key, ByteBuffer value) {
            return recordSize(magic, key == null ? 0 : key.limit(), value == null ? 0 : value.limit());
        }

        private int recordSize(byte magic, int keySize, int valueSize) {
            return recordOverhead(magic) + keySize + valueSize;
        }

        private int recordOverhead(byte magic) {
            if (magic == 0) {
                return RECORD_OVERHEAD_V0;
            } else if (magic == 1) {
                return RECORD_OVERHEAD_V1;
            }

            throw new IllegalArgumentException("Invalid magic used in LegacyRecord: " + magic);
        }

        private void writeHeader(ByteBuffer buffer, long offset, int size) {
            buffer.putLong(offset);
            buffer.putInt(size);
        }

        private void writeHeader(DataOutputStream out, long offset, int size) throws IOException {
            out.writeLong(offset);
            out.writeInt(size);
        }

        private long toInnerOffset(long offset) {
            // Simulate the error that the relative offset offset is always 0
            if (magic > 0 && compressionType != CompressionType.NONE) {
                return 0;
            }
            return offset;
        }

        public void closeForRecordAppends() {
            if (appendStream != CLOSED_STREAM) {
                try {
                    appendStream.close();
                } catch (IOException e) {
                    throw new KafkaException(e);
                } finally {
                    appendStream = CLOSED_STREAM;
                }
            }
        }

        private int writeDefaultBatchHeader() {
            ByteBuffer buffer = bufferStream.buffer();
            int pos = buffer.position();
            buffer.position(initialPosition);
            int size = pos - initialPosition;
            int writtenCompressed = size - DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
            int offsetDelta = (int) (lastOffset - baseOffset);

            writeHeader(buffer, baseOffset, offsetDelta, size, magic,
                    compressionType, timestampType, firstTimestamp, numRecords);

            buffer.position(pos);
            return writtenCompressed;
        }

        private int writeLegacyCompressedWrapperHeader() {
            ByteBuffer buffer = bufferStream.buffer();
            int pos = buffer.position();
            buffer.position(initialPosition);

            int wrapperSize = pos - initialPosition - LOG_OVERHEAD;
            int writtenCompressed = wrapperSize - recordOverhead(magic);
            writeHeader(buffer, lastOffset, wrapperSize);

            long timestamp = RecordBatch.NO_TIMESTAMP;
            LegacyRecord.writeCompressedRecordHeader(buffer, magic, wrapperSize,
                    timestamp, compressionType, timestampType);

            buffer.position(pos);
            return writtenCompressed;
        }

        private void writeHeader(ByteBuffer buffer,
                                 long baseOffset,
                                 int lastOffsetDelta,
                                 int sizeInBytes,
                                 byte magic,
                                 CompressionType compressionType,
                                 TimestampType timestampType,
                                 long firstTimestamp,
                                 int numRecords) {
            if (magic < RecordBatch.CURRENT_MAGIC_VALUE) {
                throw new IllegalArgumentException("Invalid magic value " + magic);
            }
            if (firstTimestamp < 0 && firstTimestamp != RecordBatch.NO_TIMESTAMP) {
                throw new IllegalArgumentException("Invalid message timestamp " + firstTimestamp);
            }

            short attributes = computeAttributes(compressionType, timestampType, false, false);

            int position = buffer.position();
            buffer.putLong(position + BASE_OFFSET_OFFSET, baseOffset);
            buffer.putInt(position + LENGTH_OFFSET, sizeInBytes - LOG_OVERHEAD);
            buffer.putInt(position + PARTITION_LEADER_EPOCH_OFFSET, RecordBatch.NO_PARTITION_LEADER_EPOCH);
            buffer.put(position + MAGIC_OFFSET, magic);
            buffer.putShort(position + ATTRIBUTES_OFFSET, attributes);
            buffer.putLong(position + FIRST_TIMESTAMP_OFFSET, firstTimestamp);
            buffer.putLong(position + MAX_TIMESTAMP_OFFSET, RecordBatch.NO_TIMESTAMP);
            buffer.putInt(position + LAST_OFFSET_DELTA_OFFSET, lastOffsetDelta);
            buffer.putLong(position + PRODUCER_ID_OFFSET, RecordBatch.NO_PRODUCER_ID);
            buffer.putShort(position + PRODUCER_EPOCH_OFFSET, RecordBatch.NO_PRODUCER_EPOCH);
            buffer.putInt(position + BASE_SEQUENCE_OFFSET, RecordBatch.NO_SEQUENCE);
            buffer.putInt(position + RECORDS_COUNT_OFFSET, numRecords);
            long crc = Crc32C.compute(buffer, ATTRIBUTES_OFFSET, sizeInBytes - ATTRIBUTES_OFFSET);
            buffer.putInt(position + CRC_OFFSET, (int) crc);
            buffer.position(position + RECORD_BATCH_OVERHEAD);
        }

        private byte computeAttributes(CompressionType type, TimestampType timestampType,
                                       boolean isTransactional, boolean isControl) {
            if (timestampType == TimestampType.NO_TIMESTAMP_TYPE) {
                throw new IllegalArgumentException(
                        "Timestamp type must be provided to compute attributes for message "
                                + "format v2 and above");
            }

            byte attributes = isTransactional ? TRANSACTIONAL_FLAG_MASK : 0;
            if (isControl) {
                attributes |= CONTROL_FLAG_MASK;
            }
            if (type.id > 0) {
                attributes |= COMPRESSION_CODEC_MASK & type.id;
            }
            if (timestampType == TimestampType.LOG_APPEND_TIME) {
                attributes |= TIMESTAMP_TYPE_MASK;
            }
            return attributes;
        }

        public MemoryRecords build() {
            closeForRecordAppends();

            if (magic > RecordBatch.MAGIC_VALUE_V1) {
                writeDefaultBatchHeader();
            } else if (compressionType != CompressionType.NONE) {
                writeLegacyCompressedWrapperHeader();
            }

            ByteBuffer buffer = bufferStream.buffer().duplicate();
            buffer.flip();
            buffer.position(initialPosition);
            return MemoryRecords.readableRecords(buffer.slice());
        }

    }

}

