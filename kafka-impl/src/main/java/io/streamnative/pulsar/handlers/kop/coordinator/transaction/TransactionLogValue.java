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
package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.RecordBatch;


/**
 * Transaction log value.
 */
@Data
@AllArgsConstructor
public class TransactionLogValue {

    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;

    private final long producerId;
    private final short producerEpoch;
    private final int transactionTimeoutMs;
    private final byte transactionStatus;
    private final List<PartitionsSchema> transactionPartitions;
    private final long transactionLastUpdateTimestampMs;
    private final long transactionStartTimestampMs;

    private static final String PRODUCER_ID_FIELD = "producer_id";
    private static final String PRODUCER_EPOCH_FIELD = "producer_epoch";
    private static final String TXN_TIMEOUT_MS_FIELD = "transaction_timeout_ms";
    private static final String TXN_STATUS_FIELD = "transaction_status";
    private static final String TXN_PARTITIONS_FIELD = "transaction_partitions";
    private static final String TXN_LAST_UPDATE_TIMESTAMP_FIELD = "transaction_last_update_timestamp_ms";
    private static final String TXN_START_TIMESTAMP_FIELD = "transaction_start_timestamp_ms";

    private static final Schema SCHEMA_0 =
            new Schema(
                    new Field(PRODUCER_ID_FIELD, Type.INT64, "Producer id in use by the transactional id"),
                    new Field(PRODUCER_EPOCH_FIELD, Type.INT16, "Epoch associated with the producer id"),
                    new Field(TXN_TIMEOUT_MS_FIELD, Type.INT32, "Transaction timeout in milliseconds"),
                    new Field(TXN_STATUS_FIELD, Type.INT8, "TransactionState the transaction is in"),
                    new Field(TXN_PARTITIONS_FIELD, ArrayOf.nullable(PartitionsSchema.SCHEMA_0),
                            "Set of partitions involved in the transaction"),
                    new Field(TXN_LAST_UPDATE_TIMESTAMP_FIELD, Type.INT64, "Time the transaction was last updated"),
                    new Field(TXN_START_TIMESTAMP_FIELD, Type.INT64, "Time the transaction was started")
            );

    private static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
    };

    public TransactionLogValue(TransactionMetadata.TxnTransitMetadata txnTransitMetadata) {
        if (txnTransitMetadata.getTxnState() == TransactionState.EMPTY
                && !txnTransitMetadata.getTopicPartitions().isEmpty()) {
            throw new IllegalStateException("Transaction is not expected to have any partitions since its state is "
                    + txnTransitMetadata.getTxnState() + ":" + txnTransitMetadata.toString());
        }

        this.producerId = txnTransitMetadata.getProducerId();
        this.producerEpoch = txnTransitMetadata.getProducerEpoch();
        this.transactionTimeoutMs = txnTransitMetadata.getTxnTimeoutMs();
        this.transactionStatus = txnTransitMetadata.getTxnState().getValue();
        this.transactionPartitions = txnTransitMetadata.getTopicPartitions().stream()
                .map(partition -> new PartitionsSchema(partition.topic(),
                        Lists.newArrayList(partition.partition())))
                .collect(Collectors.toList());
        this.transactionLastUpdateTimestampMs = txnTransitMetadata.getTxnLastUpdateTimestamp();
        this.transactionStartTimestampMs = txnTransitMetadata.getTxnStartTimestamp();
    }

    public static Schema getSchema(short schemaVersion) {
        return SCHEMAS[schemaVersion];
    }

    public ByteBuffer toByteBuffer() {
        return toByteBuffer(HIGHEST_SUPPORTED_VERSION);
    }

    public ByteBuffer toByteBuffer(short schemaVersion) {
        if (this.getTransactionStatus() == TransactionState.EMPTY.getValue() && !transactionPartitions.isEmpty()) {
            throw new IllegalStateException("Transaction is not expected to have any partitions since its state is "
                    + transactionStatus);
        }

        Struct struct = new Struct(getSchema(schemaVersion));
        struct.set(PRODUCER_ID_FIELD, producerId);
        struct.set(PRODUCER_EPOCH_FIELD, producerEpoch);
        struct.set(TXN_TIMEOUT_MS_FIELD, transactionTimeoutMs);
        struct.set(TXN_STATUS_FIELD, transactionStatus);
        struct.set(TXN_PARTITIONS_FIELD,
                transactionPartitions.stream().map(PartitionsSchema::toStruct).toArray());
        struct.set(TXN_LAST_UPDATE_TIMESTAMP_FIELD, transactionLastUpdateTimestampMs);
        struct.set(TXN_START_TIMESTAMP_FIELD, transactionStartTimestampMs);

        ByteBuffer byteBuffer = ByteBuffer.allocate(2 /* version */ + struct.sizeOf());
        byteBuffer.putShort(schemaVersion);
        struct.writeTo(byteBuffer);
        byteBuffer.flip();
        return byteBuffer;
    }

    public static TransactionLogValue decode(ByteBuffer byteBuffer, short schemaVersion) {
        Schema schema = getSchema(schemaVersion);
        // skip version
        byteBuffer.getShort();
        Struct struct = schema.read(byteBuffer);

        List<PartitionsSchema> partitionsSchemas =
                Arrays.stream(struct.getArray(TXN_PARTITIONS_FIELD))
                        .map(obj -> PartitionsSchema.fromStruct((Struct) obj))
                        .collect(Collectors.toList());

        return new TransactionLogValue(
                struct.getLong(PRODUCER_ID_FIELD),
                struct.getShort(PRODUCER_EPOCH_FIELD),
                struct.getInt(TXN_TIMEOUT_MS_FIELD),
                struct.getByte(TXN_STATUS_FIELD),
                partitionsSchemas,
                struct.getLong(TXN_LAST_UPDATE_TIMESTAMP_FIELD),
                struct.getLong(TXN_START_TIMESTAMP_FIELD)
        );
    }

    public static TransactionMetadata readTxnRecordValue(String transactionalId, ByteBuffer byteBuffer) {
        // tombstone
        if (byteBuffer == null) {
            return null;
        }
        TransactionLogValue value = decode(byteBuffer, HIGHEST_SUPPORTED_VERSION);
        TransactionMetadata metadata = TransactionMetadata.builder()
                .transactionalId(transactionalId)
                .producerId(value.getProducerId())
                .lastProducerId(RecordBatch.NO_PRODUCER_ID)
                .producerEpoch(value.getProducerEpoch())
                .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                .txnTimeoutMs(value.getTransactionTimeoutMs())
                .state(TransactionState.byteToState(value.getTransactionStatus()))
                .topicPartitions(Sets.newHashSet())
                .txnStartTimestamp(value.getTransactionStartTimestampMs())
                .txnLastUpdateTimestamp(value.getTransactionLastUpdateTimestampMs())
                .build();
        if (!metadata.getState().equals(TransactionState.EMPTY)) {
            value.getTransactionPartitions().forEach(partitionsSchema -> {
                metadata.addPartitions(
                        partitionsSchema.getPartitionIds().stream()
                                .map(partition -> new TopicPartition(partitionsSchema.getTopic(), partition))
                                .collect(Collectors.toSet()));
            });
        }
        return metadata;
    }

    /**
     * Partition schema.
     */
    @Data
    @AllArgsConstructor
    public static class PartitionsSchema {

        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;

        private String topic;
        private List<Integer> partitionIds;

        private static final String TOPIC_FIELD = "topic";
        private static final String PARTITION_IDS_FIELD = "partition_ids";

        private static final Schema SCHEMA_0 =
                new Schema(
                        new Field(TOPIC_FIELD, Type.STRING, ""),
                        new Field(PARTITION_IDS_FIELD, new ArrayOf(Type.INT32), "")
                );

        private static final Schema[] SCHEMAS = new Schema[] {
                SCHEMA_0
        };

        public static Schema getSchema(short schemaVersion) {
            return SCHEMAS[schemaVersion];
        }

        public Struct toStruct() {
            Struct struct = new Struct(getSchema(HIGHEST_SUPPORTED_VERSION));
            struct.set(TOPIC_FIELD, topic);
            struct.set(PARTITION_IDS_FIELD, partitionIds.toArray());
            return struct;
        }

        public static PartitionsSchema fromStruct(Struct struct) {
            return new PartitionsSchema(
                    struct.getString("topic"),
                    Arrays.stream(struct.getArray("partition_ids"))
                            .map(id -> (Integer) id)
                            .collect(Collectors.toList())
            );
        }

        public byte[] toBytes(short schemaVersion) {
            Struct struct = new Struct(getSchema(schemaVersion));
            struct.set(TOPIC_FIELD, topic);
            struct.set(PARTITION_IDS_FIELD, partitionIds);

            ByteBuffer byteBuffer = ByteBuffer.allocate(2 /* versoin */ + struct.sizeOf());
            byteBuffer.putShort(schemaVersion);
            struct.writeTo(byteBuffer);
            return byteBuffer.array();
        }

        public static PartitionsSchema decode(ByteBuf byteBuf, short schemaVersion) {
            Schema schema = getSchema(schemaVersion);
            // skip version
            byteBuf.readShort();
            Struct struct = schema.read(byteBuf.nioBuffer());
            Object[] partitionIdObjects = struct.getArray(PARTITION_IDS_FIELD);
            return new PartitionsSchema(
                    struct.getString(TOPIC_FIELD),
                    Arrays.stream(partitionIdObjects).map(o -> (Integer) o).collect(Collectors.toList()));
        }
    }

}
