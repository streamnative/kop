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

import static org.testng.AssertJUnit.assertEquals;

import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.transaction.coordinator.proto.TxnStatus;
import org.mockito.internal.util.collections.Sets;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;


/**
 * Transaction log test.
 */
public class TransactionLogTest {

    private static final Short producerEpoch = 0;
    private static final Integer transactionTimeoutMs = 1000;

    Set<TopicPartition> topicPartitions = Sets.newSet(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic2", 1),
            new TopicPartition("topic2", 2));

    @Test
    public void shouldThrowExceptionWriteInvalidTxn() {
        String transactionalId = "transactionalId";
        long producerId = 23423L;

        TransactionMetadata txnMetadata = TransactionMetadata.builder()
                .transactionalId(transactionalId)
                .producerId(producerId)
                .lastProducerId(RecordBatch.NO_PRODUCER_ID)
                .producerEpoch(producerEpoch)
                .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                .txnTimeoutMs(transactionTimeoutMs)
                .state(TransactionState.EMPTY)
                .topicPartitions(Sets.newSet())
                .txnStartTimestamp(0)
                .txnLastUpdateTimestamp(0)
                .build();
        txnMetadata.addPartitions(topicPartitions);

        try {
            new TransactionLogValue(txnMetadata.prepareNoTransit()).toByteBuffer();
            Assert.fail("Transaction state is empty, topicPartitions should be empty.");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains(
                    "Transaction is not expected to have any partitions since its state is EMPTY"));
        }
    }

    @Test
    public void shouldReadWriteMessages() {
        Map<String, Long> pidMappings = Maps.newHashMap();
        pidMappings.put("zero", 0L);
        pidMappings.put("one", 1L);
        pidMappings.put("two", 2L);
        pidMappings.put("three", 3L);
        pidMappings.put("four", 4L);
        pidMappings.put("five", 5L);

        Map<Long, TransactionState> transactionStates = Maps.newHashMap();
        transactionStates.put(0L, TransactionState.EMPTY);
        transactionStates.put(1L, TransactionState.ONGOING);
        transactionStates.put(2L, TransactionState.PREPARE_COMMIT);
        transactionStates.put(3L, TransactionState.COMPLETE_COMMIT);
        transactionStates.put(4L, TransactionState.PREPARE_ABORT);
        transactionStates.put(5L, TransactionState.COMPLETE_ABORT);

        TypedMessageBuilderImpl<ByteBuffer> typedMessageBuilder =
                new TypedMessageBuilderImpl<>(null, Schema.BYTEBUFFER);

        // generate transaction log messages
        List<Message<ByteBuffer>> txnMessages = new ArrayList<>();
        pidMappings.forEach((transactionalId, producerId) -> {
            TransactionMetadata txnMetadata = TransactionMetadata.builder()
                    .transactionalId(transactionalId)
                    .producerId(producerId)
                    .lastProducerId(RecordBatch.NO_PRODUCER_ID)
                    .producerEpoch(producerEpoch)
                    .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                    .txnTimeoutMs(transactionTimeoutMs)
                    .state(transactionStates.get(producerId))
                    .topicPartitions(Sets.newSet())
                    .txnStartTimestamp(0)
                    .txnLastUpdateTimestamp(0)
                    .build();

            if (!txnMetadata.getState().equals(TransactionState.EMPTY)) {
                txnMetadata.addPartitions(topicPartitions);
            }

            byte[] keyBytes = new TransactionLogKey(transactionalId).toBytes();
            ByteBuffer valueBytes = new TransactionLogValue(txnMetadata.prepareNoTransit()).toByteBuffer();

            typedMessageBuilder.keyBytes(keyBytes);
            typedMessageBuilder.value(valueBytes);
            txnMessages.add(typedMessageBuilder.getMessage());
        });

        int count = 0;
        for (Message<ByteBuffer> message : txnMessages) {
            TransactionLogKey logKey = TransactionLogKey.decode(
                    ByteBuffer.wrap(message.getKeyBytes()), TransactionLogKey.HIGHEST_SUPPORTED_VERSION);
            String transactionalId = logKey.getTransactionId();
            TransactionMetadata txnMetadata = TransactionLogValue.readTxnRecordValue(
                    transactionalId, message.getValue());

            assertEquals(pidMappings.get(transactionalId), new Long(txnMetadata.getProducerId()));
            assertEquals(producerEpoch, new Short(txnMetadata.getProducerEpoch()));
            assertEquals(transactionTimeoutMs, new Integer(txnMetadata.getTxnTimeoutMs()));
            assertEquals(transactionStates.get(txnMetadata.getProducerId()), txnMetadata.getState());

            if (txnMetadata.getState().equals(TransactionState.EMPTY)) {
                assertEquals(Collections.EMPTY_SET, txnMetadata.getTopicPartitions());
            } else {
                assertEquals(topicPartitions, txnMetadata.getTopicPartitions());
            }

            count += 1;
        }

        assertEquals(pidMappings.size(), count);
    }

    @Test
    public void txnLogKeyCodecTest() {
        final String txnId = "txn-10";
        TransactionLogKey logKey =
                new TransactionLogKey(txnId);
        byte[] bytes = logKey.toBytes();

        TransactionLogKey decodeResult =
                TransactionLogKey.decode(
                        ByteBuffer.wrap(bytes),
                        TransactionLogKey.HIGHEST_SUPPORTED_VERSION);
        Assert.assertEquals(txnId, decodeResult.getTransactionId());
    }

    @Test
    public void txnLogValueTest() {
        final Long producerId = 2L;
        final short producerEpoch = 3;
        final int transactionTimeoutMs = 10000;
        final byte transactionStatus = TxnStatus.COMMITTED_VALUE;
        final List<TransactionLogValue.PartitionsSchema> transactionPartitions = Lists.newArrayList(
                new TransactionLogValue.PartitionsSchema("test-topic1", Lists.newArrayList(1, 2)),
                new TransactionLogValue.PartitionsSchema("test-topic2", Lists.newArrayList(3, 4))
        );
        final long transactionStartTimestampMs = System.currentTimeMillis();
        final long transactionLastUpdateTimestampMs = transactionStartTimestampMs + 5000;

        TransactionLogValue logValue =
                new TransactionLogValue(
                        producerId,
                        producerEpoch,
                        transactionTimeoutMs,
                        transactionStatus,
                        transactionPartitions,
                        transactionLastUpdateTimestampMs,
                        transactionStartTimestampMs
                );

        TransactionLogValue decodeLogValue = TransactionLogValue.decode(
                logValue.toByteBuffer(),
                TransactionLogValue.HIGHEST_SUPPORTED_VERSION);

        Assert.assertEquals(producerId, new Long(decodeLogValue.getProducerId()));
        Assert.assertEquals(producerEpoch, decodeLogValue.getProducerEpoch());
        Assert.assertEquals(transactionTimeoutMs, decodeLogValue.getTransactionTimeoutMs());
        Assert.assertEquals(transactionStatus, decodeLogValue.getTransactionStatus());
        Assert.assertEquals(transactionLastUpdateTimestampMs, decodeLogValue.getTransactionLastUpdateTimestampMs());
        Assert.assertEquals(transactionStartTimestampMs, decodeLogValue.getTransactionStartTimestampMs());

        for (int i = 0; i < transactionPartitions.size(); i++) {
            TransactionLogValue.PartitionsSchema partitionsSchema = transactionPartitions.get(i);
            TransactionLogValue.PartitionsSchema decodePartitionSchema =
                    decodeLogValue.getTransactionPartitions().get(i);
            Assert.assertEquals(partitionsSchema.getTopic(), decodePartitionSchema.getTopic());
            Assert.assertEquals(partitionsSchema.getPartitionIds(), decodePartitionSchema.getPartitionIds());
        }
    }

}