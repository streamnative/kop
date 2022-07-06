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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.KopBrokerLookupManager;
import io.streamnative.pulsar.handlers.kop.utils.timer.MockTime;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.TransactionResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link TransactionMarkerChannelManager}.
 */
public class TransactionMarkerChannelManagerTest {

    private TransactionMarkerChannelManager transactionMarkerChannelManager;

    private KopBrokerLookupManager kopBrokerLookupManager;

    private TransactionStateManager txnStateManager;

    private OrderedScheduler scheduler;

    private KafkaServiceConfiguration conf;

    private final MockTime time = new MockTime();
    private final Set<TopicPartition> partitions = Sets.newHashSet(new TopicPartition("topic1", 0));
    private final String transactionalId = "known";
    private final long producerId = 10L;
    private final short producerEpoch = 1;
    private final int txnTimeoutMs = 1;
    private final int coordinatorEpoch = 0;

    @BeforeMethod
    public void setUp() throws Exception {
        txnStateManager = mock(TransactionStateManager.class);
        when(txnStateManager.partitionFor(any())).thenReturn(0);

        kopBrokerLookupManager = mock(KopBrokerLookupManager.class);
        scheduler = mock(OrderedScheduler.class);
        conf = new KafkaServiceConfiguration();
        transactionMarkerChannelManager = spy(new TransactionMarkerChannelManager(
                "public",
                conf,
                txnStateManager,
                kopBrokerLookupManager,
                false,
                "public/default",
                scheduler
                ));
    }

    @AfterMethod
    public void tearDown() {
        transactionMarkerChannelManager = null;
        kopBrokerLookupManager = null;
        scheduler.shutdown();
        scheduler = null;
        txnStateManager = null;
    }

    @Test
    public void testTopicDeletedBeforeWriteMarker() {
        when(kopBrokerLookupManager.isTopicExists(any())).thenReturn(CompletableFuture.completedFuture(false));
        TransactionMetadata txnMetadata = TransactionMetadata.builder()
                .transactionalId(transactionalId)
                .producerId(producerId)
                .lastProducerId(producerId)
                .producerEpoch(producerEpoch)
                .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                .txnTimeoutMs(txnTimeoutMs)
                .state(TransactionState.PREPARE_COMMIT)
                .topicPartitions(partitions)
                .txnStartTimestamp(time.milliseconds())
                .txnLastUpdateTimestamp(time.milliseconds())
                .build();
        TransactionMetadata.TxnTransitMetadata transition = TransactionMetadata.TxnTransitMetadata.builder()
                .producerId(producerId)
                .lastProducerId(producerId)
                .producerEpoch(producerEpoch)
                .lastProducerEpoch(RecordBatch.NO_PRODUCER_EPOCH)
                .txnTimeoutMs(txnTimeoutMs)
                .txnState(TransactionState.COMPLETE_COMMIT)
                .topicPartitions(partitions)
                .txnStartTimestamp(time.milliseconds())
                .txnLastUpdateTimestamp(time.milliseconds())
                .build();

        TransactionStateManager.CoordinatorEpochAndTxnMetadata epochAndTxnMetadata =
                new TransactionStateManager.CoordinatorEpochAndTxnMetadata(0, txnMetadata);
        when(txnStateManager.getTransactionState(transactionalId))
                .thenReturn(new ErrorsAndData<>(Optional.of(epochAndTxnMetadata)));
        transactionMarkerChannelManager.addTxnMarkersToSend(
                0,
                TransactionResult.COMMIT,
                txnMetadata,
                transition,
                "public/default"
                );
        assertTrue(transactionMarkerChannelManager.getTransactionsWithPendingMarkers().isEmpty());
    }

}