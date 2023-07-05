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

import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public abstract class ProducerStateManagerSnapshotBufferTestBase extends KopProtocolHandlerTestBase {

    protected SystemTopicClient systemTopicClient;

    protected List<String> createdTopics = new ArrayList<>();

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        this.conf.setKafkaTransactionCoordinatorEnabled(false);
        super.internalSetup();
        systemTopicClient = new SystemTopicClient(pulsar, conf);
    }

    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @BeforeMethod
    protected void cleanUp() {
        createdTopics.forEach(topic -> {
            try {
                admin.topics().delete(topic, true);
            } catch (PulsarAdminException e) {
                log.warn("Failed to delete topic {}", topic, e);
            }
        });
        createdTopics.clear();
    }

    public void createPartitionedTopic(String topic, int numPartitions) throws PulsarAdminException {
        admin.topics().createPartitionedTopic(topic, numPartitions);
        createdTopics.add(topic);
    }

    public void createProducerStateManagerSnapshotBufferTopic(String topic) throws PulsarAdminException {
        createPartitionedTopic(topic, getProducerStateManagerSnapshotBufferTopicNumPartitions());
    }

    protected abstract int getProducerStateManagerSnapshotBufferTopicNumPartitions();


    protected abstract ProducerStateManagerSnapshotBuffer createProducerStateManagerSnapshotBuffer(String topic);


    @Test(timeOut = 30000)
    public void testReadLatestSnapshot() throws Exception {
        String topic = "test-compaction-topic";

        createProducerStateManagerSnapshotBufferTopic(topic);
        ProducerStateManagerSnapshotBuffer buffer = createProducerStateManagerSnapshotBuffer(topic);

        final Map<String, ProducerStateManagerSnapshot> tpToSnapshot = Maps.newHashMap();

        final int partition = 20;

        for (int i = 0; i < partition; i++) {
            String topicPartition = TopicName.get("test-topic").getPartition(i).toString();
            for (int j = 0; j < 20; j++) {
                Map<Long, ProducerStateEntry> producers = Maps.newHashMap();
                producers.put(0L, new ProducerStateEntry(0L, (short) 0, 0, 0L, Optional.of(0L)));
                producers.put(1L, new ProducerStateEntry(1L, (short) 0, 0, 0L, Optional.of(1L)));

                TreeMap<Long, TxnMetadata> ongoingTxns = new TreeMap<>();
                ongoingTxns.put(0L, new TxnMetadata(0, 0L));
                ongoingTxns.put(1L, new TxnMetadata(1, 1L));

                List<AbortedTxn> abortedIndexList =
                        Lists.newArrayList(new AbortedTxn(0L, 1L, 2L, 3L));

                ProducerStateManagerSnapshot snapshotToWrite = new ProducerStateManagerSnapshot(
                        topicPartition,
                        UUID.randomUUID().toString(),
                        i,
                        producers,
                        ongoingTxns,
                        abortedIndexList);
                buffer.write(snapshotToWrite).get();
                tpToSnapshot.put(topicPartition, snapshotToWrite);
            }

        }

        for (int i = 0; i < partition; i++) {
            String topicPartition = TopicName.get("test-topic").getPartition(i).toString();
            ProducerStateManagerSnapshot snapshot = buffer.readLatestSnapshot(topicPartition).get();
            assertEquals(snapshot, tpToSnapshot.get(topicPartition));
        }
    }

    @Test(timeOut = 30000)
    public void testShutdownRecovery() throws Exception {
        String topic = "test-topic";
        createProducerStateManagerSnapshotBufferTopic(topic);
        ProducerStateManagerSnapshotBuffer buffer = createProducerStateManagerSnapshotBuffer(topic);

        Map<Long, ProducerStateEntry> producers = Maps.newHashMap();
        producers.put(0L, new ProducerStateEntry(0L, (short) 0, 0, 0L, Optional.of(0L)));
        producers.put(1L, new ProducerStateEntry(1L, (short) 0, 0, 0L, Optional.of(1L)));

        TreeMap<Long, TxnMetadata> ongoingTxns = new TreeMap<>();
        ongoingTxns.put(0L, new TxnMetadata(0, 0L));
        ongoingTxns.put(1L, new TxnMetadata(1, 1L));

        List<AbortedTxn> abortedIndexList =
                Lists.newArrayList(new AbortedTxn(0L, 1L, 2L, 3L));

        ProducerStateManagerSnapshot snapshotToWrite = new ProducerStateManagerSnapshot(
                topic,
                UUID.randomUUID().toString(),
                0,
                producers,
                ongoingTxns,
                abortedIndexList);
        buffer.write(snapshotToWrite).get();

        ProducerStateManagerSnapshot snapshot = buffer.readLatestSnapshot(topic).get();

        assertEquals(snapshot, snapshotToWrite);

        buffer.shutdown();

        buffer = createProducerStateManagerSnapshotBuffer(topic);
        snapshot = buffer.readLatestSnapshot(topic).get();
        assertEquals(snapshot, snapshotToWrite);
    }
}
