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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public abstract class ProducerStateManagerSnapshotBufferBase extends KopProtocolHandlerTestBase {

    protected SystemTopicClient systemTopicClient;

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


    protected abstract ProducerStateManagerSnapshotBuffer createProducerStateManagerSnapshotBuffer(String topic);


    @Test(timeOut = 30000)
    public void testReadLatestSnapshot() throws Exception {
        String topic = "test-compaction-topic";
        admin.topics().createPartitionedTopic(topic, 1);
        ProducerStateManagerSnapshotBuffer buffer = createProducerStateManagerSnapshotBuffer(topic);

        ProducerStateManagerSnapshot lastSnapshotToWrite = null;
        for (int i = 0; i < 20; i++) {
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
                    i,
                    producers,
                    ongoingTxns,
                    abortedIndexList);
            buffer.write(snapshotToWrite).get();
            lastSnapshotToWrite = snapshotToWrite;
        }

        ProducerStateManagerSnapshot snapshot = buffer.readLatestSnapshot(topic).get();
        assertEquals(snapshot, lastSnapshotToWrite);
    }
}
