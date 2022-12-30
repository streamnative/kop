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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

/**
 * Unit test for {@link PulsarTopicProducerStateManagerSnapshotBuffer}.
 */
@Slf4j
public class PulsarTopicProducerStateManagerSnapshotBufferTest extends ProducerStateManagerSnapshotBufferTestBase {

    @Override
    protected int getProducerStateManagerSnapshotBufferTopicNumPartitions() {
        return 1;
    }

    @Override
    protected ProducerStateManagerSnapshotBuffer createProducerStateManagerSnapshotBuffer(String topic) {
        return new PulsarTopicProducerStateManagerSnapshotBuffer(
                topic, systemTopicClient, getProtocolHandler().getRecoveryExecutor());
    }

    @Test
    public void testSerializeAndDeserialize() {
        for (int i = 0; i < 100; i++) {
            Map<Long, ProducerStateEntry> producers = Maps.newHashMap();
            TreeMap<Long, TxnMetadata> ongoingTxns = new TreeMap<>();
            List<AbortedTxn> abortedIndexList = Lists.newArrayList();
            for (int k = 0; k < i; k++) {
                producers.put((long) (i * 10 + k),
                        new ProducerStateEntry((long) (i * 10 + k), (short) 0, 0, 0L, Optional.of(0L)));
                ongoingTxns.put((long) (i * 10 + k), new TxnMetadata(i, i * 10 + k));
                abortedIndexList.add(new AbortedTxn((long) (i * 10 + k), 0L, 0L, 0L));
            }
            ProducerStateManagerSnapshot snapshot = new ProducerStateManagerSnapshot(
                    "test-topic",
                    UUID.randomUUID().toString(),
                    i,
                    producers,
                    ongoingTxns,
                    abortedIndexList);
            ByteBuffer serialized = PulsarTopicProducerStateManagerSnapshotBuffer.serialize(snapshot);
            ProducerStateManagerSnapshot deserialized =
                    PulsarTopicProducerStateManagerSnapshotBuffer.deserialize(serialized);

            assertEquals(deserialized, snapshot);
        }
    }

}
