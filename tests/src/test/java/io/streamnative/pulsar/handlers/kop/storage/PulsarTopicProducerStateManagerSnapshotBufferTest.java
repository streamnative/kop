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

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
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

    @Test(timeOut = 5_000)
    public void ensureReaderHandleCaughtExceptionTest() {
        SystemTopicClient sysTopicClient = spy(new SystemTopicClient(pulsar, conf));
        ReaderBuilder<ByteBuffer> readerBuilder = spy(sysTopicClient.newReaderBuilder());
        when(readerBuilder.createAsync()).thenReturn(CompletableFuture.failedFuture(new RuntimeException("inject")));
        when(sysTopicClient.newReaderBuilder()).thenReturn(readerBuilder);

        PulsarTopicProducerStateManagerSnapshotBuffer snapshotBuffer =
                new PulsarTopicProducerStateManagerSnapshotBuffer("snapshot-test-topic", sysTopicClient);
        CompletableFuture<Reader<ByteBuffer>> readerFuture = snapshotBuffer.ensureReaderHandle();
        if (readerFuture != null) {
            try {
                readerFuture.get();
                fail("should fail");
            } catch (Exception e) {
                assertEquals(e.getCause().getMessage(), "inject");
            }
        } else {
            log.info("This is expected behavior.");
        }
    }

    @Test(timeOut = 5_000)
    public void ensureProducerCaughtExceptionTest() {
        SystemTopicClient sysTopicClient = spy(new SystemTopicClient(pulsar, conf));
        ProducerBuilder<ByteBuffer> producerBuilder = spy(sysTopicClient.newProducerBuilder());
        when(producerBuilder.createAsync()).thenReturn(CompletableFuture.failedFuture(new RuntimeException("inject")));
        when(sysTopicClient.newProducerBuilder()).thenReturn(producerBuilder);

        PulsarTopicProducerStateManagerSnapshotBuffer snapshotBuffer =
                new PulsarTopicProducerStateManagerSnapshotBuffer("snapshot-test-topic", sysTopicClient);
        CompletableFuture<Producer<ByteBuffer>> producerFuture = snapshotBuffer.ensureProducerHandle();
        if (producerFuture != null) {
            try {
                producerFuture.get();
                fail("should fail");
            } catch (Exception e) {
                assertEquals(e.getCause().getMessage(), "inject");
            }
        } else {
            log.info("This is expected behavior.");
        }
    }

}
