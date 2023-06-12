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
package io.streamnative.pulsar.handlers.kop.coordinator;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class CompactedPartitionedTopicTest extends KopProtocolHandlerTestBase {

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        executor.shutdown();
        super.internalCleanup();
    }

    @Test(timeOut = 30000)
    public void testCompaction() throws Exception {
        final var topic = "test-compaction";
        final var numPartitions = 3;
        admin.topics().createPartitionedTopic(topic, numPartitions);
        @Cleanup final var compactedTopic = new CompactedPartitionedTopic<>(pulsarClient, Schema.STRING,
                1000, topic, executor, String::isEmpty);

        final var numMessages = 100;
        final var futures = new ArrayList<CompletableFuture<MessageId>>();
        for (int i = 0; i < numMessages; i++) {
            for (int j = 0; j < numPartitions; j++) {
                futures.add(compactedTopic.sendAsync(j, ("A" + j).getBytes(), "msg-" + i, i + 100));
                futures.add(compactedTopic.sendAsync(j, ("B" + j).getBytes(), "msg-" + i, i + 100));
            }
        }
        CoreUtils.waitForAll(futures).get();

        Callable<Map<String, List<String>>> readKeyValues = () -> {
            final var keyValues = new ConcurrentHashMap<String, List<String>>();
            for (int i = 0; i < numPartitions; i++) {
                final var readResult = compactedTopic.readToLatest(i, msg -> keyValues.computeIfAbsent(
                        new String(msg.getKeyBytes()), __ -> new CopyOnWriteArrayList<>()
                ).add(msg.getValue())).get();
                log.info("Load from partition {}: {}ms and {} messages",
                        i, readResult.timeMs(), readResult.numMessages());
            }
            return keyValues;
        };
        var keyValues = readKeyValues.call();
        Assert.assertEquals(keyValues.keySet().size(), numPartitions * 2);
        for (int i = 0; i < numPartitions; i++) {
            final var values = IntStream.range(0, numMessages).mapToObj(__ -> "msg-" + __).toList();
            Assert.assertEquals(keyValues.get("A" + i), values);
            Assert.assertEquals(keyValues.get("B" + i), values);
        }

        for (int i = 0; i < numPartitions; i++) {
            final var partition = topic + TopicName.PARTITIONED_TOPIC_SUFFIX + i;
            admin.topics().triggerCompaction(partition);
            Awaitility.await().atMost(Duration.ofSeconds(3)).until(() ->
                    admin.topics().compactionStatus(partition).status == LongRunningProcessStatus.Status.SUCCESS);
        }

        // Clear the cache of the 1st partition
        compactedTopic.remove(0);
        keyValues = readKeyValues.call();

        final var singleMessageList = Collections.singletonList("msg-" + (numMessages - 1));
        Assert.assertEquals(keyValues.keySet(), new HashSet<>(Arrays.asList("A0", "B0")));
        Assert.assertEquals(keyValues.get("A0"), singleMessageList);
        Assert.assertEquals(keyValues.get("B0"), singleMessageList);
    }

    @Test(timeOut = 30000)
    public void testSkipEmptyMessages() throws Exception {
        final var topic = "test-skip-empty-messages";
        admin.topics().createPartitionedTopic(topic, 1);
        @Cleanup final var compactedTopic = new CompactedPartitionedTopic<>(pulsarClient, Schema.BYTEBUFFER,
                1000, topic, executor, buffer -> buffer.limit() == 0);
        @Cleanup final var producer = pulsarClient.newProducer(Schema.BYTEBUFFER)
                .topic(topic + TopicName.PARTITIONED_TOPIC_SUFFIX + 0).create();
        final var numMessages = 3 * 100;
        for (int i = 0; i < numMessages; i++) {
            producer.send(ByteBuffer.allocate((i % 3 == 0 ? 0 : 1)));
        }

        final var numMessagesReceived = new AtomicInteger(0);
        compactedTopic.readToLatest(0, msg -> numMessagesReceived.incrementAndGet()).get();
        Assert.assertEquals(numMessagesReceived.get(), numMessages - numMessages / 3);
    }

    @Test(timeOut = 30000)
    public void testClose() throws Exception {
        final var topic = "test-close";
        admin.topics().createPartitionedTopic(topic, 1);
        final var compactedTopic = new CompactedPartitionedTopic<>(pulsarClient, Schema.STRING,
                1000, topic, executor, __ -> false);
        final var numMessages = 100;
        for (int i = 0; i < numMessages; i++) {
            compactedTopic.sendAsync(0, "key".getBytes(), "value", 1).get();
        }
        final var numMessagesReceived = new AtomicInteger(0);
        final var future = compactedTopic.readToLatest(0, msg -> numMessagesReceived.incrementAndGet());
        compactedTopic.close();
        final var readResult = future.get();
        log.info("Load {} messages in {}ms", readResult.timeMs(), readResult.numMessages());
        Assert.assertEquals(readResult.numMessages(), numMessagesReceived.get());
        Assert.assertTrue(readResult.numMessages() < numMessages);
    }
}
