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
package io.streamnative.pulsar.handlers.kop;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Pulsar service configuration object.
 */
@Slf4j
public class KafkaTopicConsumerManagerTest extends KopProtocolHandlerTestBase {

    private KafkaTopicManager kafkaTopicManager;
    private KafkaRequestHandler kafkaRequestHandler;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        // Perform topic lookup to let broker acquire the ownership of namespace bundles so that
        // `BrokerService#getOrCreateTopic` won't fail with "Namespace bundle not served by this instance".
        this.triggerTopicLookup(conf.getKafkaTenant() + "/" + conf.getKafkaNamespace() + "/setup-topic", 16);
        kafkaRequestHandler = newRequestHandler();

        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockCtx).channel();
        kafkaRequestHandler.ctx = mockCtx;

        kafkaTopicManager = new KafkaTopicManager(kafkaRequestHandler,
                new KafkaTopicLookupService(pulsar.getBrokerService(), mock(KopBrokerLookupManager.class)));
        kafkaTopicManager.setRemoteAddress(InternalServerCnx.MOCKED_REMOTE_ADDRESS);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private String registerPartitionedTopic(final String topic) throws Exception {
        admin.topics().createPartitionedTopic(topic, 1);
        admin.lookups().lookupPartitionedTopic(topic);
        String partitionName = TopicName.get(topic).getPartition(0).toString();
        CompletableFuture<Topic> handle =
                pulsar.getBrokerService().getOrCreateTopic(partitionName);
        handle.get();
        return partitionName;
    }

    @Test
    public void testGetTopicConsumerManager() throws Exception {
        String topicName = "persistent://public/default/testGetTopicConsumerManager";
        String fullTopicName = registerPartitionedTopic(topicName);
        CompletableFuture<KafkaTopicConsumerManager> tcm = kafkaTopicManager.getTopicConsumerManager(fullTopicName);
        KafkaTopicConsumerManager topicConsumerManager = tcm.get();
        assertNotNull(topicConsumerManager);

        // 1. verify another get with same topic will return same tcm
        tcm = kafkaTopicManager.getTopicConsumerManager(fullTopicName);
        KafkaTopicConsumerManager topicConsumerManager2 = tcm.get();
        assertNotNull(topicConsumerManager2);

        assertTrue(topicConsumerManager == topicConsumerManager2);
        assertEquals(kafkaRequestHandler.getKafkaTopicManagerSharedState()
                .getKafkaTopicConsumerManagerCache().getCount(), 1);

        // 2. verify another get with different topic will return different tcm
        String topicName2 = "persistent://public/default/testGetTopicConsumerManager2";
        String fullTopicName2 = registerPartitionedTopic(topicName2);
        tcm = kafkaTopicManager.getTopicConsumerManager(fullTopicName2);
        topicConsumerManager2 = tcm.get();
        assertNotNull(topicConsumerManager2);
        assertTrue(topicConsumerManager != topicConsumerManager2);
        assertEquals(kafkaRequestHandler.getKafkaTopicManagerSharedState()
                .getKafkaTopicConsumerManagerCache().getCount(), 2);
    }


    @Test
    public void testTopicConsumerManagerRemoveAndAdd() throws Exception {
        String topicName = "persistent://public/default/testTopicConsumerManagerRemoveAndAdd";
        String fullTopicName = registerPartitionedTopic(topicName);
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        @Cleanup
        final KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        int i = 0;
        String messagePrefix = "testTopicConsumerManagerRemoveAndAdd_message_";
        long offset = -1L;
        for (; i < 5; i++) {
            String message = messagePrefix + i;
            offset = producer.send(new ProducerRecord<>(topicName, i, message)).get().offset();
        }

        CompletableFuture<KafkaTopicConsumerManager> tcm = kafkaTopicManager.getTopicConsumerManager(fullTopicName);
        KafkaTopicConsumerManager topicConsumerManager = tcm.get();

        // before a read, first get cursor of offset.
        Pair<ManagedCursor, Long> cursorPair = topicConsumerManager.removeCursorFuture(offset).get();
        assertEquals(topicConsumerManager.getCursors().size(), 0);
        ManagedCursor cursor = cursorPair.getLeft();
        assertEquals(cursorPair.getRight(), Long.valueOf(offset));

        // another write.
        producer.send(new ProducerRecord<>(topicName, i, messagePrefix + i)).get();
        i++;

        // simulate a read complete;
        offset++;
        topicConsumerManager.add(offset, Pair.of(cursor, offset));
        assertEquals(topicConsumerManager.getCursors().size(), 1);

        // another read, cache hit.
        cursorPair  = topicConsumerManager.removeCursorFuture(offset).get();
        assertEquals(topicConsumerManager.getCursors().size(), 0);
        ManagedCursor cursor2 = cursorPair.getLeft();

        assertEquals(cursor2, cursor);
        assertEquals(cursor2.getName(), cursor.getName());
        assertEquals(cursorPair.getRight(), Long.valueOf(offset));

        // simulate a read complete, add back offset.
        offset++;
        topicConsumerManager.add(offset, Pair.of(cursor2, offset));

        // produce another 3 message
        for (; i < 10; i++) {
            String message = messagePrefix + i;
            offset = producer.send(new ProducerRecord<>(topicName, i, message)).get().offset();
        }

        // try read last messages, so read not continuous
        cursorPair = topicConsumerManager.removeCursorFuture(offset).get();
        // since above remove will use a new cursor. there should be one in the map.
        assertEquals(topicConsumerManager.getCursors().size(), 1);
        cursor2 = cursorPair.getLeft();
        assertNotEquals(cursor2.getName(), cursor.getName());
        assertEquals(cursorPair.getRight(), Long.valueOf(offset));
    }

    @Test
    public void testTopicConsumerManagerRemoveCursorAndBacklog() throws Exception {
        String kafkaTopicName = "RemoveCursorAndBacklog";
        String pulsarTopicName = "persistent://public/default/" + kafkaTopicName;
        String pulsarPartitionName = pulsarTopicName + "-partition-" + 0;

        // create partitioned topic with 1 partition.
        admin.topics().createPartitionedTopic(kafkaTopicName, 1);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        @Cleanup
        final KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        int i = 0;
        String messagePrefix = "testTopicConsumerManagerRemoveCursor_message_XXXXXX_";

        long offset1 = -1L;
        for (; i < 5; i++) {
            String message = messagePrefix + i % 10;
            offset1 = producer.send(new ProducerRecord<>(kafkaTopicName, i, message)).get().offset();
        }

        // produce another 5 message
        long offset2 = -1L;
        for (; i < 10; i++) {
            String message = messagePrefix + i % 10;
            offset2 = producer.send(new ProducerRecord<>(kafkaTopicName, i, message)).get().offset();
        }

        // produce another 5 message
        long offset3 = -1L;
        for (; i < 15; i++) {
            String message = messagePrefix + i % 10;
            offset3 = producer.send(new ProducerRecord<>(kafkaTopicName, i, message)).get().offset();
        }

        for (; i < 20; i++) {
            String message = messagePrefix + i % 10;
            producer.send(new ProducerRecord<>(kafkaTopicName, i, message)).get();
        }

        CompletableFuture<KafkaTopicConsumerManager> tcm = kafkaTopicManager
            .getTopicConsumerManager(pulsarPartitionName);
        KafkaTopicConsumerManager topicConsumerManager = tcm.get();

        // before a read, first get cursor of offset.
        Pair<ManagedCursor, Long> cursorPair1 = topicConsumerManager.removeCursorFuture(offset1).get();
        Pair<ManagedCursor, Long> cursorPair2 = topicConsumerManager.removeCursorFuture(offset2).get();
        Pair<ManagedCursor, Long> cursorPair3 = topicConsumerManager.removeCursorFuture(offset3).get();
        assertEquals(topicConsumerManager.getCursors().size(), 0);

        ManagedCursor cursor1 = cursorPair1.getLeft();
        ManagedCursor cursor2 = cursorPair2.getLeft();
        ManagedCursor cursor3 = cursorPair3.getLeft();

        PersistentTopic persistentTopic = (PersistentTopic)
                pulsar.getBrokerService().getTopicReference(pulsarPartitionName).get();

        long backlogSize = persistentTopic.getStats(true, true, true).backlogSize;
        verifyBacklogAndNumCursor(persistentTopic, backlogSize, 3);

        // simulate a read complete;
        offset1++;
        offset2++;
        offset3++;

        topicConsumerManager.add(offset1, Pair.of(cursor1, offset1));
        topicConsumerManager.add(offset2, Pair.of(cursor2, offset2));
        topicConsumerManager.add(offset3, Pair.of(cursor3, offset3));
        assertEquals(topicConsumerManager.getCursors().size(), 3);

        // simulate cursor deleted, and backlog cleared.
        topicConsumerManager.deleteOneExpiredCursor(offset3);
        verifyBacklogAndNumCursor(persistentTopic, backlogSize, 2);
        topicConsumerManager.deleteOneExpiredCursor(offset2);
        verifyBacklogAndNumCursor(persistentTopic, backlogSize, 1);
        topicConsumerManager.deleteOneExpiredCursor(offset1);
        verifyBacklogAndNumCursor(persistentTopic, 0, 0);

        assertEquals(topicConsumerManager.getCursors().size(), 0);
    }

    // dump Topic Stats, mainly want to get and verify backlogSize.
    private void verifyBacklogAndNumCursor(PersistentTopic persistentTopic,
                                           long expectedBacklog,
                                           int numCursor) throws Exception {
        AtomicLong backlog = new AtomicLong(0);
        AtomicInteger cursorCount = new AtomicInteger(0);
        retryStrategically(
            ((test) -> {
                backlog.set(persistentTopic.getStats(true, true, true).backlogSize);
                return backlog.get() == expectedBacklog;
            }),
            5,
            200);

        if (log.isDebugEnabled()) {
            TopicStats topicStats = persistentTopic.getStats(true, true, true);
            log.info(" dump topicStats for topic : {}, storageSize: {}, backlogSize: {}, expected: {}",
                persistentTopic.getName(),
                topicStats.getStorageSize(), topicStats.getBacklogSize(), expectedBacklog);

            topicStats.getSubscriptions().forEach((subname, substats) -> {
                log.debug(" dump sub: subname - {}, activeConsumerName {}, "
                        + "consumers {}, msgBacklog {}, unackedMessages {}.",
                    subname,
                    substats.getActiveConsumerName(), substats.getConsumers(),
                    substats.getMsgBacklog(), substats.getUnackedMessages());
            });
        }

        persistentTopic.getManagedLedger().getCursors().forEach(cursor -> {
            if (log.isDebugEnabled()) {
                log.debug(" dump cursor: cursor - {}, durable: {}, numberEntryis: {},"
                        + " readPosition: {}, markdeletePosition: {}",
                    cursor.getName(), cursor.isDurable(), cursor.getNumberOfEntries(),
                    cursor.getReadPosition(), cursor.getMarkDeletedPosition());
            }
            cursorCount.incrementAndGet();
        });

        // verify.
        assertEquals(backlog.get(), expectedBacklog);
        assertEquals(cursorCount.get(), numCursor);
    }

    @Test(timeOut = 20000)
    public void testOnlyOneCursorCreated() throws Exception {
        final String topic = "testOnlyOneCursorCreated";
        final String partitionName = new KopTopic(topic, "public/default").getPartitionName(0);
        admin.topics().createPartitionedTopic(topic, 1);

        final int numMessages = 100;

        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());
        for (int i = 0; i < numMessages; i++) {
            producer.send(new ProducerRecord<>(topic, "msg-" + i)).get();
        }

        @Cleanup
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(newKafkaConsumerProperties());
        consumer.subscribe(Collections.singleton(topic));

        int numReceived = 0;
        while (numReceived < numMessages) {
            numReceived += consumer.poll(Duration.ofSeconds(1)).count();
        }

        final List<KafkaTopicConsumerManager> tcmList = kafkaRequestHandler.getKafkaTopicManagerSharedState()
                .getKafkaTopicConsumerManagerCache().getTopicConsumerManagers(partitionName);
        assertFalse(tcmList.isEmpty());
        // Only 1 cursor should be created for a consumer even if there were a lot of FETCH requests
        // This check is to ensure that KafkaTopicConsumerManager#add is called in FETCH request handler
        assertEquals(tcmList.get(0).getCreatedCursors().size(), 1);
        assertEquals(tcmList.get(0).getNumCreatedCursors(), 1);
    }

    @Test(timeOut = 20000)
    public void testCursorCountForMultiGroups() throws Exception {
        final String topic = "test-cursor-count-for-multi-groups";
        final String partitionName = new KopTopic(topic, "public/default").getPartitionName(0);
        final int numMessages = 100;
        final int numConsumers = 5;

        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());
        for (int i = 0; i < numMessages; i++) {
            producer.send(new ProducerRecord<>(topic, "msg-" + i)).get();
        }
        producer.close();

        final List<KafkaConsumer<String, String>> consumers = IntStream.range(0, numConsumers)
                .mapToObj(i -> {
                    final Properties props = newKafkaConsumerProperties();
                    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + i);
                    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                    consumer.subscribe(Collections.singleton(topic));
                    return consumer;
                }).collect(Collectors.toList());

        final CountDownLatch latch = new CountDownLatch(numConsumers);
        final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        for (int i = 0; i < numConsumers; i++) {
            final int index = i;
            final KafkaConsumer<String, String> consumer = consumers.get(i);
            executor.execute(() -> {
                int numReceived = 0;
                while (numReceived < numMessages) {
                    final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                    records.forEach(record -> {
                        if (log.isDebugEnabled()) {
                            log.debug("Group {} received message {}", index, record.value());
                        }
                    });
                    numReceived += records.count();
                }
                latch.countDown();
            });
        }
        latch.await(10, TimeUnit.SECONDS);

        final List<KafkaTopicConsumerManager> tcmList = kafkaRequestHandler.getKafkaTopicManagerSharedState()
                .getKafkaTopicConsumerManagerCache().getTopicConsumerManagers(partitionName);
        assertEquals(tcmList.size(), numConsumers);

        // All TCMs share the same topic, so each internal PersistentTopic of TCM has `numConsumers` cursors.
        for (int i = 0; i < numConsumers; i++) {
            assertEquals(tcmList.get(i).getNumCreatedCursors(), numConsumers);
        }

        // Since consumer close will make connection disconnected and all TCMs will be cleared, we should call it after
        // the test is verified.
        consumers.forEach(KafkaConsumer::close);
        Awaitility.await().atMost(Duration.ofSeconds(3)).until(() -> tcmList.get(0).getNumCreatedCursors() == 0);
        for (int i = 0; i < numConsumers; i++) {
            assertEquals(tcmList.get(i).getNumCreatedCursors(), 0);
        }
    }

    // KafkaTopicManager#close should only remove TCM cache for the specific address
    @Test(timeOut = 20000)
    public void testTopicManagerClose() throws Exception {
        final String topic = "test-topic-manager-close";
        final int numPartitions = 2;
        admin.topics().createPartitionedTopic(topic, numPartitions);

        final List<KafkaConsumer<String, String>> consumers = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            consumers.add(new KafkaConsumer<>(newKafkaConsumerProperties()));
            consumers.get(i).assign(Collections.singleton(new TopicPartition(topic, i)));
        }

        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());
        for (int i = 0; i < numPartitions; i++) {
            producer.send(new ProducerRecord<>(topic, i, null, "msg-" + i)).get();
            final ConsumerRecords<String, String> records = consumers.get(i).poll(Duration.ofSeconds(1));
            assertEquals(records.count(), 1);
            assertEquals(records.iterator().next().value(), "msg-" + i);
        }

        final Function<Integer, KafkaTopicConsumerManager> getTcmForPartition = partition -> {
            final String fullTopicName = new KopTopic(topic, "public/default").getPartitionName(partition);
            final List<KafkaTopicConsumerManager> tcmList =
                    kafkaRequestHandler.getKafkaTopicManagerSharedState()
                            .getKafkaTopicConsumerManagerCache().getTopicConsumerManagers(fullTopicName);
            return tcmList.isEmpty() ? null : tcmList.get(0);
        };

        final List<KafkaTopicConsumerManager> originalTcmList = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            final KafkaTopicConsumerManager tcm = getTcmForPartition.apply(i);
            assertNotNull(tcm);
            assertFalse(tcm.isClosed());
            originalTcmList.add(tcm);
        }

        producer.close(); // trigger KafkaTopicManager#close but the TCM cache was not affected
        assertSame(getTcmForPartition.apply(0), originalTcmList.get(0));
        assertFalse(originalTcmList.get(0).isClosed());
        assertSame(getTcmForPartition.apply(1), originalTcmList.get(1));
        assertFalse(originalTcmList.get(1).isClosed());

        consumers.get(1).close(); // trigger KafkaTopicManager#close, only the partition 1 related cache was removed
        // Because the KafkaRequestHandler.close() is called by channelInActive, when channelInActive called,
        // the tcp connect already closed. We need ensure topicManager.close() is called.
        Awaitility.await()
                .atMost(Duration.ofSeconds(3))
                .until(() -> originalTcmList.get(1).getNumCreatedCursors() == 0);
        assertSame(getTcmForPartition.apply(0), originalTcmList.get(0));
        assertFalse(originalTcmList.get(0).isClosed());
        // The tcm of partition 1 was closed and it was removed from cache
        assertNull(getTcmForPartition.apply(1));
        assertTrue(originalTcmList.get(1).isClosed());

        consumers.get(0).close(); // Now all TCM cache was cleared
        Awaitility.await()
                .atMost(Duration.ofSeconds(3))
                .until(() -> originalTcmList.get(0).getNumCreatedCursors() == 0);
        assertNull(getTcmForPartition.apply(0));
        assertNull(getTcmForPartition.apply(1));
        assertTrue(originalTcmList.get(0).isClosed());
        assertTrue(originalTcmList.get(1).isClosed());
    }

    @Test(timeOut = 20000)
    public void testUnloadTopic() throws Exception {
        final String topic = "test-unload-topic";
        final String fullTopicName = "persistent://public/default/" + topic + "-partition-0";
        final int numPartitions = 1;
        admin.topics().createPartitionedTopic(topic, numPartitions);

        final int totalMessages = 5;
        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());
        int numMessages = 0;
        while (numMessages < totalMessages) {
            producer.send(new ProducerRecord<>(topic, null, "test-value" + numMessages)).get();
            numMessages++;
        }

        // We first get KafkaTopicConsumerManager, and then unload topic,
        // so that KafkaTopicConsumerManager will become invalid
        CompletableFuture<KafkaTopicConsumerManager> tcm = kafkaTopicManager.getTopicConsumerManager(fullTopicName);
        KafkaTopicConsumerManager topicConsumerManager = tcm.get();
        // unload topic
        admin.topics().unload(fullTopicName);

        // This proves that ManagedLedger has been closed
        // and that the newly added code has taken effect.
        try {
            topicConsumerManager.removeCursorFuture(totalMessages - 1).get();
            fail("should have failed");
        } catch (ExecutionException ex) {
            log.info("error", ex);
            assertTrue(ex.getCause().getMessage().contains("Current managedLedger for "
                    + fullTopicName + " has been closed."));
        }

    }
}
