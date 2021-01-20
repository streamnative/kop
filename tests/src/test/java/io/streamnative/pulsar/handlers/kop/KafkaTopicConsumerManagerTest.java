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
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.TopicStats;
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
    private SocketAddress serviceAddress;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();

        ProtocolHandler handler = pulsar.getProtocolHandlers().protocol("kafka");
        GroupCoordinator groupCoordinator = ((KafkaProtocolHandler) handler).getGroupCoordinator();
        TransactionCoordinator transactionCoordinator = ((KafkaProtocolHandler) handler).getTransactionCoordinator();

        kafkaRequestHandler = new KafkaRequestHandler(
            pulsar,
            (KafkaServiceConfiguration) conf,
            groupCoordinator,
            transactionCoordinator,
            false,
            getPlainEndPoint());

        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockCtx).channel();
        kafkaRequestHandler.ctx = mockCtx;

        serviceAddress = new InetSocketAddress(pulsar.getBindAddress(), kafkaBrokerPort);

        kafkaTopicManager = new KafkaTopicManager(kafkaRequestHandler);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testGetTopicConsumerManager() throws Exception {
        String topicName = "persistent://public/default/testGetTopicConsumerManager";
        admin.lookups().lookupTopic(topicName);
        CompletableFuture<KafkaTopicConsumerManager> tcm = kafkaTopicManager.getTopicConsumerManager(topicName);
        KafkaTopicConsumerManager topicConsumerManager = tcm.get();

        // 1. verify another get with same topic will return same tcm
        tcm = kafkaTopicManager.getTopicConsumerManager(topicName);
        KafkaTopicConsumerManager topicConsumerManager2 = tcm.get();

        assertTrue(topicConsumerManager == topicConsumerManager2);
        assertEquals(kafkaTopicManager.getConsumerTopicManagers().size(), 1);

        // 2. verify another get with different topic will return different tcm
        String topicName2 = "persistent://public/default/testGetTopicConsumerManager2";
        admin.lookups().lookupTopic(topicName2);
        tcm = kafkaTopicManager.getTopicConsumerManager(topicName2);
        topicConsumerManager2 = tcm.get();
        assertTrue(topicConsumerManager != topicConsumerManager2);
        assertEquals(kafkaTopicManager.getConsumerTopicManagers().size(), 2);
    }


    @Test
    public void testTopicConsumerManagerRemoveAndAdd() throws Exception {
        String topicName = "persistent://public/default/testTopicConsumerManagerRemoveAndAdd";
        admin.lookups().lookupTopic(topicName);

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        KProducer kProducer = new KProducer(topicName, true, getKafkaBrokerPort());
        int i = 0;
        String messagePrefix = "testTopicConsumerManagerRemoveAndAdd_message_";
        long offset = -1L;
        for (; i < 5; i++) {
            String message = messagePrefix + i;
            offset = producer.send(new ProducerRecord<>(topicName, i, message)).get().offset();
        }

        CompletableFuture<KafkaTopicConsumerManager> tcm = kafkaTopicManager.getTopicConsumerManager(topicName);
        KafkaTopicConsumerManager topicConsumerManager = tcm.get();

        // before a read, first get cursor of offset.
        Pair<ManagedCursor, Long> cursorPair = topicConsumerManager.remove(offset);
        assertEquals(topicConsumerManager.getConsumers().size(), 0);
        ManagedCursor cursor = cursorPair.getLeft();
        assertEquals(cursorPair.getRight(), Long.valueOf(offset));

        // another write.
        producer.send(new ProducerRecord<>(topicName, i, messagePrefix + i)).get();
        i++;

        // simulate a read complete;
        offset++;
        topicConsumerManager.add(offset, Pair.of(cursor, offset));
        assertEquals(topicConsumerManager.getConsumers().size(), 1);

        // another read, cache hit.
        cursorPair  = topicConsumerManager.remove(offset);
        assertEquals(topicConsumerManager.getConsumers().size(), 0);
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
        cursorPair = topicConsumerManager.remove(offset);
        // since above remove will use a new cursor. there should be one in the map.
        assertEquals(topicConsumerManager.getConsumers().size(), 1);
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
        Pair<ManagedCursor, Long> cursorPair1 = topicConsumerManager.remove(offset1);
        Pair<ManagedCursor, Long> cursorPair2 = topicConsumerManager.remove(offset2);
        Pair<ManagedCursor, Long> cursorPair3 = topicConsumerManager.remove(offset3);
        assertEquals(topicConsumerManager.getConsumers().size(), 0);

        ManagedCursor cursor1 = cursorPair1.getLeft();
        ManagedCursor cursor2 = cursorPair2.getLeft();
        ManagedCursor cursor3 = cursorPair3.getLeft();

        PersistentTopic persistentTopic = (PersistentTopic)
                pulsar.getBrokerService().getTopicReference(pulsarPartitionName).get();

        long backlogSize = persistentTopic.getStats(true).backlogSize;
        verifyBacklogAndNumCursor(persistentTopic, backlogSize, 3);

        // simulate a read complete;
        offset1++;
        offset2++;
        offset3++;

        topicConsumerManager.add(offset1, Pair.of(cursor1, offset1));
        topicConsumerManager.add(offset2, Pair.of(cursor2, offset2));
        topicConsumerManager.add(offset3, Pair.of(cursor3, offset3));
        assertEquals(topicConsumerManager.getConsumers().size(), 3);

        // simulate cursor deleted, and backlog cleared.
        topicConsumerManager.deleteOneExpiredCursor(offset3);
        verifyBacklogAndNumCursor(persistentTopic, backlogSize, 2);
        topicConsumerManager.deleteOneExpiredCursor(offset2);
        verifyBacklogAndNumCursor(persistentTopic, backlogSize, 1);
        topicConsumerManager.deleteOneExpiredCursor(offset1);
        verifyBacklogAndNumCursor(persistentTopic, 0, 0);

        assertEquals(topicConsumerManager.getConsumers().size(), 0);
    }

    // dump Topic Stats, mainly want to get and verify backlogSize.
    private void verifyBacklogAndNumCursor(PersistentTopic persistentTopic,
                                           long expectedBacklog,
                                           int numCursor) throws Exception {
        AtomicLong backlog = new AtomicLong(0);
        AtomicInteger cursorCount = new AtomicInteger(0);
        retryStrategically(
            ((test) -> {
                backlog.set(persistentTopic.getStats(true).backlogSize);
                return backlog.get() == expectedBacklog;
            }),
            5,
            200);

        if (log.isDebugEnabled()) {
            TopicStats topicStats = persistentTopic.getStats(true);
            log.info(" dump topicStats for topic : {}, storageSize: {}, backlogSize: {}, expected: {}",
                persistentTopic.getName(),
                topicStats.storageSize, topicStats.backlogSize, expectedBacklog);

            topicStats.subscriptions.forEach((subname, substats) -> {
                log.debug(" dump sub: subname - {}, activeConsumerName {}, "
                        + "consumers {}, msgBacklog {}, unackedMessages {}.",
                    subname,
                    substats.activeConsumerName, substats.consumers,
                    substats.msgBacklog, substats.unackedMessages);
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
}
