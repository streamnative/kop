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

import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Pulsar service configuration object.
 */
@Slf4j
public class KafkaTopicConsumerManagerTest extends MockKafkaServiceBaseTest {

    private KafkaTopicManager kafkaTopicManager;
    private KafkaRequestHandler kafkaRequestHandler;
    private SocketAddress serviceAddress;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        // super class already created clusters and tenants
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
        admin.namespaces().setRetention("public/default",
            new RetentionPolicies(20, 100));

        kafkaRequestHandler = new KafkaRequestHandler(
            kafkaService,
            kafkaService.getKafkaConfig(),
            kafkaService.getGroupCoordinator(), false);
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockCtx).channel();
        kafkaRequestHandler.ctx = mockCtx;

        serviceAddress = new InetSocketAddress(kafkaService.getBindAddress(), kafkaBrokerPort);

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

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
            .topic(topicName)
            .enableBatching(false);

        Producer<byte[]> producer = producerBuilder.create();
        MessageIdImpl messageId = null;
        int i = 0;
        String messagePrefix = "testTopicConsumerManagerRemoveAndAdd_message_";
        for (; i < 5; i++) {
            String message = messagePrefix + i;
            messageId = (MessageIdImpl) producer.newMessage()
                .keyBytes(kafkaIntSerialize(Integer.valueOf(i)))
                .value(message.getBytes())
                .send();
        }

        CompletableFuture<KafkaTopicConsumerManager> tcm = kafkaTopicManager.getTopicConsumerManager(topicName);
        KafkaTopicConsumerManager topicConsumerManager = tcm.get();

        long offset = MessageIdUtils.getOffset(messageId.getLedgerId(), messageId.getEntryId());

        // before a read, first get cursor of offset.
        CompletableFuture<Pair<ManagedCursor, Long>> cursorCompletableFuture = topicConsumerManager.remove(offset);
        assertEquals(topicConsumerManager.getConsumers().size(), 0);
        ManagedCursor cursor = cursorCompletableFuture.get().getLeft();
        assertEquals(cursorCompletableFuture.get().getRight(), Long.valueOf(offset));

        // another write.
        producer.newMessage()
            .keyBytes(kafkaIntSerialize(Integer.valueOf(i)))
            .value((messagePrefix + i).getBytes())
            .send();
        i++;

        // simulate a read complete;
        offset += 1 << MessageIdUtils.BATCH_BITS;
        topicConsumerManager.add(offset, Pair.of(cursor, offset));
        assertEquals(topicConsumerManager.getConsumers().size(), 1);

        // another read, cache hit.
        cursorCompletableFuture = topicConsumerManager.remove(offset);
        assertEquals(topicConsumerManager.getConsumers().size(), 0);
        ManagedCursor cursor2 = cursorCompletableFuture.get().getLeft();

        assertTrue(cursor2 == cursor);
        assertEquals(cursor2.getName(), cursor.getName());
        assertEquals(cursorCompletableFuture.get().getRight(), Long.valueOf(offset));

        // simulate a read complete, add back offset.
        offset += 1 << MessageIdUtils.BATCH_BITS;
        topicConsumerManager.add(offset, Pair.of(cursor2, offset));

        // produce another 3 message
        for (; i < 10; i++) {
            String message = messagePrefix + i;
            messageId = (MessageIdImpl) producer.newMessage()
                .keyBytes(kafkaIntSerialize(Integer.valueOf(i)))
                .value(message.getBytes())
                .send();
        }

        // try read last messages, so read not continuous
        offset = MessageIdUtils.getOffset(messageId.getLedgerId(), messageId.getEntryId());
        cursorCompletableFuture = topicConsumerManager.remove(offset);
        // since above remove will use a new cursor. there should be one in the map.
        assertEquals(topicConsumerManager.getConsumers().size(), 1);
        cursor2 = cursorCompletableFuture.get().getLeft();
        assertNotEquals(cursor2.getName(), cursor.getName());
        assertEquals(cursorCompletableFuture.get().getRight(), Long.valueOf(offset));
    }

}
