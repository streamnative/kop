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


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.PublishRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test KoP message publish throttling.
 */
@Slf4j
public class MessagePublishThrottlingTest extends KopProtocolHandlerTestBase {

    private final boolean preciseTopicPublishRateLimiterEnable;

    public MessagePublishThrottlingTest(boolean preciseTopicPublishRateLimiterEnable) {
        this.preciseTopicPublishRateLimiterEnable = preciseTopicPublishRateLimiterEnable;
    }

    public MessagePublishThrottlingTest() {
        this(false);
    }


    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setPreciseTopicPublishRateLimiterEnable(preciseTopicPublishRateLimiterEnable);
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
        super.internalSetup();
        log.info("success internal setup");
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "isTopicLevel")
    protected static Object[][] topicLevelProvider() {
        // isTopicLevel
        return new Object[][]{
                {true},
                {false}
        };
    }

    @Test(timeOut = 30 * 1000, dataProvider = "isTopicLevel")
    public void testPublishByteThrottling(boolean isTopicLevel) throws Exception {

        final String namespace = "public/throttling_publish" + (isTopicLevel ? "_topic_level" : "_namespace_level");
        final String topicName = "persistent://" + namespace + "/testPublishByteThrottling";

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        PublishRate topicPublishMsgRate = new PublishRate();
        long topicByteRate = 400;
        topicPublishMsgRate.publishThrottlingRateInByte = topicByteRate;

        // Produce message with Kafka producer, to make sure topic are created.
        @Cleanup
        KafkaProducer<Integer, byte[]> producer = createKafkaProducer();
        producer.send(new ProducerRecord<>(topicName, new byte[50])).get();

        String topicNameWithPartition = TopicName.get(topicName).getPartition(0).toString();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topicNameWithPartition).get().get();
        // Verify both broker and topic limiter is disabled
        assertEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // Enable throttling
        if (isTopicLevel) {
            admin.topicPolicies().setPublishRate(topicName, topicPublishMsgRate);
        } else {
            admin.namespaces().setPublishRate(namespace, topicPublishMsgRate);
        }

        Awaitility.await().untilAsserted(() -> {
            assertNotEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);
        });

        Producer prod = topic.getProducers().values().iterator().next();
        // reset counter
        prod.updateRates();
        int numMessage = 10;
        int msgBytes = 110;
        int numThread = 4;
        this.sendMessagesFromMultiThreads(producer, topicName, numThread, numMessage, msgBytes);

        prod.updateRates();
        double rateIn = prod.getStats().msgThroughputIn;

        int totalBytes = numMessage * msgBytes * numThread;

        log.info("Byte rate in: {} byte/s, total: {} bytes", rateIn, numMessage * msgBytes * numThread);
        if (preciseTopicPublishRateLimiterEnable) {
            assertTrue(rateIn <= topicByteRate + 100);
            assertTrue(rateIn >= topicByteRate - 100);
        } else {
            assertTrue(rateIn <= totalBytes);
        }

        // Disable throttling
        topicPublishMsgRate.publishThrottlingRateInByte = -1;
        if (isTopicLevel) {
            admin.topicPolicies().setPublishRate(topicName, topicPublishMsgRate);
        } else {
            admin.namespaces().setPublishRate(namespace, topicPublishMsgRate);
        }
        Awaitility.await().untilAsserted(() -> {
            assertEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);
        });
        // reset counter
        prod.updateRates();
        for (int i = 0; i < numMessage; i++) {
            producer.send(new ProducerRecord<>(topicName, new byte[msgBytes])).get();
        }

        prod.updateRates();
        rateIn = prod.getStats().msgThroughputIn;

        log.info("Byte rate in: {} byte/s, total: {} bytes. (Disabled throttling)", rateIn, numMessage * msgBytes);
        assertTrue(rateIn >= numMessage * msgBytes);
    }

    @Test(timeOut = 30 * 1000, dataProvider = "isTopicLevel")
    public void testPublishMsgNumThrottling(boolean isTopicLevel) throws Exception {

        final String namespace = "public/throttling_publish_msg_num"
                + (isTopicLevel ? "_topic_level" : "_namespace_level");
        final String topicName = "persistent://" + namespace + "/testPublishMsgNumThrottling";

        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        PublishRate topicPublishMsgRate = new PublishRate();
        int topicPublishRateInMsg = 10;
        topicPublishMsgRate.publishThrottlingRateInMsg = topicPublishRateInMsg;

        // Produce message with Kafka producer, to make sure topic are created.
        @Cleanup
        KafkaProducer<Integer, byte[]> producer = createKafkaProducer();
        producer.send(new ProducerRecord<>(topicName, new byte[50])).get();

        String topicNameWithPartition = TopicName.get(topicName).getPartition(0).toString();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(topicNameWithPartition).get().get();
        // Verify both broker and topic limiter is disabled
        assertEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // Enable throttling
        if (isTopicLevel) {
            admin.topicPolicies().setPublishRate(topicName, topicPublishMsgRate);
        } else {
            admin.namespaces().setPublishRate(namespace, topicPublishMsgRate);
        }

        Awaitility.await().untilAsserted(() -> {
            assertNotEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);
        });

        Producer prod = topic.getProducers().values().iterator().next();
        // reset counter
        prod.updateRates();
        int numMessage = 20;
        int msgBytes = 50;
        int numThread = 4;
        this.sendMessagesFromMultiThreads(producer, topicName, numThread, numMessage, msgBytes);

        prod.updateRates();
        double rateIn = prod.getStats().getMsgRateIn();

        log.info("Msg rate in: {} msgs/s, total: {} msgs", rateIn, numMessage * numThread);
        if (preciseTopicPublishRateLimiterEnable) {
            // 0 <= topicPublishRateInMsg <= 20
            assertTrue(rateIn <= topicPublishRateInMsg + 10);
            assertTrue(rateIn >= topicPublishRateInMsg - 10);
        } else {
            assertTrue(rateIn >= topicPublishRateInMsg);
        }

        // Disable throttling
        topicPublishMsgRate.publishThrottlingRateInMsg = -1;
        if (isTopicLevel) {
            admin.topicPolicies().setPublishRate(topicName, topicPublishMsgRate);
        } else {
            admin.namespaces().setPublishRate(namespace, topicPublishMsgRate);
        }
        Awaitility.await().untilAsserted(() -> {
            assertEquals(topic.getTopicPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);
        });
        // reset counter
        prod.updateRates();
        for (int i = 0; i < numMessage; i++) {
            producer.send(new ProducerRecord<>(topicName, new byte[msgBytes])).get();
        }

        prod.updateRates();
        rateIn = prod.getStats().getMsgRateIn();

        log.info("Msg rate in: {} msg/s, total: {} msgs. (Disabled throttling)", rateIn, numMessage);
        assertTrue(rateIn >= numMessage);
    }

    protected KafkaProducer<Integer, byte[]> createKafkaProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return new KafkaProducer<>(props);
    }

    private void sendMessagesFromMultiThreads(KafkaProducer<Integer, byte[]> producer, String topicName,
                                              int numThread, int numMessage, int msgBytes) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(numThread);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int n = 0; n < numThread; n++) {
            final CompletableFuture<Void> future = new CompletableFuture<>();
            futures.add(future);
            executorService.submit(() -> {
                for (int i = 0; i < numMessage; i++) {
                    try {
                        producer.send(new ProducerRecord<>(topicName, new byte[msgBytes])).get();
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Send message failed.", e);
                        future.completeExceptionally(e);
                    }
                }
                future.complete(null);
            });
        }
        FutureUtil.waitForAll(futures).get();
        executorService.shutdown();
    }
}
