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
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KopEventManagerTest extends KopProtocolHandlerTestBase {
    private AdminClient adminClient;
    private KafkaProducer<String, String> kafkaProducer;
    private String broker;
    private final String topic1 = "test-topic1";
    private final String topic2 = "test-topic2";
    private final String topic3 = "test-topic3";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        final EndPoint plainEndPoint = getPlainEndPoint();
        this.broker = plainEndPoint.getHostname() + ":" + plainEndPoint.getPort();
        Properties adminPro = new Properties();
        adminPro.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        this.adminClient = AdminClient.create(adminPro);
        final Properties producerPro = new Properties();
        producerPro.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        producerPro.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerPro.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        this.kafkaProducer = new KafkaProducer<>(producerPro);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        adminClient.close();
        kafkaProducer.close();
        super.internalCleanup();
    }

    @Test(timeOut = 6000)
    public void testOneTopicGroupState() throws Exception {
        // 1. create topics
        createTopics(Collections.singletonList(topic1));
        // 2. send messages
        sendOneMessages(topic1);
        // 3. check group state which only consumed one topic
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final String groupId1 = "test-group1";
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId1);

        final KafkaConsumer<String, String> kafkaConsumer1 = new KafkaConsumer<>(properties);
        kafkaConsumer1.subscribe(Collections.singletonList(topic1));
        ConsumerRecords<String, String> records = kafkaConsumer1.poll(Duration.ofMillis(500));
        assertEquals(records.count(), 1);

        // 4. check group state must be Stable
        Map<String, ConsumerGroupDescription> describeGroup1 =
                adminClient.describeConsumerGroups(Collections.singletonList(groupId1))
                        .all()
                        .get(1000, TimeUnit.MILLISECONDS);
        assertTrue(describeGroup1.containsKey(groupId1));
        assertEquals(ConsumerGroupState.STABLE, describeGroup1.get(groupId1).state());
        // 5. close consumer1
        kafkaConsumer1.close();
        // 6. check group state must be Empty
        Map<String, ConsumerGroupDescription> describeGroup2 =
                adminClient.describeConsumerGroups(Collections.singletonList(groupId1))
                        .all()
                        .get(1000, TimeUnit.MILLISECONDS);
        assertTrue(describeGroup1.containsKey(groupId1));
        assertEquals(ConsumerGroupState.EMPTY, describeGroup2.get(groupId1).state());
        // 7. delete topic1
        adminClient.deleteTopics(Collections.singletonList(topic1));
        // 8. describe group who only consume topic1 which have been deleted
        // check group state must be Dead
        retryUntilStateDead(groupId1, 5);
    }

    @Test(timeOut = 6000)
    public void testTwoTopicsGroupState() throws Exception {
        // 1. create topics
        createTopics(Arrays.asList(topic2, topic3));
        // 2. send messages
        sendOneMessages(topic2);
        sendOneMessages(topic3);

        // 3. check group state which consumed two topics
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        final String groupId2 = "test-group2";
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId2);
        final KafkaConsumer<String, String> kafkaConsumer2 = new KafkaConsumer<>(properties);
        kafkaConsumer2.subscribe(Arrays.asList(topic2, topic3));
        int consumeCount = 0;
        while (consumeCount < 2) {
            ConsumerRecords<String, String> records = kafkaConsumer2.poll(Duration.ofMillis(500));
            consumeCount += records.count();
        }
        // 4. check group state must be Stable
        Map<String, ConsumerGroupDescription> describeGroup1 =
                adminClient.describeConsumerGroups(Collections.singletonList(groupId2))
                        .all()
                        .get(1000, TimeUnit.MILLISECONDS);
        assertTrue(describeGroup1.containsKey(groupId2));
        assertEquals(ConsumerGroupState.STABLE, describeGroup1.get(groupId2).state());

        // 5. close consumer2
        kafkaConsumer2.close();

        // 6. check group state must be Empty
        Map<String, ConsumerGroupDescription> describeGroup2 =
                adminClient.describeConsumerGroups(Collections.singletonList(groupId2))
                        .all()
                        .get(1000, TimeUnit.MILLISECONDS);
        assertTrue(describeGroup2.containsKey(groupId2));
        assertEquals(ConsumerGroupState.EMPTY, describeGroup2.get(groupId2).state());

        // 7. delete topic2 and topic3
        List<String> deleteTopics = Lists.newArrayList();
        deleteTopics.add(topic2);
        deleteTopics.add(topic3);
        adminClient.deleteTopics(deleteTopics);
        // 8. check group state must be Dead
        retryUntilStateDead(groupId2, 5);
    }

    private void createTopics(List<String> topics) throws ExecutionException, InterruptedException {
        List<NewTopic> topicsList = Lists.newArrayList();
        topics.forEach(
                topic -> {
                    NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                    topicsList.add(newTopic);
                }
        );
        adminClient.createTopics(topicsList).all().get();
    }

    private void sendOneMessages(String topic) {
        kafkaProducer.send(new ProducerRecord<>(topic, null, "test-value"));
    }

    private void retryUntilStateDead(String groupId, int timeOutSec) throws Exception {
        long startTimeMs = System.currentTimeMillis();
        long deadTimeMs = startTimeMs + timeOutSec * 1000L;

        Map<String, ConsumerGroupDescription> describeGroup = null;

        while (System.currentTimeMillis() < deadTimeMs) {
            describeGroup = adminClient.describeConsumerGroups(Collections.singletonList(groupId))
                    .all()
                    .get(1000, TimeUnit.MILLISECONDS);
            assertTrue(describeGroup.containsKey(groupId));
            if (describeGroup.get(groupId).state().name().equals("DEAD")) {
                break;
            }
        }

        assertEquals(ConsumerGroupState.DEAD, describeGroup.get(groupId).state());

    }

}
