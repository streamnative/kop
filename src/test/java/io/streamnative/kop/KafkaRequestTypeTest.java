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
package io.streamnative.kop;


import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test for Different kafka request type.
 */
@Slf4j
public class KafkaRequestTypeTest extends MockKafkaServiceBaseTest {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        // so that clients can test short names
        admin.clusters().createCluster("test", new ClusterData("http://127.0.0.1:" + brokerWebservicePort));

        admin.tenants().createTenant("public",
            new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
        admin.namespaces().setRetention("public/default",
            new RetentionPolicies(20, 100));
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000)
    public void testKafkaProducePulsarConsume() throws Exception {
        String topicName = "kopKafkaProducePulsarConsume";

        // create partitioned topic.
        kafkaService.getAdminClient().topics().createPartitionedTopic(topicName, 1);

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic("persistent://public/default/" + topicName + PARTITIONED_TOPIC_SUFFIX + 0)
            .subscriptionName("test_producer_sub").subscribe();

        // 1. produce message with Kafka producer.
        KProducer kProducer = new KProducer(topicName, false);

        int totalMsgs = 10;
        String messageStrPrefix = "Message_Kop_KafkaProducePulsarConsume_";

        for (int i = 0; i < totalMsgs; i ++) {
            String messageStr = messageStrPrefix + i;
            try {
                kProducer.getProducer()
                    .send(new ProducerRecord<>(
                            topicName,
                            i,
                            messageStr))
                    .get();
                log.debug("Sent message: (" + i + ", " + messageStr + ")");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 2. Consume messages use Pulsar client Consumer. verify content and key
        Message<byte[]> msg = null;
        for (int i = 0; i < totalMsgs; i++) {
            msg = consumer.receive(100, TimeUnit.MILLISECONDS);

            assertEquals(messageStrPrefix + i, new String(msg.getValue()));
            assertEquals(Integer.valueOf(i), kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey())));

            log.debug("Pulsar consumer get message: {}, key: {}",
                new String(msg.getData()), kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey())).toString());
            consumer.acknowledge(msg);
        }

        // verify have received all messages
        msg = consumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);
    }

    @Test(timeOut = 20000)
    public void testKafkaProduceKafkaConsume() throws Exception {
        String topicName = "kopKafkaProduceKafkaConsume";

        // create partitioned topic.
        kafkaService.getAdminClient().topics().createPartitionedTopic(topicName, 1);

        // 1. produce message with Kafka producer.
        int totalMsgs = 10;
        String messageStrPrefix = "Message_Kop_KafkaProduceKafkaConsume_";

        KProducer producer = new KProducer(topicName, false);

        for (int i = 0; i < totalMsgs; i ++) {
            String messageStr = messageStrPrefix + i;
            try {
                producer.getProducer()
                    .send(new ProducerRecord<>(
                        topicName,
                        i,
                        messageStr))
                    .get();
                log.debug("Sent message: (" + i + ", " + messageStr + ")");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // 2. use kafka consumer to consume.
        KConsumer kConsumer = new KConsumer(topicName);
        kConsumer.getConsumer().assign(Collections.singletonList(new TopicPartition(topicName, 0)));

        for (int i = 0; i < totalMsgs; i ++) {
            log.debug("start poll: {}", i);
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                assertEquals(messageStrPrefix + i, record.value());
                assertEquals(Integer.valueOf(i), record.key());

                log.debug("Received message: {}, {} at offset {}",
                    record.key(), record.value(), record.offset());
            }
        }
    }

    @Test(timeOut = 20000)
    public void testPulsarProduceKafkaConsume() throws Exception {
        String topicName = "kopPulsarProduceKafkaConsume";
        String pulsarTopicName = "persistent://public/default/" + topicName + PARTITIONED_TOPIC_SUFFIX + 0;

        // create partitioned topic.
        kafkaService.getAdminClient().topics().createPartitionedTopic(topicName, 1);

        // create a consumer to retention the data?
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic("persistent://public/default/" + topicName + PARTITIONED_TOPIC_SUFFIX + 0)
            .subscriptionName("test_producer_sub").subscribe();


        // 1. use pulsar producer to produce.
        int totalMsgs = 10;
        String messageStrPrefix = "Message_Kop_PulsarProduceKafkaConsume_";

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
            .topic(pulsarTopicName)
            .enableBatching(false);

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < totalMsgs; i++) {
            String message = messageStrPrefix + i;
            producer.newMessage()
                .keyBytes(kafkaIntSerialize(Integer.valueOf(i)))
                .value(message.getBytes())
                .send();
        }

        // 2. use kafka consumer to consume.
        KConsumer kConsumer = new KConsumer(topicName);
        kConsumer.getConsumer().assign(Collections.singletonList(new TopicPartition(topicName, 0)));

        for (int i = 0; i < totalMsgs; i ++) {
            log.debug("start poll: {}", i);
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                assertEquals(messageStrPrefix + i, record.value());
                assertEquals(Integer.valueOf(i), record.key());

                log.debug("Received message: {}, {} at offset {}",
                    record.key(), record.value(), record.offset());
            }
        }
    }

}
