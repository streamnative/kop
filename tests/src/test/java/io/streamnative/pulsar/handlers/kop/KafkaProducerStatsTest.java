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
import com.google.common.collect.Sets;
import java.util.Properties;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for producer stats.
 */
@Slf4j
public class KafkaProducerStatsTest extends KopProtocolHandlerTestBase {

    @DataProvider(name = "partitionsAndBatch")
    public static Object[][] partitionsAndBatch() {
        return new Object[][] {
                { 1, true },
                { 1, false },
                { 7, true },
                { 7, false }
        };
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        } else {
            admin.clusters().updateCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        }

        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        } else {
            admin.tenants().updateTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        }
        if (!admin.namespaces().getNamespaces("public").contains("public/default")) {
            admin.namespaces().createNamespace("public/default");
            admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/default",
                    new RetentionPolicies(60, 1000));
        }
        if (!admin.namespaces().getNamespaces("public").contains("public/__kafka")) {
            admin.namespaces().createNamespace("public/__kafka");
            admin.namespaces().setNamespaceReplicationClusters("public/__kafka", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/__kafka",
                    new RetentionPolicies(-1, -1));
        }
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000, dataProvider = "partitionsAndBatch")
    public void testKafkaProducePulsarMetrics(int partitionNumber, boolean isBatch) throws Exception {
        String kafkaTopicName = "kopKafkaProducePulsarMetrics" + partitionNumber;
        String pulsarTopicName = "persistent://public/default/" + kafkaTopicName;

        // create partitioned topic.
        admin.topics().createPartitionedTopic(kafkaTopicName, partitionNumber);

        // 1. produce message with Kafka producer.
        @Cleanup
        KProducer kProducer = new KProducer(kafkaTopicName, false, getKafkaBrokerPort());

        int totalMsgs = 10;

        String messageStrPrefix = "Message_Kop_KafkaProducePulsarConsume_"  + partitionNumber + "_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            ProducerRecord record = new ProducerRecord<>(
                    kafkaTopicName,
                    i,
                    messageStr);

            kProducer.getProducer().send(record).get();

            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
            }
        }

        long msgInCounter = admin.topics().getPartitionedStats(pulsarTopicName, false).msgInCounter;
        assertEquals(msgInCounter, totalMsgs);
        long bytesInCounter = admin.topics().getPartitionedStats(pulsarTopicName, false).bytesInCounter;
        assertNotEquals(bytesInCounter, 0);
    }

    @Test(timeOut = 20000)
    public void testKafkaProducePulsarRates() throws Exception {
        String topicName = "testBrokerPublishMetrics";
        String pulsarTopicName = "persistent://public/default/" + topicName + "-partition-0";

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 1);

        // 1. produce message with Kafka producer.
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaBrokerPort);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        @Cleanup
        KafkaProducer<byte[], byte[]> producer = new org.apache.kafka.clients.producer.KafkaProducer(properties);
        int numMessages = 100;
        int msgBytes = 80;
        for (int i = 0; i < numMessages; i++) {
            producer.send(new ProducerRecord<>(topicName, new byte[msgBytes])).get();
        }

        // create producer and topic
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(pulsarTopicName).get().get();

        Producer prod = topic.getProducers().values().iterator().next();
        // reset counter
        prod.updateRates();
        double msgThroughputIn = prod.getStats().msgThroughputIn;
        double msgRateIn = prod.getStats().msgRateIn;
        double averageMsgSize = prod.getStats().averageMsgSize;
        Assert.assertTrue(msgThroughputIn > numMessages * msgBytes);
        Assert.assertTrue(msgRateIn > numMessages);
        Assert.assertTrue(averageMsgSize > msgBytes);

        producer.close();
    }
}
