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
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.policies.data.ClusterData;
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
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    @Test(timeOut = 20000)
    public void testProduceRequest() throws Exception {
        String topicName = "kopTopicProduce";

        // create partitioned topic.
        kafkaService.getAdminClient().topics().createPartitionedTopic(topicName, 1);

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic("persistent://public/default/" + topicName + PARTITIONED_TOPIC_SUFFIX + 0)
            .subscriptionName("test_producer_sub").subscribe();


        // 1. produce message with Kafka producer.
        Producer producer = new Producer(topicName, false);

        int messageNo = 0;
        int totalMsgs = 10;

        while (messageNo < totalMsgs) {
            String messageStr = "Message_Kop_" + messageNo;

            try {
                producer.getProducer().send(new ProducerRecord<>(topicName,
                    messageNo,
                    messageStr)).get();
                log.info("Sent message: (" + messageNo + ", " + messageStr + ")");
            } catch (Exception e) {
                e.printStackTrace();
            }
            ++messageNo;
        }

        assertEquals(totalMsgs, messageNo);

        Message<byte[]> msg = null;

        // 2. Consume messages use Pulsar client Consumer.
        for (int i = 0; i < totalMsgs; i++) {
            msg = consumer.receive(100, TimeUnit.MILLISECONDS);
            log.info("Pulsar consumer get message: {}", new String(msg.getData()));
            consumer.acknowledge(msg);
        }

        // verify have received all messages
        msg = consumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);
    }
}
