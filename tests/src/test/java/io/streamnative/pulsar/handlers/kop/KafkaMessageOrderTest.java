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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test for Different kafka produce messages.
 */
@Slf4j
public class KafkaMessageOrderTest extends KopProtocolHandlerTestBase {

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

    @Test(timeOut = 20000)
    public void testKafkaProducePulsarConsumeMessageOrder() throws Exception {
        String topicName = "kopKafkaProducePulsarConsumeMessageOrder";
        String pulsarTopicName = "persistent://public/default/" + topicName;

        // create partitioned topic with 1 partition.
        pulsar.getAdminClient().topics().createPartitionedTopic(topicName, 1);

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(pulsarTopicName)
            .subscriptionName("test_k_producer_k_pconsumer_order_sub")
            .subscribe();

        // 1. produce message with Kafka producer.
        @Cleanup
        KProducer kProducer = new KProducer(topicName, false, getKafkaBrokerPort());

        int totalMsgs = 10;
        String messageStrPrefix = "Message_Kop_KafkaProducePulsarConsumeOrder_";

        List<Future<RecordMetadata>> futures = Lists.newArrayListWithExpectedSize(totalMsgs);

        // send in sync mode
        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            ProducerRecord record = new ProducerRecord<>(
                topicName,
                i,
                messageStr);

            futures.add(kProducer.getProducer().send(record));

            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
            }
        }

        retryStrategically((test) ->
                !futures.stream().anyMatch(future -> !future.isDone()),
            5,
            200);

        // 2. Consume messages use Pulsar client Consumer. verify content and key and headers
        Message<byte[]> msg = null;
        for (int i = 0; i < totalMsgs; i++) {
            msg = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            Integer key = kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey()));
            assertEquals(messageStrPrefix + key.toString(), new String(msg.getValue()));

            if (log.isDebugEnabled()) {
                log.info("Pulsar consumer get i: {} message: {}, key: {}",
                    i,
                    new String(msg.getData()),
                    kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey())).toString());
            }
            assertEquals(i, key.intValue());

            consumer.acknowledge(msg);
        }

        // verify have received all messages
        msg = consumer.receive(100, TimeUnit.MILLISECONDS);
        assertNull(msg);
    }


}
