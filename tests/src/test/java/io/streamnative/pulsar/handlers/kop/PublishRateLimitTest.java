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


import com.google.common.collect.Sets;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.PublishRateLimiter;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test for ratelimit.
 * verify InternalServerCnx.enableCnxAutoRead worked well. should not meet NPE
 */
@Slf4j
public class PublishRateLimitTest extends KopProtocolHandlerTestBase {

    @Override
    protected void resetConfig() {
        super.resetConfig();
        // set to easy to trigger rate limit.
        this.conf.setBrokerPublisherThrottlingTickTimeMillis(1);
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

    @Test(timeOut = 20000)
    public void testBrokerPublishByteThrottling() throws Exception {
        String topicName = "kopBrokerPublishByteThrottling";
        String pulsarTopicName = "persistent://public/default/" + topicName + "-partition-0";

        // NOTE: KoP doesn't support non-partitioned topics, so we need to create it first to avoid Pulsar producer
        //   create a non-partitioned topic automatically.
        admin.topics().createPartitionedTopic(topicName, 1);

        // 1. produce message with Kafka producer.
        @Cleanup
        KProducer kProducer = new KProducer(topicName, false, getKafkaBrokerPort());

        long byteRate = 400;
        // create producer and topic
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
            .topic(pulsarTopicName)
            .enableBatching(false)
            .maxPendingMessages(30000).create();
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService()
            .getTopicIfExists(pulsarTopicName).get().get();
        // (1) verify byte-rate is -1 disabled
        Assert.assertEquals(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // enable throttling
        admin.brokers()
            .updateDynamicConfiguration("brokerPublisherThrottlingMaxByteRate", Long.toString(byteRate));

        retryStrategically(
            (test) ->
                (topic.getBrokerPublishRateLimiter() != PublishRateLimiter.DISABLED_RATE_LIMITER),
            5,
            200);

        log.info("Get broker configuration after enable: brokerTick {},  MaxMessageRate {}, MaxByteRate {}",
            pulsar.getConfiguration().getBrokerPublisherThrottlingTickTimeMillis(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxMessageRate(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxByteRate());

        Assert.assertNotEquals(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        Producer prod = topic.getProducers().values().iterator().next();
        // reset counter
        prod.updateRates();
        int numMessage = 20;
        int msgBytes = 80;

        for (int i = 0; i < numMessage; i++) {
            producer.send(new byte[msgBytes]);
        }
        // calculate rates and due to throttling rate should be < total per-second
        prod.updateRates();
        double rateIn = prod.getStats().msgThroughputIn;
        log.info("1-st byte rate in: {}, total: {} ", rateIn, numMessage * msgBytes);
        Assert.assertTrue(rateIn < numMessage * msgBytes);

        String messageStrPrefix = "Message_Kop_KafkaProduceKafkaConsume_";
        for (int i = 0; i < numMessage; i++) {
            String messageStr = messageStrPrefix + i;
            ProducerRecord record = new ProducerRecord<>(
                topicName,
                i,
                messageStr);
            kProducer.getProducer()
                .send(record)
                .get();
            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
            }
        }

        // disable throttling
        admin.brokers()
            .updateDynamicConfiguration("brokerPublisherThrottlingMaxByteRate", Long.toString(0));
        retryStrategically((test) ->
                topic.getBrokerPublishRateLimiter().equals(PublishRateLimiter.DISABLED_RATE_LIMITER),
            5,
            200);

        log.info("Get broker configuration after disable: brokerTick {},  MaxMessageRate {}, MaxByteRate {}",
            pulsar.getConfiguration().getBrokerPublisherThrottlingTickTimeMillis(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxMessageRate(),
            pulsar.getConfiguration().getBrokerPublisherThrottlingMaxByteRate());

        Assert.assertEquals(topic.getBrokerPublishRateLimiter(), PublishRateLimiter.DISABLED_RATE_LIMITER);

        // reset counter
        prod.updateRates();
        for (int i = 0; i < numMessage; i++) {
            producer.send(new byte[msgBytes]);
        }

        prod.updateRates();
        rateIn = prod.getStats().msgThroughputIn;
        log.info("2-nd byte rate in: {}, total: {} ", rateIn, numMessage * msgBytes);
        Assert.assertTrue(rateIn > numMessage * msgBytes);

        producer.close();
    }

}
