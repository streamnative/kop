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
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * test for kop prometheus metrics.
 */
@Slf4j
public class MetricsProviderTest extends KopProtocolHandlerTestBase{

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


    @Test(timeOut = 30000)
    public void testMetricsProvider() throws Exception {
        int partitionNumber = 1;
        String kafkaTopicName = "kopKafkaProducePulsarMetrics" + partitionNumber;

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

        // 2. consume messages with Kafka consumer
        @Cleanup
        KConsumer kConsumer = new KConsumer(kafkaTopicName, getKafkaBrokerPort());
        List<TopicPartition> topicPartitions = IntStream.range(0, partitionNumber)
                .mapToObj(i -> new TopicPartition(kafkaTopicName, i)).collect(Collectors.toList());
        kConsumer.getConsumer().assign(topicPartitions);

        int msgs = 0;
        while (msgs < totalMsgs) {
            if (log.isDebugEnabled()) {
                log.debug("start poll message: {}", msgs);
            }
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                if (log.isDebugEnabled()) {
                    log.debug("Kafka consumer get message: {}, key: {} at offset {}",
                            record.key(), record.value(), record.offset());
                }
                msgs++;
            }
        }
        Assert.assertEquals(msgs, totalMsgs);

        // commit offsets
        kConsumer.getConsumer().commitSync(Duration.ofSeconds(5));

        try {
            Thread.sleep(1000);
        } catch (Exception e) {

        }

        HttpClient httpClient = HttpClientBuilder.create().build();
        final String metricsEndPoint = pulsar.getWebServiceAddress() + "/metrics";
        HttpResponse response = httpClient.execute(new HttpGet(metricsEndPoint));
        InputStream inputStream = response.getEntity().getContent();
        InputStreamReader isReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(isReader);
        StringBuffer sb = new StringBuffer();
        String str;
        while ((str = reader.readLine()) != null) {
            if (str.contains("NaN") || str.contains("Infinity")) {
                continue;
            }
            sb.append(str);
        }

        // request stats
        Assert.assertTrue(sb.toString().contains("kop_server_REQUEST_QUEUE_SIZE"));
        Assert.assertTrue(sb.toString().contains("kop_server_REQUEST_QUEUED_LATENCY"));
        Assert.assertTrue(sb.toString().contains("kop_server_REQUEST_PARSE"));
        Assert.assertTrue(sb.toString().contains("kop_server_REQUEST_LATENCY"));
        Assert.assertTrue(sb.toString().contains("request=\"ApiVersions\""));
        Assert.assertTrue(sb.toString().contains("request=\"ListOffsets\""));
        Assert.assertTrue(sb.toString().contains("request=\"Fetch\""));
        Assert.assertTrue(sb.toString().contains("kop_server_REQUEST_LATENCY{success=\"true\",quantile=\"0.99\", "
                + "request=\"Produce\"}"));

        // response stats
        Assert.assertTrue(sb.toString().contains("kop_server_RESPONSE_QUEUE_SIZE"));
        Assert.assertTrue(sb.toString().contains("kop_server_RESPONSE_BLOCKED_TIMES"));
        Assert.assertTrue(sb.toString().contains("kop_server_RESPONSE_BLOCKED_LATENCY"));

        // produce stats
        Assert.assertTrue(sb.toString().contains("kop_server_PRODUCE_ENCODE"));
        Assert.assertTrue(sb.toString().contains("kop_server_MESSAGE_PUBLISH"));
        Assert.assertTrue(sb.toString().contains("kop_server_MESSAGE_QUEUED_LATENCY"));

        // fetch stats
        Assert.assertTrue(sb.toString().contains("kop_server_PREPARE_METADATA"));
        Assert.assertTrue(sb.toString().contains("kop_server_MESSAGE_READ"));
        Assert.assertTrue(sb.toString().contains("kop_server_FETCH_DECODE"));

        // consumer stats
        Assert.assertTrue(sb.toString().contains("kop_server_MESSAGE_OUT{group=\"DemoKafkaOnPulsarConsumer\","
                + "partition=\"0\",topic=\"kopKafkaProducePulsarMetrics1\"} 10"));
        Assert.assertTrue(sb.toString().contains("kop_server_BYTES_OUT{group=\"DemoKafkaOnPulsarConsumer\","
                + "partition=\"0\",topic=\"kopKafkaProducePulsarMetrics1\"} 1130"));
        Assert.assertTrue(sb.toString().contains("kop_server_BYTES_OUT"));
    }
}
