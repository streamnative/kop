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
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;

import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;

import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for Different kafka produce messages.
 */
@Slf4j
public class KafkaMessageOrderTest extends KopProtocolHandlerTestBase {

    @DataProvider(name = "batchSizeList")
    public static Object[][] batchSizeList() {
        // For the messageStrPrefix in testKafkaProduceMessageOrder(), 100 messages will be split to 50, 34, 25, 20
        // batches associated with following batch.size config.
        return new Object[][] { { 200 }, { 250 }, { 300 }, { 350 } };
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

    @Test(timeOut = 20000, dataProvider = "batchSizeList")
    public void testKafkaProduceMessageOrder(int batchSize) throws Exception {
        String topicName = "kopKafkaProducePulsarConsumeMessageOrder";
        String pulsarTopicName = "persistent://public/default/" + topicName;

        // create partitioned topic with 1 partition.
        pulsar.getAdminClient().topics().createPartitionedTopic(topicName, 1);

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(pulsarTopicName)
            .subscriptionName("testKafkaProduce-PulsarConsume")
            .subscribe();

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize); // avoid all messages are in a single batch

        // 1. produce message with Kafka producer.
        @Cleanup
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        int totalMsgs = 100;
        String messageStrPrefix = "Message_Kop_KafkaProducePulsarConsumeOrder_";

        Map<Long, Set<Long>> ledgerToEntrySet = new ConcurrentHashMap<>();
        for (int i = 0; i < totalMsgs; i++) {
            final int index = i;
            producer.send(new ProducerRecord<>(topicName, i, messageStrPrefix + i), (recordMetadata, e) -> {
                assertNull(e);
                MessageIdImpl id = (MessageIdImpl) MessageIdUtils.getMessageId(recordMetadata.offset());
                log.info("Success write message {} to {} ({}, {})", index, recordMetadata.offset(),
                        id.getLedgerId(), id.getEntryId());
                ledgerToEntrySet.computeIfAbsent(id.getLedgerId(), key -> Collections.synchronizedSet(new HashSet<>()))
                        .add(id.getEntryId());
            });
        }

        // 2. Consume messages use Pulsar client Consumer.
        Message<byte[]> msg = null;
        for (int i = 0; i < totalMsgs; i++) {
            msg = consumer.receive(1000, TimeUnit.MILLISECONDS);
            assertNotNull(msg);
            Integer key = kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey()));
            assertEquals(messageStrPrefix + key.toString(), new String(msg.getValue()));

            if (log.isDebugEnabled()) {
                log.debug("Pulsar consumer get i: {} message: {}, key: {}",
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

        final AtomicInteger numEntries = new AtomicInteger(0);
        ledgerToEntrySet.forEach((ledgerId, entrySet) -> numEntries.set(numEntries.get() + entrySet.size()));
        log.info("Successfully write {} entries of {} messages to bookie", numEntries.get(), totalMsgs);
        assertTrue(numEntries.get() > 1 && numEntries.get() < totalMsgs);

        // 3. Consume messages use Kafka consumer.
        @Cleanup
        KConsumer kConsumer = new KConsumer(topicName, getKafkaBrokerPort(), "testKafkaProduce-KafkaConsume");
        kConsumer.getConsumer().subscribe(Collections.singleton(topicName));
        for (int i = 0; i < totalMsgs; ) {
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            if (log.isDebugEnabled()) {
                for (ConsumerRecord<Integer, String> record : records) {
                    log.debug("Kafka consumer get i: {} message: {}, key: {}", i, record.value(), record.key());
                    assertEquals(record.key().intValue(), i);
                    i++;
                }
            } else {
                i += records.count();
            }
        }
    }


}
