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
package io.streamnative.kop.coordinator.group;

import static io.streamnative.kop.KafkaProtocolHandler.PLAINTEXT_PREFIX;
import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import io.streamnative.kop.KafkaService;
import io.streamnative.kop.KafkaServiceConfiguration;
import io.streamnative.kop.MockKafkaServiceBaseTest;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import org.apache.bookkeeper.test.PortManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;



/**
 * Unit test {@link GroupCoordinator}.
 */
public class DistributedGroupCoordinatorTest extends MockKafkaServiceBaseTest {

    protected KafkaServiceConfiguration conf1;
    protected KafkaServiceConfiguration conf2;
    protected KafkaService kafkaService1;
    protected KafkaService kafkaService2;

    protected int primaryBrokerWebservicePort;
    protected int secondaryBrokerWebservicePort;
    protected int primaryBrokerPort;
    protected int secondaryBrokerPort;
    protected int primaryKafkaBrokerPort;
    protected int secondaryKafkaBrokerPort;

    protected int offsetsTopicNumPartitions;

    private static final Logger log = LoggerFactory.getLogger(DistributedGroupCoordinatorTest.class);

    protected KafkaServiceConfiguration resetConfig(int brokerPort, int webPort, int kafkaPort) {
        KafkaServiceConfiguration kConfig = new KafkaServiceConfiguration();
        kConfig.setBrokerServicePort(Optional.ofNullable(brokerPort));
        kConfig.setWebServicePort(Optional.ofNullable(webPort));
        kConfig.setListeners(PLAINTEXT_PREFIX + "localhost:" + kafkaPort);

        kConfig.setOffsetsTopicNumPartitions(offsetsTopicNumPartitions);
        kConfig.setEnableGroupCoordinator(true);

        kConfig.setAdvertisedAddress("localhost");
        kConfig.setClusterName(configClusterName);
        kConfig.setManagedLedgerCacheSizeMB(8);
        kConfig.setActiveConsumerFailoverDelayTimeMillis(0);
        kConfig.setDefaultNumberOfNamespaceBundles(2);
        kConfig.setZookeeperServers("localhost:2181");
        kConfig.setConfigurationStoreServers("localhost:3181");
        kConfig.setEnableGroupCoordinator(true);
        kConfig.setAuthenticationEnabled(false);
        kConfig.setAuthorizationEnabled(false);
        kConfig.setAllowAutoTopicCreation(true);
        kConfig.setAllowAutoTopicCreationType("partitioned");
        kConfig.setBrokerDeleteInactiveTopicsEnabled(false);

        return kConfig;
    }

    @Override
    protected void resetConfig() {
        offsetsTopicNumPartitions = 8;
        primaryBrokerWebservicePort = PortManager.nextFreePort();
        secondaryBrokerWebservicePort = PortManager.nextFreePort();
        primaryBrokerPort = PortManager.nextFreePort();
        secondaryBrokerPort = PortManager.nextFreePort();
        primaryKafkaBrokerPort = PortManager.nextFreePort();
        secondaryKafkaBrokerPort = PortManager.nextFreePort();
        conf1 = resetConfig(
            primaryBrokerPort,
            primaryBrokerWebservicePort,
            primaryKafkaBrokerPort);
        conf2 = resetConfig(
            secondaryBrokerPort,
            secondaryBrokerWebservicePort,
            secondaryKafkaBrokerPort);
        conf = conf1;

        brokerPort = primaryBrokerPort;
        brokerWebservicePort = primaryBrokerWebservicePort;
        kafkaBrokerPort = primaryKafkaBrokerPort;

        log.info("Ports --  broker1: {}, brokerWeb1:{}, kafka1: {}",
            primaryBrokerPort, primaryBrokerWebservicePort, primaryKafkaBrokerPort);
        log.info("Ports --  broker2: {}, brokerWeb2:{}, kafka2: {}\n",
            secondaryBrokerPort, secondaryBrokerWebservicePort, secondaryKafkaBrokerPort);
    }

    @Override
    protected void startBroker() throws Exception {
        this.kafkaService1 = startBroker(conf1);
        this.kafkaService = kafkaService1;
        this.kafkaService2 = startBroker(conf2);
    }

    @Override
    protected void stopBroker() throws Exception {
        kafkaService1.close();
        kafkaService2.close();
    }

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

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

        List<String> brokers =  admin.brokers().getActiveBrokers(configClusterName);
        Assert.assertEquals(brokers.size(), 2);
        log.info("broker1: {} broker2: {}", brokers.get(0), brokers.get(1));
    }


    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        log.info("--- Shutting down ---");
        super.internalCleanup();
    }

    protected void kafkaPublishMessage(KProducer kProducer, int numMessages, String messageStrPrefix) throws Exception {
        for (int i = 0; i < numMessages; i++) {
            String messageStr = messageStrPrefix + i;
            ProducerRecord record = new ProducerRecord<>(
                kProducer.getTopic(),
                i,
                messageStr);

            kProducer.getProducer()
                .send(record);
            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message with header: ({}, {})", i, messageStr);
            }
        }
    }

    protected void kafkaConsumeCommitMessage(KConsumer kConsumer,
                                             int numMessages,
                                             String messageStrPrefix,
                                             List<TopicPartition> topicPartitions) {
        kConsumer.getConsumer().assign(topicPartitions);
        int i = 0;
        while (i < numMessages) {
            if (log.isDebugEnabled()) {
                log.debug("start poll message: {}", i);
            }
            ConsumerRecords<Integer, String> records1 = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records1) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());

                if (log.isDebugEnabled()) {
                    log.debug("Kafka consumer get message: {}, key: {} at offset {}",
                        record.key(), record.value(), record.offset());
                }
                i++;
            }
        }
        kConsumer.getConsumer().commitSync();
        assertEquals(i, numMessages);
    }

    @Test(timeOut = 40000)
    public void testMutiCoordinator() throws Exception {
        int partitionNumber = 10;
        String kafkaTopicName = "kopKafkaProduceKafkaConsume" + partitionNumber;
        String pulsarTopicName = "persistent://public/default/" + kafkaTopicName;

        // create partitioned topic.
        kafkaService.getAdminClient().topics().createPartitionedTopic(kafkaTopicName, partitionNumber);

        // 1. produce message with Kafka producer.
        int totalMsgs = 50;
        String messageStrPrefix = "Message_Kop_KafkaProduceKafkaConsume_" + partitionNumber + "_";
        @Cleanup
        KProducer kProducer = new KProducer(kafkaTopicName, false, getKafkaBrokerPort());
        kafkaPublishMessage(kProducer, totalMsgs, messageStrPrefix);

        // 2. create 2 kafka consumer from different consumer group.
        //    consume data and commit offsets for 2 consumer group.
        @Cleanup
        KConsumer kConsumer1 = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), "consumer-group-1");
        @Cleanup
        KConsumer kConsumer2 = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), "consumer-group-2");
        @Cleanup
        KConsumer kConsumer3 = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), "consumer-group-3");
        @Cleanup
        KConsumer kConsumer4 = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), "consumer-group-4");

        List<TopicPartition> topicPartitions = IntStream.range(0, partitionNumber)
            .mapToObj(i -> new TopicPartition(kafkaTopicName, i)).collect(Collectors.toList());
        log.info("Partition size: {}, will consume and commitOffset for 2 consumers", topicPartitions.size());

        kafkaConsumeCommitMessage(kConsumer1, totalMsgs, messageStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer2, totalMsgs, messageStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer3, totalMsgs, messageStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer4, totalMsgs, messageStrPrefix, topicPartitions);

        // 3. use a map for serving broker and topics <broker, topics>, verify both broker has messages produced.
        Map<String, List<String>> topicMap = Maps.newHashMap();
        for (int ii = 0; ii < partitionNumber; ii++) {
            String topicName = pulsarTopicName + PARTITIONED_TOPIC_SUFFIX + ii;
            String result = admin.lookups().lookupTopic(topicName);
            topicMap.putIfAbsent(result, Lists.newArrayList());
            topicMap.get(result).add(topicName);
            log.info("serving broker for topic {} is {}", topicName, result);
        }
        assertTrue(topicMap.size() == 2);

        AtomicInteger numberTopic = new AtomicInteger(0);
        topicMap.values().stream().forEach(list -> numberTopic.addAndGet(list.size()));
        assertTrue(numberTopic.get() == partitionNumber);

        final PartitionedTopicStats topicStats = admin.topics().getPartitionedStats(pulsarTopicName, true);
        log.info("PartitionedTopicStats for topic {} : {}",  pulsarTopicName, new Gson().toJson(topicStats));

        topicMap.forEach((broker, topics) -> {
            AtomicLong brokerStorageSize = new AtomicLong(0);
            topics.forEach(topic -> {
                brokerStorageSize.addAndGet(topicStats.partitions.get(topic).storageSize);
            });
            log.info("get data topics served by broker {}, broker storage size: {}",  broker, brokerStorageSize.get());
            assertTrue(brokerStorageSize.get() > 0L);
        });

        // 4. use a map for serving broker and offset topics <broker, topics>,
        //    verify both broker has offset topic served.
        Map<String, List<String>> offsetTopicMap = Maps.newHashMap();
        String offsetsTopicName = "persistent://public/default/" + GROUP_METADATA_TOPIC_NAME;
        for (int ii = 0; ii < offsetsTopicNumPartitions; ii++) {
            String offsetsTopic = offsetsTopicName + PARTITIONED_TOPIC_SUFFIX + ii;
            String result = admin.lookups().lookupTopic(offsetsTopic);
            offsetTopicMap.putIfAbsent(result, Lists.newArrayList());
            offsetTopicMap.get(result).add(offsetsTopic);
            log.info("serving broker for offset topic {} is {}", offsetsTopic, result);
        }
        assertTrue(offsetTopicMap.size() == 2);

        assertTrue(kafkaService1.getGroupCoordinator().getOffsetsProducers().size() > 0);
        assertTrue(kafkaService2.getGroupCoordinator().getOffsetsProducers().size() > 0);
        log.info("size1: {}, size2: {}",
            kafkaService1.getGroupCoordinator().getOffsetsProducers().size(),
            kafkaService2.getGroupCoordinator().getOffsetsProducers().size());

        // no more records
        ConsumerRecords<Integer, String> records = kConsumer1.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        records = kConsumer2.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        records = kConsumer3.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        records = kConsumer4.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
    }

}
