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

import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;
import static org.junit.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Test KoP cluster mode.
 * Will setup 2 brokers and do the tests.
 */
public class DistributedClusterTest extends KopProtocolHandlerTestBase {

    protected KafkaServiceConfiguration conf1;
    protected KafkaServiceConfiguration conf2;
    protected PulsarService pulsarService1;
    protected PulsarService pulsarService;

    protected int primaryBrokerWebservicePort;
    protected int secondaryBrokerWebservicePort;
    protected int primaryBrokerPort;
    protected int secondaryBrokerPort;
    protected int primaryKafkaBrokerPort;
    protected int secondaryKafkaBrokerPort;

    protected int offsetsTopicNumPartitions;

    private static final Logger log = LoggerFactory.getLogger(DistributedClusterTest.class);

    protected KafkaServiceConfiguration resetConfig(int brokerPort, int webPort, int kafkaPort) {
        KafkaServiceConfiguration kConfig = new KafkaServiceConfiguration();
        kConfig.setBrokerServicePort(Optional.ofNullable(brokerPort));
        kConfig.setWebServicePort(Optional.ofNullable(webPort));
        kConfig.setListeners(PLAINTEXT_PREFIX + "localhost:" + kafkaPort);

        kConfig.setOffsetsTopicNumPartitions(offsetsTopicNumPartitions);

        kConfig.setAdvertisedAddress("localhost");
        kConfig.setClusterName(configClusterName);
        kConfig.setManagedLedgerCacheSizeMB(8);
        kConfig.setActiveConsumerFailoverDelayTimeMillis(0);
        kConfig.setDefaultNumberOfNamespaceBundles(2);
        kConfig.setZookeeperServers("localhost:2181");
        kConfig.setConfigurationStoreServers("localhost:3181");
        kConfig.setAuthenticationEnabled(false);
        kConfig.setAuthorizationEnabled(false);
        kConfig.setAllowAutoTopicCreation(true);
        kConfig.setAllowAutoTopicCreationType("partitioned");
        kConfig.setBrokerDeleteInactiveTopicsEnabled(false);
        kConfig.setGroupInitialRebalanceDelayMs(0);
        kConfig.setBrokerShutdownTimeoutMs(0);

        // set protocol related config
        URL testHandlerUrl = this.getClass().getClassLoader().getResource("test-protocol-handler.nar");
        Path handlerPath;
        try {
            handlerPath = Paths.get(testHandlerUrl.toURI());
        } catch (Exception e) {
            log.error("failed to get handler Path, handlerUrl: {}. Exception: ", testHandlerUrl, e);
            return null;
        }

        String protocolHandlerDir = handlerPath.toFile().getParent();

        kConfig.setProtocolHandlerDirectory(
            protocolHandlerDir
        );
        kConfig.setMessagingProtocols(Sets.newHashSet("kafka"));

        return kConfig;
    }

    @Override
    protected void resetConfig() {
        offsetsTopicNumPartitions = 16;
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
        this.pulsarService1 = startBroker(conf1);
        this.pulsar = pulsarService1;
        this.pulsarService = startBroker(conf2);
    }

    @Override
    protected void stopBroker() throws Exception {
        pulsarService1.close();
        pulsarService.close();
    }

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

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


    @AfterMethod(timeOut = 30000, alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        log.info("--- Shutting down ---");
        super.internalCleanup();
    }

    protected int kafkaPublishMessage(KProducer kProducer, int numMessages, String messageStrPrefix) throws Exception {
        int i = 0;
        for (; i < numMessages; i++) {
            String messageStr = messageStrPrefix + i;
            ProducerRecord record = new ProducerRecord<>(
                kProducer.getTopic(),
                i,
                messageStr);

            kProducer.getProducer()
                .send(record)
                .get();
            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer {} Sent message with header: ({}, {})",
                    kProducer.getTopic(), i, messageStr);
            }
        }
        return i;
    }

    protected void kafkaConsumeCommitMessage(KConsumer kConsumer,
                                             int numMessages,
                                             String messageStrPrefix,
                                             List<TopicPartition> topicPartitions) {
        kConsumer.getConsumer().assign(topicPartitions);
        int i = 0;
        while (i < numMessages) {
            if (log.isDebugEnabled()) {
                log.debug("kConsumer {} start poll message: {}",
                    kConsumer.getTopic() + kConsumer.getConsumerGroup(), i);
            }
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());

                if (log.isDebugEnabled()) {
                    log.debug("Kafka consumer get message: {}, key: {} at offset {}",
                        record.key(), record.value(), record.offset());
                }
                i++;
            }
        }

        assertEquals(i, numMessages);

        try {
            kConsumer.getConsumer().commitSync(Duration.ofSeconds(1));
        } catch (Exception e) {
            log.error("Commit offset failed: ", e);
        }

        if (log.isDebugEnabled()) {
            log.debug("kConsumer {} finished poll and commit message: {}",
                kConsumer.getTopic() + kConsumer.getConsumerGroup(), i);
        }
    }

     // Unit test {@link GroupCoordinator}.
    @Test(timeOut = 30000)
    public void testMutiBrokerAndCoordinator() throws Exception {
        int partitionNumber = 10;
        String kafkaTopicName = "kopMutiBrokerAndCoordinator" + partitionNumber;
        String pulsarTopicName = "persistent://public/default/" + kafkaTopicName;

        String offsetNs = ((KafkaServiceConfiguration) conf).getKafkaMetadataTenant() + "/"
            + ((KafkaServiceConfiguration) conf).getKafkaMetadataNamespace();
        String offsetsTopicName = "persistent://" + offsetNs + "/" + GROUP_METADATA_TOPIC_NAME;

        // 0.  Preparing:
        // create partitioned topic.
        pulsarService1.getAdminClient().topics().createPartitionedTopic(kafkaTopicName, partitionNumber);
        // Because pulsarService1 is start firstly. all the offset topics is served in broker1.
        // In setting, each ns has 2 bundles. unload the first part, and this part will be served by broker2.
        pulsarService1.getAdminClient().namespaces().unloadNamespaceBundle(offsetNs, "0x00000000_0x80000000");

        log.info("unloaded offset namespace, will call lookup to force reload");

        // Offsets partitions should be served by 2 brokers now.
        Map<String, List<String>> offsetTopicMap = Maps.newHashMap();
        for (int ii = 0; ii < offsetsTopicNumPartitions; ii++) {
            String offsetsTopic = offsetsTopicName + PARTITIONED_TOPIC_SUFFIX + ii;
            String result = admin.lookups().lookupTopic(offsetsTopic);
            offsetTopicMap.putIfAbsent(result, Lists.newArrayList());
            offsetTopicMap.get(result).add(offsetsTopic);
            log.info("serving broker for offset topic {} is {}", offsetsTopic, result);
        }
        assertEquals(offsetTopicMap.size(), 2);

        final AtomicInteger numberTopic = new AtomicInteger(0);
        offsetTopicMap.values().stream().forEach(list -> numberTopic.addAndGet(list.size()));
        assertEquals(numberTopic.get(), offsetsTopicNumPartitions);

        // 1. produce message with Kafka producer.
        int totalMsgs = 50;
        String messageStrPrefix = "Message_Kop_KafkaProduceKafkaConsume_" + partitionNumber + "_";
        KProducer kProducer = new KProducer(kafkaTopicName, false, getKafkaBrokerPort(), true);
        kafkaPublishMessage(kProducer, totalMsgs, messageStrPrefix);

        // 2. create 4 kafka consumer from different consumer groups.
        //    consume data and commit offsets for 4 consumer group.
        KConsumer kConsumer1 = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), "consumer-group-1");
        KConsumer kConsumer2 = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), "consumer-group-2");
        KConsumer kConsumer3 = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), "consumer-group-3");
        KConsumer kConsumer4 = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), "consumer-group-4");

        List<TopicPartition> topicPartitions = IntStream.range(0, partitionNumber)
            .mapToObj(i -> new TopicPartition(kafkaTopicName, i)).collect(Collectors.toList());

        log.info("Partition size: {}, will consume and commitOffset for 4 consumers",
            topicPartitions.size());

        kafkaConsumeCommitMessage(kConsumer1, totalMsgs, messageStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer2, totalMsgs, messageStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer3, totalMsgs, messageStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer4, totalMsgs, messageStrPrefix, topicPartitions);


        // 3. use a map for serving broker and topics <broker, topics>, verify both broker has messages served.
        Map<String, List<String>> topicMap = Maps.newHashMap();
        for (int ii = 0; ii < partitionNumber; ii++) {
            String topicName = pulsarTopicName + PARTITIONED_TOPIC_SUFFIX + ii;
            String result = admin.lookups().lookupTopic(topicName);
            topicMap.putIfAbsent(result, Lists.newArrayList());
            topicMap.get(result).add(topicName);
            log.info("serving broker for topic {} is {}", topicName, result);
        }
        assertEquals(topicMap.size(), 2);

        final AtomicInteger numberTopic2 = new AtomicInteger(0);
        topicMap.values().stream().forEach(list -> numberTopic2.addAndGet(list.size()));
        assertEquals(numberTopic2.get(), partitionNumber);

        offsetTopicMap = Maps.newHashMap();
        for (int ii = 0; ii < offsetsTopicNumPartitions; ii++) {
            String offsetsTopic = offsetsTopicName + PARTITIONED_TOPIC_SUFFIX + ii;
            String result = admin.lookups().lookupTopic(offsetsTopic);
            offsetTopicMap.putIfAbsent(result, Lists.newArrayList());
            offsetTopicMap.get(result).add(offsetsTopic);
            log.info("serving broker for offset topic {} is {}", offsetsTopic, result);
        }

        // 4. unload ns, coordinator will be on another broker
        //    verify consumer group still keep the old offset, and consumers will poll no data.
        log.info("Unload offset namespace, this will trigger another reload. After reload verify offset.");
        pulsarService1.getAdminClient().namespaces().unload(offsetNs);

        // verify offset be kept and no more records could read.
        ConsumerRecords<Integer, String> records = kConsumer1.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        records = kConsumer2.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        records = kConsumer3.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        records = kConsumer4.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());

        // 5. another round publish and consume after ns unload.
        kafkaPublishMessage(kProducer, totalMsgs, messageStrPrefix);
        kafkaConsumeCommitMessage(kConsumer1, totalMsgs, messageStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer2, totalMsgs, messageStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer3, totalMsgs, messageStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer4, totalMsgs, messageStrPrefix, topicPartitions);

        offsetTopicMap = Maps.newHashMap();
        for (int ii = 0; ii < offsetsTopicNumPartitions; ii++) {
            String offsetsTopic = offsetsTopicName + PARTITIONED_TOPIC_SUFFIX + ii;
            String result = admin.lookups().lookupTopic(offsetsTopic);
            offsetTopicMap.putIfAbsent(result, Lists.newArrayList());
            offsetTopicMap.get(result).add(offsetsTopic);
            log.info("serving broker for offset topic {} is {}", offsetsTopic, result);
        }

        // 6. unload ns, coordinator will be on another broker
        //    verify consumer group still keep the old offset, and consumers will poll no data.
        log.info("Unload offset namespace, this will trigger another reload");
        pulsarService1.getAdminClient().namespaces().unload(offsetNs);

        // verify offset be kept and no more records could read.
        records = kConsumer1.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        records = kConsumer2.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        records = kConsumer3.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        records = kConsumer4.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());

        kProducer.close();
        kConsumer1.close();
        kConsumer2.close();
        kConsumer3.close();
        kConsumer4.close();
    }

    // Unit test for unload / reload user topic bundle, verify it works well.
    // NOTE: Currently the testMultiBrokerUnloadReload is flaky. If it ran after other tests, it would be easy to fail.
    //   So we just change the priority to make it run first to avoid CI failing at this test for this moment.
    @Test(timeOut = 30000, priority = -1)
    public void testMultiBrokerUnloadReload() throws Exception {
        int partitionNumber = 10;
        String kafkaTopicName = "kopMultiBrokerUnloadReload" + partitionNumber;
        String pulsarTopicName = "persistent://public/default/" + kafkaTopicName;
        String kopNamespace = "public/default";

        // 0.  Preparing: create partitioned topic.
        pulsarService1.getAdminClient().topics().createPartitionedTopic(kafkaTopicName, partitionNumber);

        // 1. use a map for serving broker and topics <broker, topics>, verify both broker has messages served.
        Map<String, List<String>> topicMap = Maps.newHashMap();
        for (int ii = 0; ii < partitionNumber; ii++) {
            String topicName = pulsarTopicName + PARTITIONED_TOPIC_SUFFIX + ii;
            String result = admin.lookups().lookupTopic(topicName);
            topicMap.putIfAbsent(result, Lists.newArrayList());
            topicMap.get(result).add(topicName);
            log.info("serving broker for topic {} is {}", topicName, result);
        }
        assertEquals(topicMap.size(), 2);

        // 2. produce consume message with Kafka producer.
        int totalMsgs = 50;
        String messageStrPrefix = "Message_" + kafkaTopicName + "_";
        KProducer kProducer = new KProducer(kafkaTopicName, false, getKafkaBrokerPort(), true);
        kafkaPublishMessage(kProducer, totalMsgs, messageStrPrefix);

        List<TopicPartition> topicPartitions = IntStream.range(0, partitionNumber)
            .mapToObj(i -> new TopicPartition(kafkaTopicName, i)).collect(Collectors.toList());
        KConsumer kConsumer1 = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), "consumer-group-1");
        KConsumer kConsumer2 = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), "consumer-group-2");
        log.info("Partition size: {}, will consume and commitOffset for 2 consumers",
            topicPartitions.size());
        kafkaConsumeCommitMessage(kConsumer1, totalMsgs, messageStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer2, totalMsgs, messageStrPrefix, topicPartitions);

        // 3. unload
        log.info("Unload namespace, lookup will trigger another reload.");
        pulsarService1.getAdminClient().namespaces().unload(kopNamespace);

        // 4. publish consume again
        log.info("Re Publish / Consume again.");
        kafkaPublishMessage(kProducer, totalMsgs, messageStrPrefix);
        kafkaConsumeCommitMessage(kConsumer1, totalMsgs, messageStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer2, totalMsgs, messageStrPrefix, topicPartitions);

        kProducer.close();
        kConsumer1.close();
        kConsumer2.close();
    }


    /**
     * Unit test for test in distributed cluster env produce and consume non-partitioned topic,
     * verify it works well.
     */
    @Test(timeOut = 30000)
    public void testMultiBrokerProduceAndConsumeNonPartitionedTopic() throws Exception {
        String kafkaTopicName = "kopMultiBrokerNonPartitionedTopic";
        String pulsarTopicName = "persistent://public/default/" + kafkaTopicName;
        String kopNamespace = "public/default";
        int totalMsgs = 50;
        List<TopicPartition> topicPartitions =
                Collections.singletonList(new TopicPartition(pulsarTopicName, 0));
        String messageStrPrefix = "Message_" + kafkaTopicName + "_";
        KProducer kProducer1 = null;
        KProducer kProducer2 = null;
        KConsumer kConsumer1 = null;
        KConsumer kConsumer2 = null;
        // 0.  Preparing: create non-partitioned topic.
        pulsarService1.getAdminClient().topics().createNonPartitionedTopic(kafkaTopicName);
        PartitionedTopicMetadata partitionedTopicMetadata = admin.topics().getPartitionedTopicMetadata(pulsarTopicName);
        assertEquals(partitionedTopicMetadata.partitions, 0);
        try {
            // 1. check lookup result, use other kafka broker port as bootstrap port.
            String result = admin.lookups().lookupTopic(pulsarTopicName);
            log.info("Server address:{}", result);
            int kafkaBrokerPort;
            if (result.endsWith(String.valueOf(primaryBrokerPort))) {
                kafkaBrokerPort = secondaryKafkaBrokerPort;
            } else {
                kafkaBrokerPort = primaryKafkaBrokerPort;
            }
            log.info("kafkaBrokerPort:{}", kafkaBrokerPort);

            // 2. produce consume message with Kafka producer.
            kProducer1 = new KProducer(kafkaTopicName, false, kafkaBrokerPort, true);
            kafkaPublishMessage(kProducer1, totalMsgs / 2, messageStrPrefix);

            kProducer2 = new KProducer(kafkaTopicName, false, kafkaBrokerPort, true);
            kafkaPublishMessage(kProducer2, totalMsgs / 2, messageStrPrefix);

            kConsumer1 = new KConsumer(kafkaTopicName, kafkaBrokerPort, "consumer-group-1");
            kConsumer2 = new KConsumer(kafkaTopicName, kafkaBrokerPort, "consumer-group-2");

            log.info("Partition size: {}, will consume and commitOffset for 2 consumers",
                    topicPartitions.size());
            kafkaConsumeCommitMessage(kConsumer1, totalMsgs, messageStrPrefix, topicPartitions);
            kafkaConsumeCommitMessage(kConsumer2, totalMsgs, messageStrPrefix, topicPartitions);

            // 3. unload
            log.info("Unload namespace, lookup will trigger another reload.");
            pulsarService1.getAdminClient().namespaces().unload(kopNamespace);

            // 4. publish consume again
            log.info("Re Publish / Consume again.");
            kafkaPublishMessage(kProducer1, totalMsgs, messageStrPrefix);
            kafkaConsumeCommitMessage(kConsumer1, totalMsgs, messageStrPrefix, topicPartitions);
            kafkaConsumeCommitMessage(kConsumer2, totalMsgs, messageStrPrefix, topicPartitions);

        } finally {
            if (kProducer1 != null) {
                kProducer1.close();
            }
            if (kProducer2 != null) {
                kProducer2.close();
            }
            if (kConsumer1 != null) {
                kConsumer1.close();
            }
            if (kConsumer2 != null) {
                kConsumer2.close();
            }
            pulsarService1.getAdminClient().topics().delete(pulsarTopicName);
        }

    }

    /**
     * Unit test for test in distributed cluster env produce and consume one partition topic,
     * verify it works well (This test case is used to compare with non-partitioned topic behavior).
     */
    @Test(timeOut = 30000)
    public void testMultiBrokerProduceAndConsumeOnePartitionedTopic() throws Exception {
        String kafkaTopicName = "kopMultiBrokerOnePartitionedTopic";
        String pulsarTopicName = "persistent://public/default/" + kafkaTopicName;
        String kopNamespace = "public/default";
        int totalMsgs = 50;
        List<TopicPartition> topicPartitions =
                Collections.singletonList(new TopicPartition(kafkaTopicName, 0));
        String messageStrPrefix = "Message_" + kafkaTopicName + "_";
        KProducer kProducer1 = null;
        KProducer kProducer2 = null;
        KConsumer kConsumer1 = null;
        KConsumer kConsumer2 = null;
        // 0.  Preparing: create non-partitioned topic.
        pulsarService1.getAdminClient().topics().createPartitionedTopic(pulsarTopicName, 1);
        try {
            // 1. check lookup result.
            String result = admin.lookups().lookupTopic(pulsarTopicName);
            log.info("Server address:{}", result);
            int kafkaPort;
            if (result.endsWith(String.valueOf(primaryBrokerPort))) {
                kafkaPort = secondaryKafkaBrokerPort;
            } else {
                kafkaPort = primaryKafkaBrokerPort;
            }

            // 2. produce consume message with Kafka producer.
            kProducer1 = new KProducer(kafkaTopicName, false, kafkaPort, true);
            kafkaPublishMessage(kProducer1, totalMsgs / 2, messageStrPrefix);

            kProducer2 = new KProducer(kafkaTopicName, false, kafkaPort, true);
            kafkaPublishMessage(kProducer2, totalMsgs / 2, messageStrPrefix);

            kConsumer1 = new KConsumer(kafkaTopicName, kafkaPort, "consumer-group-1");
            kConsumer2 = new KConsumer(kafkaTopicName, kafkaPort, "consumer-group-2");

            log.info("Partition size: {}, will consume and commitOffset for 2 consumers",
                    topicPartitions.size());
            kafkaConsumeCommitMessage(kConsumer1, totalMsgs, messageStrPrefix, topicPartitions);
            kafkaConsumeCommitMessage(kConsumer2, totalMsgs, messageStrPrefix, topicPartitions);

            // 3. unload
            log.info("Unload namespace, lookup will trigger another reload.");
            pulsarService1.getAdminClient().namespaces().unload(kopNamespace);

            // 4. publish consume again
            log.info("Re Publish / Consume again.");
            kafkaPublishMessage(kProducer1, totalMsgs, messageStrPrefix);
            kafkaConsumeCommitMessage(kConsumer1, totalMsgs, messageStrPrefix, topicPartitions);
            kafkaConsumeCommitMessage(kConsumer2, totalMsgs, messageStrPrefix, topicPartitions);

        } finally {
            if (kProducer1 != null) {
                kProducer1.close();
            }
            if (kProducer2 != null) {
                kProducer2.close();
            }
            if (kConsumer1 != null) {
                kConsumer1.close();
            }
            if (kConsumer2 != null) {
                kConsumer2.close();
            }
            pulsarService1.getAdminClient().topics().deletePartitionedTopic(pulsarTopicName);
        }

    }

    @Test(timeOut = 30000)
    public void testOneBrokerShutdown() throws Exception {
        int partitionNumber = 10;
        String kafkaTopicName = "kopOneBrokerShutdown" + partitionNumber;
        String pulsarTopicName = "persistent://public/default/" + kafkaTopicName;

        // 0.  Preparing: create partitioned topic.
        pulsarService1.getAdminClient().topics().createPartitionedTopic(kafkaTopicName, partitionNumber);

        // 1. use a map for serving broker and topics <broker, topics>, verify both broker has messages served.
        Map<String, List<String>> topicMap = Maps.newHashMap();
        for (int ii = 0; ii < partitionNumber; ii++) {
            String topicName = pulsarTopicName + PARTITIONED_TOPIC_SUFFIX + ii;
            String result = admin.lookups().lookupTopic(topicName);
            topicMap.putIfAbsent(result, Lists.newArrayList());
            topicMap.get(result).add(topicName);
            log.info("serving broker for topic {} is {}", topicName, result);
        }
        assertEquals(topicMap.size(), 2);

        // 2. produce consume message with Kafka producer.
        int totalMsgs = 50;
        String messageStrPrefix = "Message_" + kafkaTopicName + "_";
        KProducer kProducer = new KProducer(kafkaTopicName, false, getKafkaBrokerPort(), true);
        kafkaPublishMessage(kProducer, totalMsgs, messageStrPrefix);

        List<TopicPartition> topicPartitions = IntStream.range(0, partitionNumber)
            .mapToObj(i -> new TopicPartition(kafkaTopicName, i)).collect(Collectors.toList());
        KConsumer kConsumer1 = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), "consumer-group-1");
        KConsumer kConsumer2 = new KConsumer(kafkaTopicName, getKafkaBrokerPort(), "consumer-group-2");
        log.info("Partition size: {}, will consume and commitOffset for 2 consumers",
            topicPartitions.size());
        kafkaConsumeCommitMessage(kConsumer1, totalMsgs, messageStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer2, totalMsgs, messageStrPrefix, topicPartitions);

        // 3. close first broker
        log.info("will close first kafkaService");
        pulsarService1.close();

        // 4. publish consume again
        log.info("Re Publish / Consume again.");
        kafkaPublishMessage(kProducer, totalMsgs, messageStrPrefix);
        kafkaConsumeCommitMessage(kConsumer1, totalMsgs, messageStrPrefix, topicPartitions);
        kafkaConsumeCommitMessage(kConsumer2, totalMsgs, messageStrPrefix, topicPartitions);

        kProducer.close();
        kConsumer1.close();
        kConsumer2.close();
    }
}
