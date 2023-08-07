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
package io.streamnative.pulsar.handlers.kop.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import io.jsonwebtoken.lang.Maps;
import io.streamnative.pulsar.handlers.kop.KafkaLogConfig;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;


/**
 * Test Kafka admin operations.
 *
 *
 * Don't support admin api list.
 *
 * | Method                       | Is Support                                     |
 * |------------------------------|------------------------------------------------|
 * | createAcls                   | Don't support, use pulsar admin to manage acl. |
 * | deleteAcls                   | Don't support, use pulsar admin to manage acl. |
 * | describeAcls                 | Don't support, use pulsar admin to manage acl. |
 * | listAcls                     | Don't support, use pulsar admin to manage acl. |
 * | electLeaders                 | Don't support, it depends on the pulsar.       |
 * | describeUserScramCredentials | Don't support                                  |
 * | alterUserScramCredentials    | Don't support                                  |
 * | alterPartitionReassignments  | Don't support                                  |
 * | listPartitionReassignments   | Don't support                                  |
 * | describeDelegationToken      | Don't support                                  |
 * | createDelegationToken        | Don't support                                  |
 * | alterReplicaLogDirs          | Don't support                                  |
 * | describeReplicaLogDirs       | Don't support                                  |
 * | describeLogDirs              | Don't support                                  |
 *
 * TODO: Can support in the future.
 *
 * | Method                       | How to support.                                |
 * |------------------------------|------------------------------------------------|
 * | alterClientQuotas            | Can use pulsar admin handle it.                |
 * | describeClientQuotas         | Can get it from pulsar admin.                  |
 * | deleteConsumerGroupOffsets   | Can handle by group coordinator.               |
 * | alterConfigs                 | Maybe can store in the metadata store.         |
 * | incrementalAlterConfigs      | Maybe can store in the metadata store.         |
 */
@Slf4j
public class KafkaAdminTest extends KopProtocolHandlerTestBase {

    private AdminClient kafkaAdmin;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setDefaultNumPartitions(2);
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.namespaces().getNamespaces("public").contains("public/__kafka")) {
            admin.namespaces().createNamespace("public/__kafka");
            admin.namespaces().setNamespaceReplicationClusters("public/__kafka", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/__kafka",
                    new RetentionPolicies(-1, -1));
        }

        admin.tenants().createTenant("my-tenant",
                TenantInfo.builder()
                        .adminRoles(Collections.emptySet())
                        .allowedClusters(Collections.singleton(configClusterName))
                        .build());
        admin.namespaces().createNamespace("my-tenant/my-ns");

        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        kafkaAdmin = AdminClient.create(props);
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        kafkaAdmin.close();
    }

    @Test(timeOut = 30000)
    public void testCreateTopics() throws ExecutionException, InterruptedException {
        // Create a topic with 1 partition and a replication factor of 1
        kafkaAdmin.createTopics(
                Collections.singleton(new NewTopic("my-topic", 1, (short) 1))).all().get();

        Set<String> topics = kafkaAdmin.listTopics().names().get();

        assertTrue(topics.contains("my-topic"));

        kafkaAdmin.deleteTopics(Collections.singleton("my-topic")).all().get();

        topics = kafkaAdmin.listTopics().names().get();
        assertTrue(topics.isEmpty());
    }

    @Test(timeOut = 30000)
    public void testDescribeTopics() throws ExecutionException, InterruptedException {
        // Create a topic with 10 partition and a replication factor of 1

        kafkaAdmin.createTopics(
                Collections.singleton(new NewTopic("my-topic", 10, (short) 1))).all().get();

        TopicDescription topicDescription = kafkaAdmin.describeTopics(Collections.singleton("my-topic"))
                .all().get().get("my-topic");

        assertEquals(topicDescription.partitions().size(), 10);
        assertFalse(topicDescription.isInternal());
        assertEquals(topicDescription.name(), "my-topic");

        kafkaAdmin.deleteTopics(Collections.singleton("my-topic")).all().get();

        var topics = kafkaAdmin.listTopics().names().get();
        assertTrue(topics.isEmpty());
    }

    @Test(timeOut = 30000)
    public void testListTopics() throws ExecutionException, InterruptedException {
        // Create a topic with 1 partition and a replication factor of 1
        kafkaAdmin.createTopics(
                Collections.singleton(new NewTopic("my-topic", 1, (short) 1))).all().get();

        ListTopicsResult listTopicsResult = kafkaAdmin.listTopics();
        Set<String> topics = listTopicsResult.names().get();
        assertTrue(topics.contains("my-topic"));

        Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        assertTrue(topicListings.stream().anyMatch(topicListing -> topicListing.name().equals("my-topic")));

        kafkaAdmin.deleteTopics(Collections.singleton("my-topic")).all().get();

        topics = kafkaAdmin.listTopics().names().get();
        assertTrue(topics.isEmpty());
    }

    @Test(timeOut = 30000)
    public void testCreatePartitions() throws ExecutionException, InterruptedException {
        // Create a topic with 1 partition and a replication factor of 1
        kafkaAdmin.createTopics(
                Collections.singleton(new NewTopic("my-topic", 1, (short) 1))).all().get();

        Map<String, NewPartitions> newPartitions = new HashMap<>();
        newPartitions.put("my-topic", NewPartitions.increaseTo(2));
        CreatePartitionsResult createPartitionsResult = kafkaAdmin.createPartitions(newPartitions);
        createPartitionsResult.all().get();

        TopicDescription topicDescription = kafkaAdmin.describeTopics(Collections.singleton("my-topic"))
                .all().get().get("my-topic");

        assertEquals(topicDescription.partitions().size(), 2);
        assertFalse(topicDescription.isInternal());
        assertEquals(topicDescription.name(), "my-topic");

        kafkaAdmin.deleteTopics(Collections.singleton("my-topic")).all().get();

        var topics = kafkaAdmin.listTopics().names().get();
        assertTrue(topics.isEmpty());
    }

    @Test(timeOut = 20000)
    public void testDescribeCluster() throws Exception {
        DescribeClusterResult describeClusterResult = kafkaAdmin.describeCluster();
        String clusterId = describeClusterResult.clusterId().get();
        Collection<Node> nodes = describeClusterResult.nodes().get();
        Node node = describeClusterResult.controller().get();
        Set<AclOperation> aclOperations = describeClusterResult.authorizedOperations().get();

        assertEquals(clusterId, conf.getClusterName());
        assertEquals(nodes.size(), 1);
        assertEquals(node.host(), "localhost");
        assertEquals(node.port(), getKafkaBrokerPort());
        assertEquals(node.id(),
                Murmur3_32Hash.getInstance().makeHash((node.host() + node.port()).getBytes(StandardCharsets.UTF_8)));
        assertEquals(node.host(), "localhost");
        assertEquals(node.port(), getKafkaBrokerPort());
        assertNull(node.rack());
        assertNull(aclOperations);
    }

    @Test
    public void testDescribeBrokerConfigs() throws Exception {
        Map<ConfigResource, Config> brokerConfigs = kafkaAdmin.describeConfigs(Collections.singletonList(
                new ConfigResource(ConfigResource.Type.BROKER, ""))).all().get();
        assertEquals(1, brokerConfigs.size());
        Config brokerConfig = brokerConfigs.values().iterator().next();
        assertEquals(brokerConfig.get("num.partitions").value(), conf.getDefaultNumPartitions() + "");
        assertEquals(brokerConfig.get("default.replication.factor").value(), "1");
        assertEquals(brokerConfig.get("delete.topic.enable").value(), "true");
        assertEquals(brokerConfig.get("message.max.bytes").value(), conf.getMaxMessageSize() + "");
    }


    @Test(timeOut = 20000)
    public void testDescribeConsumerGroups() throws Exception {
        final String topic = "public/default/test-describe-group-offset";
        final int numMessages = 10;
        final String messagePrefix = "msg-";
        final String group = "test-group";

        admin.topics().createPartitionedTopic(topic, 1);

        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());

        for (int i = 0; i < numMessages; i++) {
            producer.send(new ProducerRecord<>(topic, i + "", messagePrefix + i));
        }
        producer.close();

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(newKafkaConsumerProperties(group));
        consumer.subscribe(Collections.singleton(topic));

        int fetchMessages = 0;
        while (fetchMessages < numMessages) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));
            fetchMessages += records.count();
        }
        assertEquals(fetchMessages, numMessages);

        consumer.commitSync();

        ConsumerGroupDescription groupDescription =
                kafkaAdmin.describeConsumerGroups(Collections.singletonList(group))
                        .all().get().get(group);
        assertEquals(1, groupDescription.members().size());

        // member assignment topic name must be short topic name
        groupDescription.members().forEach(memberDescription -> memberDescription.assignment().topicPartitions()
                .forEach(topicPartition -> assertEquals(topic, topicPartition.topic())));

        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsetAndMetadataMap =
                kafkaAdmin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
        assertEquals(1, offsetAndMetadataMap.size());

        //  topic name from offset fetch response must be short topic name
        offsetAndMetadataMap.keySet().forEach(topicPartition -> assertEquals(topic, topicPartition.topic()));

        consumer.close();

        admin.topics().deletePartitionedTopic(topic, true);
    }

    @Test
    public void testDeleteConsumerGroups() throws Exception {
        final String topic = "test-delete-groups";
        final int numMessages = 10;
        final String messagePrefix = "msg-";
        final String group = "test-group";

        admin.topics().createPartitionedTopic(topic, 1);

        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());

        for (int i = 0; i < numMessages; i++) {
            producer.send(new ProducerRecord<>(topic, i + "", messagePrefix + i));
        }
        producer.close();

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(newKafkaConsumerProperties(group));
        consumer.subscribe(Collections.singleton(topic));

        int fetchMessages = 0;
        while (fetchMessages < numMessages) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));
            fetchMessages += records.count();
        }
        assertEquals(fetchMessages, numMessages);

        consumer.commitSync();

        ConsumerGroupDescription groupDescription =
                kafkaAdmin.describeConsumerGroups(Collections.singletonList(group))
                        .all().get().get(group);
        assertEquals(1, groupDescription.members().size());

        consumer.close();

        kafkaAdmin.deleteConsumerGroups(Collections.singleton(group)).all().get();
        groupDescription =
                kafkaAdmin.describeConsumerGroups(Collections.singletonList(group))
                        .all().get().get(group);
        assertTrue(groupDescription.members().isEmpty());
        assertEquals(ConsumerGroupState.DEAD, groupDescription.state());
        admin.topics().deletePartitionedTopic(topic, true);
    }

    @Test
    public void testListConsumerGroups() throws ExecutionException, InterruptedException, PulsarAdminException {
        final String topic = "test-list-group";
        final int numMessages = 10;
        final String messagePrefix = "msg-";
        final String group = "test-group";

        admin.topics().createPartitionedTopic(topic, 1);

        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());

        for (int i = 0; i < numMessages; i++) {
            producer.send(new ProducerRecord<>(topic, i + "", messagePrefix + i));
        }
        producer.close();

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(newKafkaConsumerProperties(group));
        consumer.subscribe(Collections.singleton(topic));

        int fetchMessages = 0;
        while (fetchMessages < numMessages) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));
            fetchMessages += records.count();
        }
        assertEquals(fetchMessages, numMessages);

        consumer.commitSync();

        ConsumerGroupDescription groupDescription =
                kafkaAdmin.describeConsumerGroups(Collections.singletonList(group))
                        .all().get().get(group);
        assertEquals(1, groupDescription.members().size());

        ListConsumerGroupsResult listConsumerGroupsResult = kafkaAdmin.listConsumerGroups();
        Collection<ConsumerGroupListing> consumerGroupListings = listConsumerGroupsResult.all().get();
        assertEquals(1, consumerGroupListings.size());
        consumerGroupListings.forEach(consumerGroupListing -> {
            log.info(consumerGroupListing.toString());
            assertEquals(group, consumerGroupListing.groupId());
            assertFalse(consumerGroupListing.isSimpleConsumerGroup());
        });

        consumer.close();

        admin.topics().deletePartitionedTopic(topic, true);
    }

    @Test(timeOut = 10000)
    public void testDeleteRecords() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());

        @Cleanup
        AdminClient kafkaAdmin = AdminClient.create(props);
        Map<String, Integer> topicToNumPartitions = new HashMap<>() {{
            put("testDeleteRecords-0", 1);
            put("testDeleteRecords-1", 3);
            put("my-tenant/my-ns/testDeleteRecords-2", 1);
            put("persistent://my-tenant/my-ns/testDeleteRecords-3", 5);
        }};
        // create
        createTopicsByKafkaAdmin(kafkaAdmin, topicToNumPartitions);
        verifyTopicsCreatedByPulsarAdmin(topicToNumPartitions);


        AtomicInteger count = new AtomicInteger();
        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());
        topicToNumPartitions.forEach((topic, numPartitions) -> {
            for (int i = 0; i < numPartitions; i++) {
                producer.send(new ProducerRecord<>(topic, i, count + "", count + ""));
                count.incrementAndGet();
            }
        });

        producer.close();

        // delete
        deleteRecordsByKafkaAdmin(kafkaAdmin, topicToNumPartitions);
    }

    private void createTopicsByKafkaAdmin(AdminClient admin, Map<String, Integer> topicToNumPartitions)
            throws ExecutionException, InterruptedException {
        final short replicationFactor = 1; // replication factor will be ignored
        admin.createTopics(topicToNumPartitions.entrySet().stream().map(entry -> {
            final String topic = entry.getKey();
            final int numPartitions = entry.getValue();
            return new NewTopic(topic, numPartitions, replicationFactor);
        }).collect(Collectors.toList())).all().get();
    }

    private void verifyTopicsCreatedByPulsarAdmin(Map<String, Integer> topicToNumPartitions)
            throws PulsarAdminException {
        for (Map.Entry<String, Integer> entry : topicToNumPartitions.entrySet()) {
            final String topic = entry.getKey();
            final int numPartitions = entry.getValue();
            assertEquals(this.admin.topics().getPartitionedTopicMetadata(topic).partitions, numPartitions);
        }
    }

    private void deleteRecordsByKafkaAdmin(AdminClient admin, Map<String, Integer> topicToNumPartitions)
            throws ExecutionException, InterruptedException {
        Map<TopicPartition, RecordsToDelete> toDelete = new HashMap<>();
        topicToNumPartitions.forEach((topic, numPartitions) -> {
            try (KConsumer consumer = new KConsumer(topic, getKafkaBrokerPort())) {
                Collection<TopicPartition> topicPartitions = new ArrayList<>();
                for (int i = 0; i < numPartitions; i++) {
                    topicPartitions.add(new TopicPartition(topic, i));
                }
                Map<TopicPartition, Long> map = consumer
                        .getConsumer().endOffsets(topicPartitions);
                map.forEach((TopicPartition topicPartition, Long offset) -> {
                    log.info("For {} we are truncating at {}", topicPartition, offset);
                    toDelete.put(topicPartition, RecordsToDelete.beforeOffset(offset));
                });
            }
        });
        admin.deleteRecords(toDelete).all().get();
        admin.deleteTopics(topicToNumPartitions.keySet()).all().get();
    }



    // Unsupported operations

    @Test
    public void testIncrementalAlterConfigs() {
        // TODO: Support incremental alter configs.
        try {
            kafkaAdmin.incrementalAlterConfigs(Collections.singletonMap(
                    new ConfigResource(ConfigResource.Type.TOPIC, "test-topic"),
                    Collections.emptyList())).all().get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof UnsupportedVersionException);
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsets() throws Exception {
        final String topic = "test-delete-group-offsets";
        final int numMessages = 10;
        final String messagePrefix = "msg-";
        final String group = "test-delete-group";

        admin.topics().createPartitionedTopic(topic, 1);

        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());

        for (int i = 0; i < numMessages; i++) {
            producer.send(new ProducerRecord<>(topic, i + "", messagePrefix + i));
        }
        producer.close();

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(newKafkaConsumerProperties(group));
        consumer.subscribe(Collections.singleton(topic));

        int fetchMessages = 0;
        while (fetchMessages < numMessages) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1000));
            fetchMessages += records.count();
        }
        assertEquals(fetchMessages, numMessages);

        consumer.commitSync();

        ConsumerGroupDescription groupDescription =
                kafkaAdmin.describeConsumerGroups(Collections.singletonList(group))
                        .all().get().get(group);
        assertEquals(1, groupDescription.members().size());

        var topicPartitionListOffsetsResultInfoMap =
                kafkaAdmin.listOffsets(Collections.singletonMap(new TopicPartition(topic, 0), OffsetSpec.latest()))
                        .all().get();

        log.info(topicPartitionListOffsetsResultInfoMap.toString());

        // TODO: Support delete consumer group offsets.
        DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsetsResult =
                kafkaAdmin.deleteConsumerGroupOffsets(group,
                        Collections.singleton(new TopicPartition(topic, 0)));
        try {
            deleteConsumerGroupOffsetsResult.all().get();
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof UnsupportedVersionException);
        }

        consumer.close();

        kafkaAdmin.deleteConsumerGroups(Collections.singleton(group)).all().get();
        groupDescription =
                kafkaAdmin.describeConsumerGroups(Collections.singletonList(group))
                        .all().get().get(group);
        assertTrue(groupDescription.members().isEmpty());
        assertEquals(ConsumerGroupState.DEAD, groupDescription.state());
        admin.topics().deletePartitionedTopic(topic, true);
    }

    @Ignore("\"org.apache.kafka.common.errors.UnsupportedVersionException: "
            + "The version of API is not supported.\" in testAlterClientQuotas")
    @Test(timeOut = 30000)
    public void testAlterClientQuotas() throws ExecutionException, InterruptedException {

        // TODO: Support alter client quotas by reuse pulsar topic policy.
        kafkaAdmin.alterClientQuotas(Collections.singleton(
                new ClientQuotaAlteration(
                        new ClientQuotaEntity(Maps.of(ClientQuotaEntity.CLIENT_ID, "test_client").build()),
                        List.of(new ClientQuotaAlteration.Op("producer_byte_rate", 1024.0))))).all().get();

        // TODO: Support describe client quotas by reuse pulsar topic policy.
        DescribeClientQuotasResult result = kafkaAdmin.describeClientQuotas(ClientQuotaFilter.all());

        try {
            result.entities().get();
        } catch (Exception ex) {
            assertTrue(ex.getCause() instanceof UnsupportedVersionException);
        }
    }

    @Test(timeOut = 10000)
    public void testDescribeAndAlterConfigs() throws Exception {
        final String topic = "testDescribeAndAlterConfigs";
        admin.topics().createPartitionedTopic(topic, 1);

        final Map<String, String> entries = KafkaLogConfig.getEntries();

        kafkaAdmin.describeConfigs(Collections.singletonList(new ConfigResource(ConfigResource.Type.TOPIC, topic)))
                .all().get().forEach((resource, config) -> {
                    assertEquals(resource.name(), topic);
                    config.entries().forEach(entry -> assertEquals(entry.value(), entries.get(entry.name())));
                });

        final String invalidTopic = "invalid-topic";
        try {
            kafkaAdmin.describeConfigs(Collections.singletonList(
                    new ConfigResource(ConfigResource.Type.TOPIC, invalidTopic))).all().get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof UnknownTopicOrPartitionException);
            assertTrue(e.getMessage().contains("Topic " + invalidTopic + " doesn't exist"));
        }

        admin.topics().createNonPartitionedTopic(invalidTopic);
        try {
            kafkaAdmin.describeConfigs(Collections.singletonList(
                    new ConfigResource(ConfigResource.Type.TOPIC, invalidTopic))).all().get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof InvalidTopicException);
            assertTrue(e.getMessage().contains("Topic " + invalidTopic + " is non-partitioned"));
        }

        // TODO: Support alter configs. Current is dummy implementation.
        // just call the API, currently we are ignoring any value
        kafkaAdmin.alterConfigs(Collections.singletonMap(
                new ConfigResource(ConfigResource.Type.TOPIC, invalidTopic),
                new Config(Collections.emptyList()))).all().get();

        admin.topics().deletePartitionedTopic(topic, true);
        admin.topics().delete(invalidTopic, true);
    }


}
