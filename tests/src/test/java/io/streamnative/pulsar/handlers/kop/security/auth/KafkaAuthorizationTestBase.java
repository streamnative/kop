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
package io.streamnative.pulsar.handlers.kop.security.auth;

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;

import com.google.common.collect.Sets;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.jsonwebtoken.SignatureAlgorithm;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.crypto.SecretKey;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for KoP enable authorization.
 */
@Slf4j
public abstract class KafkaAuthorizationTestBase extends KopProtocolHandlerTestBase {

    protected static final String TENANT = "KafkaAuthorizationTest";
    protected static final String NAMESPACE = "ns1";
    private static final String SHORT_TOPIC = "topic1";
    private static final String TOPIC = "persistent://" + TENANT + "/" + NAMESPACE + "/" + SHORT_TOPIC;
    private static final SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

    protected static final String SIMPLE_USER = "muggle_user";
    protected static final String ANOTHER_USER = "death_eater_user";
    protected static final String ADMIN_USER = "admin_user";

    private String adminToken;
    private String userToken;
    private String anotherToken;

    public KafkaAuthorizationTestBase(final String entryFormat) {
        super(entryFormat);
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        // this is for SchemaRegistry testing with Authentication and Authorization
        enableSchemaRegistry = true;

        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));

        userToken = AuthTokenUtils.createToken(secretKey, SIMPLE_USER, Optional.empty());
        adminToken = AuthTokenUtils.createToken(secretKey, ADMIN_USER, Optional.empty());
        anotherToken = AuthTokenUtils.createToken(secretKey, ANOTHER_USER, Optional.empty());
        boolean originalKafkaEnableMultiTenantMetadata = conf.isKafkaEnableMultiTenantMetadata();
        super.resetConfig();
        conf.setKafkaEnableMultiTenantMetadata(originalKafkaEnableMultiTenantMetadata);
        conf.setSaslAllowedMechanisms(Sets.newHashSet("PLAIN"));
        conf.setKafkaMetadataTenant("internal");
        conf.setKafkaMetadataNamespace("__kafka");
        conf.setKafkaTenant(TENANT);
        conf.setKafkaNamespace(NAMESPACE);
        conf.setKopSchemaRegistryNamespace(NAMESPACE);

        conf.setClusterName(super.configClusterName);
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationAllowWildcardsMatching(true);
        conf.setSuperUserRoles(Sets.newHashSet(ADMIN_USER));
        conf.setAuthenticationProviders(
                Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters("token:" + adminToken);
        conf.setProperties(properties);

        super.internalSetup();
        admin.namespaces()
                .setNamespaceReplicationClusters(TENANT + "/" + NAMESPACE, Sets.newHashSet(super.configClusterName));
        admin.topics().createPartitionedTopic(TOPIC, 1);
        admin.namespaces().grantPermissionOnNamespace(TENANT + "/" + NAMESPACE, SIMPLE_USER,
                        Sets.newHashSet(AuthAction.consume, AuthAction.produce));
    }

    @Override
    protected void createAdmin() throws Exception {
        super.admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(this.conf.getBrokerClientAuthenticationPlugin(),
                        this.conf.getBrokerClientAuthenticationParameters()).build());
    }

    @AfterClass(timeOut = 30000)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000)
    public void testAuthorizationFailed() throws PulsarAdminException {
        String newTenant = "newTenantAuthorizationFailed";
        String testTopic = "persistent://" + newTenant + "/" + NAMESPACE + "/topic1";
        try {
            admin.tenants().createTenant(newTenant,
                    TenantInfo.builder()
                            .adminRoles(Collections.singleton(ADMIN_USER))
                            .allowedClusters(Collections.singleton(configClusterName))
                            .build());
            TenantInfo tenantInfo = admin.tenants().getTenantInfo(TENANT);
            log.info("tenantInfo for {} {} in test", TENANT, tenantInfo);
            assertNotNull(tenantInfo);
            admin.namespaces().createNamespace(newTenant + "/" + NAMESPACE);
            admin.topics().createPartitionedTopic(testTopic, 1);
            @Cleanup
            KProducer kProducer = new KProducer(testTopic, false, "localhost", getKafkaBrokerPort(),
                    newTenant + "/" + NAMESPACE, "token:" + userToken);
            kProducer.getProducer().send(new ProducerRecord<>(testTopic, 0, "")).get();
            fail("should have failed");
        } catch (Exception e) {
            log.info("the error", e);
            assertTrue(e.getMessage().contains("TopicAuthorizationException"));
        } finally {
            // Cleanup
            admin.topics().deletePartitionedTopic(testTopic);
        }
    }

    @Test(timeOut = 20000)
    public void testAuthorizationSuccess() throws PulsarAdminException {
        String topic = "testAuthorizationSuccessTopic";
        String fullNewTopicName = "persistent://" + TENANT + "/" + NAMESPACE + "/" + topic;
        KProducer kProducer = new KProducer(topic, false, "localhost", getKafkaBrokerPort(),
                TENANT + "/" + NAMESPACE, "token:" + userToken);
        int totalMsgs = 10;
        String messageStrPrefix = topic + "_message_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer().send(new ProducerRecord<>(topic, i, messageStr));
        }
        KConsumer kConsumer = new KConsumer(topic, "localhost", getKafkaBrokerPort(), false,
                TENANT + "/" + NAMESPACE, "token:" + userToken, "DemoKafkaOnPulsarConsumer");
        kConsumer.getConsumer().subscribe(Collections.singleton(topic));

        int i = 0;
        while (i < totalMsgs) {
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                i++;
            }
        }
        assertEquals(i, totalMsgs);

        // no more records
        ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());

        // ensure that we can list the topic
        Map<String, List<PartitionInfo>> result = kConsumer.getConsumer().listTopics(Duration.ofSeconds(1));
        assertEquals(result.size(), 2);
        assertTrue(result.containsKey(topic),
                "list of topics " + result.keySet() + "  does not contains " + topic);

        // Cleanup
        kProducer.close();
        kConsumer.close();
        admin.topics().deletePartitionedTopic(fullNewTopicName);
    }

    @Test(timeOut = 20000)
    public void testAuthorizationSuccessByAdmin() throws PulsarAdminException {
        String topic = "testAuthorizationSuccessByAdminTopic";
        String fullNewTopicName = "persistent://" + TENANT + "/" + NAMESPACE + "/" + topic;
        KProducer kProducer = new KProducer(topic, false, "localhost", getKafkaBrokerPort(),
                TENANT + "/" + NAMESPACE, "token:" + adminToken);
        int totalMsgs = 10;
        String messageStrPrefix = topic + "_message_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer().send(new ProducerRecord<>(topic, i, messageStr));
        }
        KConsumer kConsumer = new KConsumer(topic, "localhost", getKafkaBrokerPort(), false,
                TENANT + "/" + NAMESPACE, "token:" + adminToken, "DemoKafkaOnPulsarConsumer");
        kConsumer.getConsumer().subscribe(Collections.singleton(topic));

        int i = 0;
        while (i < totalMsgs) {
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                i++;
            }
        }
        assertEquals(i, totalMsgs);

        // no more records
        ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());

        // ensure that we can list the topic
        Map<String, List<PartitionInfo>> result = kConsumer.getConsumer().listTopics(Duration.ofSeconds(1));
        assertEquals(result.size(), 2);
        assertTrue(result.containsKey(topic),
                "list of topics " + result.keySet() + "  does not contains " + topic);

        // Cleanup
        kProducer.close();
        kConsumer.close();
        admin.topics().deletePartitionedTopic(fullNewTopicName);
    }

    @Test(timeOut = 20000)
    public void testProduceWithTopicLevelPermissions()
            throws PulsarAdminException, ExecutionException, InterruptedException {
        String topic = "testTopicLevelPermissions";
        String fullNewTopicName = "persistent://" + TENANT + "/" + NAMESPACE + "/" + topic;

        // Grant produce permission with topic level permission to ANOTHER_USER
        admin.topics().grantPermission(fullNewTopicName,
                ANOTHER_USER,
                Sets.newHashSet(AuthAction.produce));

        @Cleanup
        KProducer producer = new KProducer(fullNewTopicName, false, "localhost", getKafkaBrokerPort(),
                TENANT + "/" + NAMESPACE, "token:" + anotherToken);
        int totalMsgs = 10;
        String messageStrPrefix = fullNewTopicName + "_message_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            producer.getProducer().send(new ProducerRecord<>(fullNewTopicName, i, messageStr)).get();
        }

        // Ensure admin can consume message.
        @Cleanup
        KConsumer adminConsumer = new KConsumer(fullNewTopicName, "localhost", getKafkaBrokerPort(), false,
                TENANT + "/" + NAMESPACE, "token:" + adminToken, "DemoAdminKafkaOnPulsarConsumer");
        adminConsumer.getConsumer().subscribe(Collections.singleton(fullNewTopicName));

        int i = 0;
        while (i < totalMsgs) {
            ConsumerRecords<Integer, String> records = adminConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                i++;
            }
        }
        assertEquals(i, totalMsgs);

        // no more records
        ConsumerRecords<Integer, String> records = adminConsumer.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());


        // Consume should be failed.
        @Cleanup
        KConsumer anotherConsumer = new KConsumer(fullNewTopicName, "localhost", getKafkaBrokerPort(), false,
                TENANT + "/" + NAMESPACE, "token:" + anotherToken, "DemoAnotherKafkaOnPulsarConsumer");
        anotherConsumer.getConsumer().subscribe(Collections.singleton(fullNewTopicName));
        try {
            anotherConsumer.getConsumer().poll(Duration.ofSeconds(10));
            fail("expected TopicAuthorizationException");
        } catch (TopicAuthorizationException ignore) {
            log.info("Has TopicAuthorizationException.");
        }
    }

    @Test(timeOut = 20000)
    public void testListTopic() throws Exception {
        String newTopic = "newTestListTopic";
        String fullNewTopicName = "persistent://" + TENANT + "/" + NAMESPACE + "/" + newTopic;

        KConsumer kConsumer = new KConsumer(TOPIC, "localhost", getKafkaBrokerPort(), false,
                TENANT + "/" + NAMESPACE, "token:" + userToken, "DemoKafkaOnPulsarConsumer");
        Map<String, List<PartitionInfo>> result = kConsumer.getConsumer().listTopics(Duration.ofSeconds(1));
        assertEquals(result.size(), 1);
        assertFalse(result.containsKey(newTopic));

        // Create newTopic
        admin.topics().createPartitionedTopic(fullNewTopicName, 1);

        // Grant topic level permission to ANOTHER_USER
        admin.topics().grantPermission(fullNewTopicName,
                ANOTHER_USER,
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        @Cleanup
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(this.conf.getBrokerClientAuthenticationPlugin(),
                        "token:" + anotherToken).build();
        try {
            pulsarAdmin.topics().getList(TENANT + "/" + NAMESPACE);
            fail("Should fail with NotAuthorizedException");
        } catch (PulsarAdminException.NotAuthorizedException ex) {
            // ignore
        }

        // Use consumer to list topic
        result = kConsumer.getConsumer().listTopics(Duration.ofSeconds(1));
        assertEquals(result.size(), 2);
        assertTrue(result.containsKey(newTopic));

        // Check AdminClient use specific user to list topic
        AdminClient adminClient = createAdminClient(TENANT + "/" + NAMESPACE, anotherToken);
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> topics = listTopicsResult.names().get();

        assertEquals(topics.size(), 0);

        // Cleanup
        kConsumer.close();
        adminClient.close();
        admin.topics().deletePartitionedTopic(fullNewTopicName);
    }

    @Test(timeOut = 20000)
    void testCannotAccessAnotherTenant() throws Exception {
        String newTopic = "newTestListTopic";
        String fullNewTopicName = "persistent://" + TENANT + "/" + NAMESPACE + "/" + newTopic;
        KConsumer kConsumer = new KConsumer(TOPIC, "localhost", getKafkaBrokerPort(), false,
            TENANT + "/" + NAMESPACE, "token:" + userToken, "DemoKafkaOnPulsarConsumer");
        Map<String, List<PartitionInfo>> result = kConsumer.getConsumer().listTopics(Duration.ofSeconds(1));
        // ensure that topic does not exist
        assertFalse(result.containsKey(newTopic));

        // Create newTopic
        admin.topics().createPartitionedTopic(fullNewTopicName, 1);
        result = kConsumer.getConsumer().listTopics(Duration.ofSeconds(1));
        assertTrue(result.containsKey(newTopic));

        kConsumer.getConsumer().subscribe(Collections.singleton(newTopic));
        // okay to read
        kConsumer.getConsumer().poll(Duration.ofSeconds(1));
        assertEquals(1, kConsumer.getConsumer().assignment().size());
        kConsumer.close();

        String newTenant = "secondTenant";
        String newTenantTopic = "persistent://" + newTenant + "/" + NAMESPACE + "/topic1";
        try {
            // create a new tenant on Pulsar
            admin.tenants().createTenant(newTenant,
                    TenantInfo.builder()
                            .adminRoles(Collections.singleton(ADMIN_USER))
                            .allowedClusters(Collections.singleton(configClusterName))
                            .build());
            admin.namespaces().createNamespace(newTenant + "/" + NAMESPACE);
            admin.topics().createPartitionedTopic(newTenantTopic, 1);

            // try to access a topic belonging to the newTenant
            // authentication pass because username (tenant) is for an existing tenant,
            // and the user cannot access that tenant
            KConsumer kConsumer2 = new KConsumer(newTenantTopic, "localhost", getKafkaBrokerPort(), false,
                TENANT + "/" + NAMESPACE, "token:" + userToken, "DemoKafkaOnPulsarConsumer");

            result = kConsumer2.getConsumer().listTopics(Duration.ofSeconds(1));
            // ensure that the consumer cannot see the new topic
            assertFalse(result.containsKey(newTenantTopic));

            // no error on subscribe, subscription is stored in the user tenant
            // we are not polluting the newTenant space
            kConsumer2.getConsumer().subscribe(Collections.singleton(newTenantTopic));

            // not okay to read
            try {
                kConsumer2.getConsumer().poll(Duration.ofSeconds(1));
                fail("should fail");
            } catch (TopicAuthorizationException expected) {
                assertEquals("Not authorized to access topics: [" + newTenantTopic + "]", expected.getMessage());
            }
            assertTrue(kConsumer2.getConsumer().assignment().isEmpty());

            kConsumer2.close();

            // assert that user can produce to a legit topic
            KProducer kProducer = new KProducer(TOPIC, false, "localhost", getKafkaBrokerPort(),
                    newTenant + "/" + NAMESPACE, "token:" + userToken);
            kProducer.getProducer().send(new ProducerRecord<>(TOPIC, 0, "")).get();
            kProducer.close();

            // assert that user cannot produce to the other tenant
            KProducer kProducer2 = new KProducer(newTenantTopic, false, "localhost", getKafkaBrokerPort(),
                    newTenant + "/" + NAMESPACE, "token:" + userToken);
            try {
                kProducer2.getProducer().send(new ProducerRecord<>(newTenantTopic, 0, "")).get();
                fail("should fail");
            } catch (ExecutionException expected) {
                assertTrue(expected.getCause() instanceof TopicAuthorizationException);
                assertEquals("Not authorized to access topics: [" + newTenantTopic + "]",
                        expected.getCause().getMessage());
            }
            kProducer2.close();
        } finally {
            // Cleanup
            admin.topics().deletePartitionedTopic(newTenantTopic);
        }

        // Cleanup
        admin.topics().deletePartitionedTopic(fullNewTopicName);
    }

    @Test(timeOut = 20000)
    void testProduceFailed() throws PulsarAdminException, ExecutionException, InterruptedException {
        String newTenant = "newProduceFailed";
        String testTopic = "persistent://" + newTenant + "/" + NAMESPACE + "/topic1";
        try {
            admin.tenants().createTenant(newTenant,
                    TenantInfo.builder()
                            .adminRoles(Collections.singleton(ADMIN_USER))
                            .allowedClusters(Collections.singleton(configClusterName))
                            .build());
            admin.namespaces().createNamespace(newTenant + "/" + NAMESPACE);
            admin.namespaces().grantPermissionOnNamespace(newTenant + "/" + NAMESPACE, SIMPLE_USER,
                    Sets.newHashSet(AuthAction.consume));
            admin.topics().createPartitionedTopic(testTopic, 1);

            // Admin must have produce permissions
            @Cleanup
            KProducer adminProducer = new KProducer(testTopic, false, "localhost", getKafkaBrokerPort(),
                    newTenant + "/" + NAMESPACE, "token:" + adminToken);
            int totalMsgs = 10;
            String messageStrPrefix = testTopic + "_message_";

            for (int i = 0; i < totalMsgs; i++) {
                String messageStr = messageStrPrefix + i;
                adminProducer.getProducer().send(new ProducerRecord<>(testTopic, i, messageStr)).get();
            }

            // Ensure can consume message.
            @Cleanup
            KConsumer kConsumer = new KConsumer(testTopic, "localhost", getKafkaBrokerPort(), false,
                    newTenant + "/" + NAMESPACE, "token:" + adminToken, "DemoKafkaOnPulsarConsumer");
            kConsumer.getConsumer().subscribe(Collections.singleton(testTopic));

            int i = 0;
            while (i < totalMsgs) {
                ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
                for (ConsumerRecord<Integer, String> record : records) {
                    Integer key = record.key();
                    assertEquals(messageStrPrefix + key.toString(), record.value());
                    i++;
                }
            }
            assertEquals(i, totalMsgs);

            // no more records
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofMillis(200));
            assertTrue(records.isEmpty());

            // User can't produce, because don't have produce action.
            @Cleanup
            KProducer kProducer = new KProducer(testTopic, false, "localhost", getKafkaBrokerPort(),
                    newTenant + "/" + NAMESPACE, "token:" + userToken);
            try {
                kProducer.getProducer().send(new ProducerRecord<>(testTopic, 0, "")).get();
                fail("expected TopicAuthorizationException");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof TopicAuthorizationException);
            }
        } finally {
            // Cleanup
            admin.topics().deletePartitionedTopic(testTopic);
        }
    }

    @Test(timeOut = 20000)
    void testConsumeFailed() throws PulsarAdminException, ExecutionException, InterruptedException {
        String newTenant = "testConsumeFailed";
        String testTopic = "persistent://" + newTenant + "/" + NAMESPACE + "/topic1";
        try {
            // Create new tenant, namespace and topic
            admin.tenants().createTenant(newTenant,
                    TenantInfo.builder()
                            .adminRoles(Collections.singleton(ADMIN_USER))
                            .allowedClusters(Collections.singleton(configClusterName))
                            .build());
            admin.namespaces().createNamespace(newTenant + "/" + NAMESPACE);
            admin.namespaces().grantPermissionOnNamespace(newTenant + "/" + NAMESPACE, SIMPLE_USER,
                    Sets.newHashSet(AuthAction.produce));
            admin.topics().createPartitionedTopic(testTopic, 1);

            // SIMPLE_USER can produce
            @Cleanup
            KProducer adminProducer = new KProducer(testTopic, false, "localhost", getKafkaBrokerPort(),
                    newTenant + "/" + NAMESPACE, "token:" + userToken);
            adminProducer.getProducer().send(new ProducerRecord<>(testTopic, 0, "message")).get();


            // Consume should be failed.
            @Cleanup
            KConsumer kConsumer = new KConsumer(testTopic, "localhost", getKafkaBrokerPort(), false,
                    newTenant + "/" + NAMESPACE, "token:" + userToken, "DemoKafkaOnPulsarConsumer");
            kConsumer.getConsumer().subscribe(Collections.singleton(testTopic));
            try {
                kConsumer.getConsumer().poll(Duration.ofSeconds(10));
                fail("expected TopicAuthorizationException");
            } catch (TopicAuthorizationException ignore) {
                log.info("Has TopicAuthorizationException.");
            }
        } finally {
            // Cleanup
            admin.topics().deletePartitionedTopic(testTopic);
        }
    }

    @Test(timeOut = 20000)
    public void testCreateTopicSuccess() throws ExecutionException, InterruptedException, PulsarAdminException {
        String newTopic = "testCreateTopicSuccess";
        String fullNewTopicName = "persistent://" + TENANT + "/" + NAMESPACE + "/" + newTopic;

        AdminClient adminClient = createAdminClient(TENANT + "/" + NAMESPACE, adminToken);
        CreateTopicsResult result =
                adminClient.createTopics(Collections.singleton(new NewTopic(fullNewTopicName, 1, (short) 1)));
        result.all().get();

        try {
            admin.topics().createPartitionedTopic(fullNewTopicName, 1);
        } catch (PulsarAdminException exception) {
            assertTrue(exception.getMessage().contains("This topic already exists"));
        }
        admin.topics().deletePartitionedTopic(fullNewTopicName);
        adminClient.close();
    }

    @Test(timeOut = 20000)
    public void testCreateTopicFailed() throws PulsarAdminException {
        String newTopic = "testCreateTopicFailed";
        String fullNewTopicName = "persistent://" + TENANT + "/" + NAMESPACE + "/" + newTopic;

        AdminClient adminClient = createAdminClient(TENANT + "/" + NAMESPACE, userToken);
        CreateTopicsResult result =
                adminClient.createTopics(Collections.singleton(new NewTopic(fullNewTopicName, 1, (short) 1)));
        try {
            result.all().get();
        } catch (ExecutionException | InterruptedException ex) {
            assertTrue(ex.getMessage().contains("TopicAuthorizationException"));
        }
        admin.topics().createPartitionedTopic(fullNewTopicName, 1);
        admin.topics().deletePartitionedTopic(fullNewTopicName);
        adminClient.close();
    }

    @Test(timeOut = 20000)
    public void testDeleteTopicSuccess() throws PulsarAdminException, InterruptedException {
        String newTopic = "testDeleteTopicSuccess";
        String fullNewTopicName = "persistent://" + TENANT + "/" + NAMESPACE + "/" + newTopic;

        admin.topics().createPartitionedTopic(fullNewTopicName, 1);

        AdminClient adminClient = createAdminClient(TENANT + "/" + NAMESPACE, adminToken);
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(newTopic));
        try {
            deleteTopicsResult.all().get();
        } catch (ExecutionException ex) {
            fail("Should success but have : " + ex.getMessage());
        }
        List<String> topicList = admin.topics().getList(TENANT + "/" + NAMESPACE);
        topicList.forEach(topic -> {
            if (topic.startsWith(fullNewTopicName)) {
                fail("Delete topic failed!");
            }
        });

        adminClient.close();
    }

    @Test(timeOut = 20000)
    public void testDeleteTopicFailed() throws PulsarAdminException, InterruptedException {
        String newTopic = "testDeleteTopicFailed";
        String fullNewTopicName = "persistent://" + TENANT + "/" + NAMESPACE + "/" + newTopic;

        admin.topics().createPartitionedTopic(fullNewTopicName, 1);

        AdminClient adminClient = createAdminClient(TENANT + "/" + NAMESPACE, userToken);
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(newTopic));
        try {
            deleteTopicsResult.all().get();
            fail("Should delete failed!");
        } catch (ExecutionException ex) {
            log.info("Test delete topic failed", ex);
            assertTrue(ex.getMessage().contains("TopicAuthorizationException"));
        }
        try {
            admin.topics().createPartitionedTopic(fullNewTopicName, 1);
        } catch (PulsarAdminException exception) {
            assertTrue(exception.getMessage().contains("This topic already exists"));
        }
        admin.topics().deletePartitionedTopic(fullNewTopicName);
        adminClient.close();
    }

    @Test(timeOut = 20000)
    public void testDescribeConfigSuccess() throws PulsarAdminException, ExecutionException, InterruptedException {
        String newTopic = "testDescribeConfigSuccess";
        String fullNewTopicName = "persistent://" + TENANT + "/" + NAMESPACE + "/" + newTopic;

        admin.topics().createPartitionedTopic(fullNewTopicName, 1);
        AdminClient adminClient = createAdminClient(TENANT + "/" + NAMESPACE, adminToken);
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, fullNewTopicName);

        DescribeConfigsResult describeConfigsResult =
                adminClient.describeConfigs(Collections.singleton(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        assertEquals(configResourceConfigMap.size(), 1);
        Config config = configResourceConfigMap.get(configResource);
        assertFalse(config.entries().isEmpty());

        admin.topics().deletePartitionedTopic(fullNewTopicName);
    }

    @Test(timeOut = 20000)
    public void testDescribeConfigFailed() throws PulsarAdminException {
        String newTopic = "testDescribeConfigFailed";
        String fullNewTopicName = "persistent://" + TENANT + "/" + NAMESPACE + "/" + newTopic;

        admin.topics().createPartitionedTopic(fullNewTopicName, 1);
        AdminClient adminClient = createAdminClient(TENANT + "/" + NAMESPACE, userToken);
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, fullNewTopicName);

        DescribeConfigsResult describeConfigsResult =
                adminClient.describeConfigs(Collections.singleton(configResource));
        try {
            describeConfigsResult.all().get();
        } catch (Exception ex) {
            log.error("Error", ex + "");
            assertTrue(ex.getMessage().contains("TopicAuthorizationException"));
        }

        admin.topics().deletePartitionedTopic(fullNewTopicName);
    }


    private AdminClient createAdminClient(String username, String token) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule "
                + "required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, username, "token:" + token);
        props.put("sasl.jaas.config", jaasCfg);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        return AdminClient.create(props);
    }

    @DataProvider(name = "tokenPrefix")
    public static Object[][] tokenPrefix() {
        return new Object[][] { { true }, { false } };
    }

    // this test creates the schema registry topic, and this may interfere with other tests
    @Test(timeOut = 30000, priority = 1000, dataProvider = "tokenPrefix")
    public void testAvroProduceAndConsumeWithAuth(boolean withTokenPrefix) throws Exception {

        if (conf.isKafkaEnableMultiTenantMetadata()) {
            // ensure that the KOP metadata namespace exists and that the user can write to it
            // because we require "produce" permissions on the Schema Registry Topic
            // while working in Multi Tenant mode
            if (!admin.namespaces().getNamespaces(TENANT).contains(TENANT + "/" + conf.getKafkaMetadataNamespace())) {
                admin.namespaces().createNamespace(TENANT + "/" + conf.getKafkaMetadataNamespace());
            }
            admin.namespaces()
                    .grantPermissionOnNamespace(TENANT + "/" + conf.getKafkaMetadataNamespace(), SIMPLE_USER,
                            Sets.newHashSet(AuthAction.produce, AuthAction.consume));
        }

        String topic = "SchemaRegistryTest-testAvroProduceAndConsumeWithAuth" + withTokenPrefix;
        IndexedRecord avroRecord = createAvroRecord();
        Object[] objects = new Object[]{ avroRecord, true, 130, 345L, 1.23f, 2.34d, "abc", "def".getBytes() };
        @Cleanup
        KafkaProducer<Integer, Object> producer = createAvroProducer(withTokenPrefix);
        for (int i = 0; i < objects.length; i++) {
            final Object object = objects[i];
            producer.send(new ProducerRecord<>(topic, i, object), (metadata, e) -> {
                if (e != null) {
                    log.error("Failed to send {}: {}", object, e.getMessage());
                    Assert.fail("Failed to send " + object + ": " + e.getMessage());
                }
                log.info("Success send {} to {}-partition-{}@{}",
                        object, metadata.topic(), metadata.partition(), metadata.offset());
            }).get();
        }
        producer.close();

        @Cleanup
        KafkaConsumer<Integer, Object> consumer = createAvroConsumer(withTokenPrefix);
        consumer.subscribe(Collections.singleton(topic));
        int i = 0;
        while (i < objects.length) {
            for (ConsumerRecord<Integer, Object> record : consumer.poll(Duration.ofSeconds(3))) {
                assertEquals(record.key().intValue(), i);
                assertEquals(record.value(), objects[i]);
                i++;
            }
        }
        consumer.close();
    }

    @Test(timeOut = 30000)
    public void testSchemaNoAuth() {
        final KafkaProducer<Integer, Object> producer = createAvroProducer(false, false);
        try {
            producer.send(new ProducerRecord<>("test-avro-wrong-auth", createAvroRecord())).get();
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof RestClientException);
            var restException = (RestClientException) e.getCause();
            assertEquals(restException.getErrorCode(), HttpResponseStatus.UNAUTHORIZED.code());
            assertTrue(restException.getMessage().contains("Missing AUTHORIZATION header"));
        }
        producer.close();
    }

    private IndexedRecord createAvroRecord() {
        String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
                + "\"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("name", "testUser");
        return avroRecord;
    }

    private KafkaProducer<Integer, Object> createAvroProducer(boolean withTokenPrefix) {
        return createAvroProducer(withTokenPrefix, true);
    }

    private KafkaProducer<Integer, Object> createAvroProducer(boolean withTokenPrefix, boolean withSchemaToken) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getClientPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, restConnect);

        String username = TENANT;
        String password = "token:" + userToken;

        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule "
                + "required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, username, password);
        props.put("sasl.jaas.config", jaasCfg);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");

        if (withSchemaToken) {
            props.put(KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
            props.put(KafkaAvroSerializerConfig.USER_INFO_CONFIG,
                    username + ":" + (withTokenPrefix ? password : userToken));
        }

        return new KafkaProducer<>(props);
    }


    private KafkaConsumer<Integer, Object> createAvroConsumer(boolean withTokenPrefix) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getClientPort());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avroGroup");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, restConnect);

        props.put(KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        String username = TENANT;
        String password = "token:" + userToken;

        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule "
                + "required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, username, password);
        props.put("sasl.jaas.config", jaasCfg);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put(KafkaAvroSerializerConfig.USER_INFO_CONFIG,
                username + ":" + (withTokenPrefix ? password : userToken));
        return new KafkaConsumer<>(props);
    }

}
