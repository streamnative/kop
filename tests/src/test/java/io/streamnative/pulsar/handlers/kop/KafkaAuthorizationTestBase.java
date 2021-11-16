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

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;

import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));
        ServiceConfiguration authConf = new ServiceConfiguration();
        authConf.setProperties(properties);
        provider.initialize(authConf);

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
        assertTrue(result.containsKey(SHORT_TOPIC),
                "list of topics " + result.keySet() + "  does not contains " + SHORT_TOPIC);
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
        assertTrue(result.containsKey(SHORT_TOPIC),
                "list of topics " + result.keySet() + "  does not contains " + SHORT_TOPIC);
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
        assertTrue(result.containsKey(SHORT_TOPIC),
                "list of topics " + result.keySet() + "  does not contains " + SHORT_TOPIC);
        assertFalse(result.containsKey(newTopic),
                "list of topics " + result.keySet() + "  shouldn't contains " + newTopic);

        // Create newTopic
        admin.topics().createPartitionedTopic(fullNewTopicName, 1);

        // Grant topic level permission to ANOTHER_USER
        admin.topics().grantPermission(fullNewTopicName,
                ANOTHER_USER,
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        // Use consumer to list topic
        result = kConsumer.getConsumer().listTopics(Duration.ofSeconds(1));
        assertTrue(result.containsKey(SHORT_TOPIC),
                "list of topics " + result.keySet() + "  does not contains " + SHORT_TOPIC);
        assertTrue(result.containsKey(newTopic),
                "list of topics " + result.keySet() + "  does not contains " + newTopic);

        // Check AdminClient use specific user to list topic
        AdminClient adminClient = createAdminClient(TENANT + "/" + NAMESPACE, anotherToken);
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> topics = listTopicsResult.names().get();

        assertEquals(topics.size(), 1);
        assertTrue(topics.contains(newTopic));

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

}
