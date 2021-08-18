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
import javax.crypto.SecretKey;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
@Slf4j
public abstract class KafkaAuthorizationTestBase extends KopProtocolHandlerTestBase{

    private static final String TENANT = "KafkaAuthorizationTest";
    private static final String NAMESPACE = "ns1";
    private static final String SHORT_TOPIC = "topic1";
    private static final String TOPIC = "persistent://" + TENANT + "/" + NAMESPACE + "/" + SHORT_TOPIC;

    private static final String SIMPLE_USER = "muggle_user";
    private static final String ANOTHER_USER = "death_eater_user";
    private static final String ADMIN_USER = "admin_user";

    private String adminToken;
    private String userToken;
    private String anotherToken;

    public KafkaAuthorizationTestBase(final String entryFormat) {
        super(entryFormat);
    }

    @BeforeMethod
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

        super.resetConfig();
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
                Sets.newHashSet("org.apache.pulsar.broker.authentication."
                        + "AuthenticationProviderToken"));
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

    @AfterMethod
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000)
    void testAuthorizationFailed() {
        try {
            String newTenant = "newTenant";
            String testTopic = "persistent://" + newTenant + "/" + NAMESPACE + "/topic1";
            admin.tenants().createTenant(newTenant,
                    TenantInfo.builder()
                            .adminRoles(Collections.singleton(ADMIN_USER))
                            .allowedClusters(Collections.singleton(configClusterName))
                            .build());
            admin.namespaces().createNamespace(newTenant + "/" + NAMESPACE);
            admin.topics().createPartitionedTopic(testTopic, 1);
            @Cleanup
            KProducer kProducer = new KProducer(testTopic, false, "localhost", getKafkaBrokerPort(),
                    TENANT + "/" + NAMESPACE, "token:" + userToken);
            kProducer.getProducer().send(new ProducerRecord<>(testTopic, 0, "")).get();
            fail("should have failed");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("TopicAuthorizationException"));
        }
    }

    @Test(timeOut = 20000)
    void testSuccessAutoCreateTopicBySuperUser() {
        String topic = "newTopic";
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
        Map<String, List<PartitionInfo>> result = kConsumer
                .getConsumer().listTopics(Duration.ofSeconds(1));
        assertEquals(result.size(), 2);
        assertTrue(result.containsKey(topic),
                "list of topics " + result.keySet().toString() + "  does not contains " + topic);
    }

    @Test(timeOut = 20000)
    void testAutoCreateTopicFailedBySimpleUser() {
        try {
            String topic = "newTopic";
            @Cleanup
            KProducer kProducer = new KProducer(topic, false, "localhost", getKafkaBrokerPort(),
                    TENANT + "/" + NAMESPACE, "token:" + userToken);
            kProducer.getProducer().send(new ProducerRecord<>(topic, 0, "")).get();
            fail("should have failed");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("TopicAuthorizationException"));
        }
    }

    @Test(timeOut = 20000)
    void testListTopic() throws Exception {
        String newTopic = "newTopic";
        String fullNewTopicName = "persistent://" + TENANT + "/" + NAMESPACE + "/" + newTopic;
        @Cleanup
        KConsumer kConsumer = new KConsumer(TOPIC, "localhost", getKafkaBrokerPort(), false,
                TENANT + "/" + NAMESPACE, "token:" + userToken, "DemoKafkaOnPulsarConsumer");
        Map<String, List<PartitionInfo>> result = kConsumer.getConsumer().listTopics(Duration.ofSeconds(1));
        assertEquals(result.size(), 1);
        admin.topics().createPartitionedTopic(fullNewTopicName, 1);

        // Grant topic level permission to ANOTHER_USER
        admin.topics().grantPermission(fullNewTopicName,
                ANOTHER_USER,
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));

        // Use consumer to list topic
        result = kConsumer.getConsumer().listTopics(Duration.ofSeconds(1));
        assertEquals(result.size(), 2);
        assertTrue(result.containsKey(newTopic));

        // Check AdminClient use specific user to list topic
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule "
                + "required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, TENANT + "/" + NAMESPACE, "token:" + anotherToken);
        props.put("sasl.jaas.config", jaasCfg);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        AdminClient adminClient = AdminClient.create(props);
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> topics = listTopicsResult.names().get();
        assertEquals(topics.size(), 1);
        assertTrue(topics.contains(newTopic));
    }


}
