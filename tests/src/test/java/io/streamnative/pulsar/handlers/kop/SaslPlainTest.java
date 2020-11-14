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
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Testing the SASL-PLAIN features on KoP.
 */
@Test
@Slf4j
public class SaslPlainTest extends KopProtocolHandlerTestBase {

    private static final String SIMPLE_USER = "muggle_user";
    private static final String TENANT = "SaslPlainTest";
    private static final String ANOTHER_USER = "death_eater_user";
    private static final String ADMIN_USER = "admin_user";
    private static final String NAMESPACE = "ns1";
    private static final String TOPIC = "persistent://" + TENANT + "/" + NAMESPACE + "/topic1";
    private String adminToken;
    private String userToken;
    private String anotherToken;

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

        super.resetConfig();
        ((KafkaServiceConfiguration) conf).setEnableGroupCoordinator(true);
        ((KafkaServiceConfiguration) conf).setSaslAllowedMechanisms(Sets.newHashSet("PLAIN"));
        ((KafkaServiceConfiguration) conf).setKafkaMetadataTenant("internal");
        ((KafkaServiceConfiguration) conf).setKafkaMetadataNamespace("__kafka");

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

        admin.tenants().createTenant(TENANT,
            new TenantInfo(Sets.newHashSet(ADMIN_USER), Sets.newHashSet(super.configClusterName)));
        admin.namespaces().createNamespace(TENANT + "/" + NAMESPACE);
        admin.topics().createPartitionedTopic(TOPIC, 1);
        admin
            .namespaces().grantPermissionOnNamespace(TENANT + "/" + NAMESPACE, SIMPLE_USER,
            Sets.newHashSet(AuthAction.consume, AuthAction.produce));
    }

    @Override
    protected void createAdmin() throws Exception {
        super.admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
            .authentication(this.conf.getBrokerClientAuthenticationPlugin(),
                this.conf.getBrokerClientAuthenticationParameters()).build());
    }

    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 40000)
    void simpleProduceAndConsume() throws Exception {
        KProducer kProducer = new KProducer(TOPIC, false, "localhost", getKafkaBrokerPort(),
            TENANT + "/" + NAMESPACE, "token:" + userToken);
        int totalMsgs = 10;
        String messageStrPrefix = TOPIC + "_message_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer().send(new ProducerRecord<>(TOPIC, i, messageStr));
        }

        KConsumer kConsumer = new KConsumer(TOPIC, "localhost", getKafkaBrokerPort(), false,
            TENANT + "/" + NAMESPACE, "token:" + userToken, "DemoKafkaOnPulsarConsumer");
        kConsumer.getConsumer().subscribe(Collections.singleton(TOPIC));

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
        assertEquals(result.size(), 1);
        assertTrue(result.containsKey(TOPIC),
            "list of topics " + result.keySet().toString() + "  does not contains " + TOPIC);
    }

    @Test(timeOut = 20000)
    void badCredentialFail() throws Exception {
        try {
            @Cleanup
            KProducer kProducer = new KProducer(TOPIC, false, "localhost", getKafkaBrokerPort(),
                TENANT + "/" + NAMESPACE, "token:dsa");
            kProducer.getProducer().send(new ProducerRecord<>(TOPIC, 0, "")).get();
            fail("should have failed");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("SaslAuthenticationException"));
        }
    }

    @Test(timeOut = 20000)
    void badUserFail() throws Exception {
        try {
            @Cleanup
            KProducer kProducer = new KProducer(TOPIC, false, "localhost", getKafkaBrokerPort(),
                TENANT + "/" + NAMESPACE, "token:" + anotherToken);
            kProducer.getProducer().send(new ProducerRecord<>(TOPIC, 0, "")).get();
            fail("should have failed");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("SaslAuthenticationException"));
        }
    }

    @Test(timeOut = 20000)
    void badNamespaceProvided() throws Exception {
        try {
            KProducer kProducer = new KProducer(TOPIC, false, "localhost", getKafkaBrokerPort(),
                TENANT + "/ns2", "token:" + userToken);
            kProducer.getProducer().send(new ProducerRecord<>(TOPIC, 0, "")).get();
            fail("should have failed");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("SaslAuthenticationException"));
        }
    }

    @Test(timeOut = 20000)
    void clientWithoutAuth() throws Exception {
        final int metadataTimeoutMs = 8000;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Integer.toString(metadataTimeoutMs));

        @Cleanup
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        try {
            producer.send(new ProducerRecord<>(TOPIC, "", "hello")).get();
            fail("should have failed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
            assertTrue(e.getMessage().contains("Failed to update metadata"));
        }
    }
}
