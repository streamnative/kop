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

import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test Pulsar Auth Enabled will not affect KoP usage.
 * Verify Pulsar auth enabled, and KoP could consume/publish success,
 * because internal KoP using Pulsar internal admin/client
 * This is similar to SaslPlainTest, but the KoP SASL is not enabled.
 */
@Slf4j
public abstract class PulsarAuthEnabledTestBase extends KopProtocolHandlerTestBase {
    private static final String TENANT = "PulsarAuthEnabledTest";
    private static final String ADMIN_USER = "admin_user";
    private static final String NAMESPACE = "ns2";
    private static final String SHORT_TOPIC = "topic2";
    private static final String TOPIC = "persistent://" + TENANT + "/" + NAMESPACE + "/" + SHORT_TOPIC;
    private String adminToken;

    public PulsarAuthEnabledTestBase(final String entryFormat) {
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

        adminToken = AuthTokenUtils.createToken(secretKey, ADMIN_USER, Optional.empty());

        super.resetConfig();

        ((KafkaServiceConfiguration) conf).setKafkaTenant(TENANT);
        ((KafkaServiceConfiguration) conf).setKafkaNamespace(NAMESPACE);
        ((KafkaServiceConfiguration) conf).setKafkaMetadataTenant("internal");
        ((KafkaServiceConfiguration) conf).setKafkaMetadataNamespace("__kafka");
        ((KafkaServiceConfiguration) conf).setEnableGroupCoordinator(true);

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
        admin.namespaces()
            .setNamespaceReplicationClusters(TENANT + "/" + NAMESPACE, Sets.newHashSet(super.configClusterName));
        admin.topics().createPartitionedTopic(TOPIC, 1);
        admin.namespaces().grantPermissionOnNamespace(TENANT + "/" + NAMESPACE, ADMIN_USER,
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
    void simpleProduceAndConsumeWithPulsarAuthed() throws Exception {
        @Cleanup
        KProducer kProducer = new KProducer(TOPIC, false, getKafkaBrokerPort());

        int totalMsgs = 10;
        String messageStrPrefix = TOPIC + "_message_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer().send(new ProducerRecord<>(TOPIC, i, messageStr));
        }
        KConsumer kConsumer = new KConsumer(TOPIC, getKafkaBrokerPort(), "DemoKafkaOnPulsarConsumer");

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
        assertTrue(result.containsKey(SHORT_TOPIC),
            "list of topics " + result.keySet().toString() + "  does not contains " + SHORT_TOPIC);
    }

}
