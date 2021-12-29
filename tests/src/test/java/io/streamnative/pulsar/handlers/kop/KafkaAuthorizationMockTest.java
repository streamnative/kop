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
import io.streamnative.pulsar.handlers.kop.security.auth.KafkaMockAuthorizationProvider;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import javax.crypto.SecretKey;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test for Authorization with `entryFormat=pulsar`.
 */
public class KafkaAuthorizationMockTest extends KopProtocolHandlerTestBase {

    protected static final String TENANT = "KafkaAuthorizationTest";
    protected static final String NAMESPACE = "ns1";
    private static final SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

    protected static final String ADMIN_USER = "pass.pass";

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));

        String adminToken = AuthTokenUtils.createToken(secretKey, ADMIN_USER, Optional.empty());

        conf.setSaslAllowedMechanisms(Sets.newHashSet("PLAIN"));
        conf.setKafkaMetadataTenant("internal");
        conf.setKafkaMetadataNamespace("__kafka");
        conf.setKafkaTenant(TENANT);
        conf.setKafkaNamespace(NAMESPACE);

        conf.setClusterName(super.configClusterName);
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationAllowWildcardsMatching(true);
        conf.setAuthorizationProvider(KafkaMockAuthorizationProvider.class.getName());
        conf.setAuthenticationProviders(
                Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters("token:" + adminToken);
        conf.setProperties(properties);

        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(this.conf.getBrokerClientAuthenticationPlugin(),
                        this.conf.getBrokerClientAuthenticationParameters()).build());
    }

    @Override
    protected void createAdmin() throws Exception {
        super.admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(this.conf.getBrokerClientAuthenticationPlugin(),
                        this.conf.getBrokerClientAuthenticationParameters()).build());
    }


    @Test(timeOut = 30 * 1000)
    public void testSuperUserProduceAndConsume() throws PulsarAdminException {
        String superUserToken = AuthTokenUtils.createToken(secretKey, "pass.pass", Optional.empty());
        String topic = "testSuperUserProduceAndConsumeTopic";
        String fullNewTopicName = "persistent://" + TENANT + "/" + NAMESPACE + "/" + topic;
        KProducer kProducer = new KProducer(topic, false, "localhost", getKafkaBrokerPort(),
                TENANT + "/" + NAMESPACE, "token:" + superUserToken);
        int totalMsgs = 10;
        String messageStrPrefix = topic + "_message_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer().send(new ProducerRecord<>(topic, i, messageStr));
        }
        KConsumer kConsumer = new KConsumer(topic, "localhost", getKafkaBrokerPort(), false,
                TENANT + "/" + NAMESPACE, "token:" + superUserToken, "DemoKafkaOnPulsarConsumer");
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
        assertEquals(result.size(), 1);
        assertTrue(result.containsKey(topic),
                "list of topics " + result.keySet() + "  does not contains " + topic);

        // Cleanup
        kProducer.close();
        kConsumer.close();
        admin.topics().deletePartitionedTopic(fullNewTopicName);
    }
}
