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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Testing the SASL-OAUTHBEARER features on KoP.
 */
@Slf4j
public class SaslOAuthBearerTest extends KopProtocolHandlerTestBase {

    private static final String ADMIN_USER = "admin_user";

    public SaslOAuthBearerTest() {
        super("kafka");
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        final SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        super.resetConfig();
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));

        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters(
                "token:" + AuthTokenUtils.createToken(secretKey, ADMIN_USER, Optional.empty()));
        conf.setSuperUserRoles(Sets.newHashSet(ADMIN_USER));
        final Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));
        conf.setProperties(properties);

        conf.setKopOauth2ConfigFile("src/test/resources/kop-oauth2.properties");

        conf.setSaslAllowedMechanisms(Sets.newHashSet("OAUTHBEARER"));
        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void createAdmin() throws Exception {
        super.admin = spy(PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrl.toString())
                .authentication(
                        conf.getBrokerClientAuthenticationPlugin(),
                        conf.getBrokerClientAuthenticationParameters())
                .build());
    }

    @Test(timeOut = 15000)
    public void testSimpleProduceConsume() throws Exception {
        final String topic = "testSimpleProduceConsume";
        final String message = "hello";

        final Properties producerProps = newProducerProperties();
        configureOauth2(producerProps);
        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, message)).get();
        log.info("Send to {}-partition-{}@{}", metadata.topic(), metadata.partition(), metadata.offset());

        final Properties consumerProps = newConsumerProperties();
        configureOauth2(consumerProps);
        @Cleanup
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(topic));

        final List<String> receivedMessages = new ArrayList<>();
        while (receivedMessages.isEmpty()) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                receivedMessages.add(record.value());
                log.info("Receive {} from {}-partition-{}@{}",
                        record.value(), record.topic(), record.partition(), record.offset());
            }
        }
        assertEquals(receivedMessages.size(), 1);
        assertEquals(receivedMessages.get(0), message);
    }

    @Test(timeOut = 15000)
    public void testProduceWithoutAuth() throws Exception {
        final String topic = "testProduceWithoutAuth";

        final Properties producerProps = newProducerProperties();
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);

        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        try {
            producer.send(new ProducerRecord<>(topic, "", "hello")).get();
            fail("should have failed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
            assertTrue(e.getMessage().contains("Failed to update metadata"));
        }
    }

    private Properties newProducerProperties() {
        final Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    private Properties newConsumerProperties() {
        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private void configureOauth2(final Properties props) {
        final String jaasTemplate = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"
                + " required unsecuredLoginStringClaim_sub=\"%s\";";
        props.setProperty("security.protocol", "SASL_PLAINTEXT");
        props.setProperty("sasl.mechanism", "OAUTHBEARER");
        props.setProperty("sasl.jaas.config", String.format(jaasTemplate, "user"));
    }
}
