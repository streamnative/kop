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

import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import io.streamnative.pulsar.handlers.kop.security.oauthbearer.OauthLoginCallbackHandler;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import javax.crypto.SecretKey;
import lombok.Cleanup;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Testing the SASL-OAUTHBEARER features on KoP with KoP's own login callback handler and server callback handler.
 */
public class SaslOauthKopHandlerTest extends KopProtocolHandlerTestBase {

    private static final String ADMIN_USER = "admin_user";

    public SaslOauthKopHandlerTest() {
        super("kafka");
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.resetConfig();
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));

        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        final SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
        conf.setBrokerClientAuthenticationParameters(
                "token:" + AuthTokenUtils.createToken(secretKey, ADMIN_USER, Optional.empty()));
        conf.setSuperUserRoles(Sets.newHashSet(ADMIN_USER));
        final Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));
        conf.setProperties(properties);

        conf.setSaslAllowedMechanisms(Sets.newHashSet("OAUTHBEARER"));

        // TODO: Use the real callback handler.

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

    @Test
    public void testSimpleProduceConsume() throws Exception {
        final String topic = "test";
        final Properties producerProps = newKafkaProducerProperties();
        producerProps.setProperty("sasl.login.callback.handler.class", OauthLoginCallbackHandler.class.getName());
        producerProps.setProperty("security.protocol", "SASL_PLAINTEXT");
        producerProps.setProperty("sasl.mechanism", "OAUTHBEARER");

        final String jassTemplate = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required"
                + " oauth.issuer.url=\"%s\""
                + " oauth.credentials.url=\"%s\""
                + " oauth.audience=\"%s\";";
        producerProps.setProperty("sasl.jaas.config", String.format(jassTemplate,
                "https://dev-kt-aa9ne.us.auth0.com",
                "file://" + Paths.get("./src/test/resources/credentials_file.json").toAbsolutePath(),
                "https://dev-kt-aa9ne.us.auth0.com/api/v2/"
        ));

        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        producer.send(new ProducerRecord<>(topic, "", "hello")).get();
        producer.close();
    }
}
