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
import io.streamnative.pulsar.handlers.kop.security.oauth.KopOAuthBearerToken;
import io.streamnative.pulsar.handlers.kop.security.oauth.KopOAuthBearerValidatorCallback;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.SecretKey;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import lombok.Cleanup;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredJws;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test custom AuthenticateCallbackHandler for OAUTHBEARER.
 */
public class CustomOAuthBearerCallbackHandlerTest extends KopProtocolHandlerTestBase {

    private static final String ADMIN_USER = "admin_user";
    private static final String USER = "user";

    public CustomOAuthBearerCallbackHandlerTest() {
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
        conf.setKopOauth2AuthenticateCallbackHandler(MockedAuthenticateCallbackHandler.class.getName());

        super.internalSetup();
        admin.namespaces().grantPermissionOnNamespace(
                conf.getKafkaTenant() + "/" + conf.getKafkaNamespace(),
                USER,
                Sets.newHashSet(AuthAction.consume, AuthAction.produce)
        );
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

    @Test(timeOut = 10000)
    public void testNumAuthenticateSuccess() throws Exception {
        final String topic = "testNumAuthenticateSuccess";

        final Properties props = newKafkaProducerProperties();
        final String jaasTemplate = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"
                + " required unsecuredLoginStringClaim_sub=\"%s\";";
        props.setProperty("security.protocol", "SASL_PLAINTEXT");
        props.setProperty("sasl.mechanism", "OAUTHBEARER");
        props.setProperty("sasl.jaas.config", String.format(jaasTemplate, USER));

        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        assertTrue(MockedAuthenticateCallbackHandler.getAuthenticatedUsers().isEmpty());
        producer.send(new ProducerRecord<>(topic, "hello")).get();
        assertEquals(MockedAuthenticateCallbackHandler.getAuthenticatedUsers(), Sets.newHashSet(USER));
    }

    /**
     * Mocked AuthenticateCallbackHandler that is used only for test.
     */
    public static class MockedAuthenticateCallbackHandler implements AuthenticateCallbackHandler {

        private static final Set<String> authenticatedUsers = ConcurrentHashMap.newKeySet();

        public static Set<String> getAuthenticatedUsers() {
            return new HashSet<>(authenticatedUsers);
        }

        @Override
        public void configure(Map<String, ?> configs,
                              String saslMechanism,
                              List<AppConfigurationEntry> jaasConfigEntries) {
        }

        @Override
        public void close() {
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof KopOAuthBearerValidatorCallback) {
                    KopOAuthBearerValidatorCallback validationCallback = (KopOAuthBearerValidatorCallback) callback;
                    OAuthBearerUnsecuredJws oAuthBearerUnsecuredJws = new OAuthBearerUnsecuredJws(
                            validationCallback.tokenValue(), "sub", "scope");
                    KopOAuthBearerToken token = new KopOAuthBearerToken() {
                        @Override
                        public String value() {
                            return validationCallback.tokenValue();
                        }

                        @Override
                        public Set<String> scope() {
                            return null;
                        }

                        @Override
                        public long lifetimeMs() {
                            return 0;
                        }

                        @Override
                        public String principalName() {
                            return oAuthBearerUnsecuredJws.principalName();
                        }

                        @Override
                        public AuthenticationDataSource authDataSource() {
                            return new AuthenticationDataCommand(validationCallback.tokenValue());
                        }

                        @Override
                        public String tenant() {
                            return null;
                        }

                        @Override
                        public Long startTimeMs() {
                            return null;
                        }
                    };
                    validationCallback.token(token);
                    authenticatedUsers.add(token.principalName());
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }
    }
}


