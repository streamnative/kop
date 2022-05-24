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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.security.oauth.OauthLoginCallbackHandler;
import io.streamnative.pulsar.handlers.kop.security.oauth.OauthValidatorCallbackHandler;
import io.streamnative.pulsar.handlers.kop.security.oauth.ServerConfig;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.naming.AuthenticationException;
import javax.security.auth.login.LoginException;
import lombok.Cleanup;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Testing the SASL-OAUTHBEARER features on KoP with KoP's own login callback handler and server callback handler.
 *
 * <p>The public key, issuer url, credentials url and audience are from hydra oauth server</p>
 *
 * @see OauthLoginCallbackHandler
 * @see OauthValidatorCallbackHandler
 */
public class SaslOauthKopHandlersTest extends SaslOauthBearerTestBase {

    private static final String ADMIN_USER = "simple_client_id";
    private static final String ADMIN_SECRET = "admin_secret";
    private static final String ISSUER_URL = "http://localhost:4444";
    private static final String AUDIENCE = "http://example.com/api/v2/";

    private String adminCredentialPath = null;

    @BeforeClass(timeOut = 20000)
    @Override
    protected void setup() throws Exception {
        String tokenPublicKey = HydraOAuthUtils.getPublicKeyStr();
        adminCredentialPath = HydraOAuthUtils.createOAuthClient(ADMIN_USER, ADMIN_SECRET);
        super.resetConfig();
        // Broker's config
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setAuthorizationProvider(OauthMockAuthorizationProvider.class.getName());
        conf.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationOAuth2.class.getName());
        conf.setBrokerClientAuthenticationParameters(String.format("{\"type\":\"client_credentials\","
                        + "\"privateKey\":\"%s\",\"issuerUrl\":\"%s\",\"audience\":\"%s\"}",
                adminCredentialPath, ISSUER_URL, AUDIENCE));
        final Properties properties = new Properties();
        properties.setProperty("tokenPublicKey", tokenPublicKey);
        conf.setProperties(properties);

        // KoP's config
        conf.setSaslAllowedMechanisms(Sets.newHashSet("OAUTHBEARER"));
        conf.setKopOauth2AuthenticateCallbackHandler(OauthValidatorCallbackHandler.class.getName());
        conf.setKopOauth2ConfigFile("src/test/resources/kop-handler-oauth2.properties");

        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void createAdmin() throws Exception {
        super.admin = PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrl.toString())
                .authentication(
                        AuthenticationFactoryOAuth2.clientCredentials(
                                new URL(ISSUER_URL), new URL(adminCredentialPath), AUDIENCE))
                .build();
    }

    @Test(timeOut = 15000)
    public void testSimpleProduceConsume() throws Exception {
        super.testSimpleProduceConsume();
    }

    @Test(timeOut = 15000)
    public void testGrantAndRevokePermission() throws Exception {
        OauthMockAuthorizationProvider.NULL_ROLE_STACKS.clear();
        final String namespace = conf.getKafkaTenant() + "/" + conf.getKafkaNamespace();
        final String topic = "test-grant-and-revoke-permission";
        final String role = "normal-role-" + System.currentTimeMillis();
        final String clientCredentialPath = HydraOAuthUtils.createOAuthClient(role, "secret");

        admin.namespaces().grantPermissionOnNamespace(namespace, role, Collections.singleton(AuthAction.produce));
        final Properties consumerProps = newKafkaConsumerProperties();
        internalConfigureOauth2(consumerProps, clientCredentialPath);
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(topic));
        Assert.assertThrows(TopicAuthorizationException.class, () -> consumer.poll(Duration.ofSeconds(5)));

        final Properties producerProps = newKafkaProducerProperties();
        internalConfigureOauth2(producerProps, clientCredentialPath);
        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        producer.send(new ProducerRecord<>(topic, "msg-0")).get();

        admin.namespaces().revokePermissionsOnNamespace(namespace, role);
        try {
            producer.send(new ProducerRecord<>(topic, "msg-1")).get();
            Assert.fail(role + " should not have permission to produce");
        } catch (ExecutionException e) {
            Assert.assertTrue(e.getCause() instanceof TopicAuthorizationException);
        }

        admin.namespaces().grantPermissionOnNamespace(namespace, role, Collections.singleton(AuthAction.consume));
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
        Assert.assertEquals(records.iterator().next().value(), "msg-0");

        consumer.close();
        producer.close();
        Assert.assertEquals(OauthMockAuthorizationProvider.NULL_ROLE_STACKS.size(), 0);
    }

    @Test(timeOut = 15000)
    public void testWrongSecret() throws IOException {
        final Properties producerProps = newKafkaProducerProperties();
        internalConfigureOauth2(producerProps,
                HydraOAuthUtils.writeCredentialsFile(ADMIN_USER, ADMIN_SECRET + "-wrong", "test-wrong-secret.json"));
        try {
            new KafkaProducer<>(producerProps);
        } catch (Exception e) {
            Assert.assertNotNull(e.getCause());
            Assert.assertTrue(e.getCause().getCause() instanceof LoginException);
        }
    }

    @Test
    public void testAuthenticationHasException() throws Exception {

        // Mock the AuthenticationProvider, make sure throw a AuthenticationException.
        AuthenticationProviderToken mockedAuthenticationProviderToken = Mockito.mock(AuthenticationProviderToken.class);
        doThrow(new AuthenticationException("Mock authentication exception."))
                .when(mockedAuthenticationProviderToken)
                .newAuthState(Mockito.any(AuthData.class), Mockito.isNull(), Mockito.isNull());

        AuthenticationService mockedAuthenticationServer = Mockito.mock(AuthenticationService.class);
        when(mockedAuthenticationServer.getAuthenticationProvider(Mockito.eq(
                ServerConfig.DEFAULT_OAUTH_VALIDATE_METHOD))).thenReturn(mockedAuthenticationProviderToken);
        BrokerService brokerService = Mockito.spy(pulsar.getBrokerService());
        doReturn(mockedAuthenticationServer).when(brokerService).getAuthenticationService();
        doReturn(brokerService).when(pulsar).getBrokerService();

        final String namespace = conf.getKafkaTenant() + "/" + conf.getKafkaNamespace();
        final String topic = "test-authentication-has-exception";
        final String role = "test-role-" + System.currentTimeMillis();
        final String clientCredentialPath = HydraOAuthUtils.createOAuthClient(role, "secret");

        admin.namespaces().grantPermissionOnNamespace(namespace, role, Collections.singleton(AuthAction.consume));
        final Properties consumerProps = newKafkaConsumerProperties();
        internalConfigureOauth2(consumerProps, clientCredentialPath);
        @Cleanup
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(topic));

        Assert.assertThrows(SaslAuthenticationException.class, () -> consumer.poll(Duration.ofSeconds(5)));

        Mockito.reset(brokerService);
    }

    @Test(timeOut = 15000)
    public void testProduceWithoutAuth() throws Exception {
        super.testProduceWithoutAuth();
    }

    private void internalConfigureOauth2(final Properties props, final String credentialPath) {
        props.setProperty("sasl.login.callback.handler.class", OauthLoginCallbackHandler.class.getName());
        props.setProperty("security.protocol", "SASL_PLAINTEXT");
        props.setProperty("sasl.mechanism", "OAUTHBEARER");

        final String jaasTemplate = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required"
                + " oauth.issuer.url=\"%s\""
                + " oauth.credentials.url=\"%s\""
                + " oauth.audience=\"%s\";";
        props.setProperty("sasl.jaas.config", String.format(jaasTemplate,
                ISSUER_URL,
                credentialPath,
                AUDIENCE
        ));
    }

    @Override
    protected void configureOauth2(final Properties props) {
        internalConfigureOauth2(props, adminCredentialPath);
    }

    public static class OauthMockAuthorizationProvider extends PulsarAuthorizationProvider {

        public static final List<StackTraceElement> NULL_ROLE_STACKS = Collections.synchronizedList(new ArrayList<>());

        @Override
        public CompletableFuture<Boolean> isSuperUser(String role,
                                                      AuthenticationDataSource authenticationData,
                                                      ServiceConfiguration serviceConfiguration) {
            try {
                return CompletableFuture.completedFuture(role.equals(ADMIN_USER));
            } catch (NullPointerException e) {
                NULL_ROLE_STACKS.addAll(Arrays.asList(e.getStackTrace()));
                return CompletableFuture.completedFuture(true);
            }
        }
    }
}
