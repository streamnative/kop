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

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.security.oauth.OauthLoginCallbackHandler;
import io.streamnative.pulsar.handlers.kop.security.oauth.OauthValidatorCallbackHandler;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.security.auth.login.LoginException;
import lombok.Cleanup;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authorization.PulsarAuthorizationProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import sh.ory.hydra.ApiException;
import sh.ory.hydra.api.AdminApi;
import sh.ory.hydra.model.OAuth2Client;

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

    // Generated from ci/hydra/keys/public_key.json
    private static final String TOKEN_PUBLIC_KEY = "data:;base64,MIICIjANBgkqhk"
            + "iG9w0BAQEFAAOCAg8AMIICCgKCAgEA4g8rgGslfLNGdfh94Kbf"
            + "sMPjgX17nnEHnCLhrlVyA+jxSThiQyQVQCkZfav9k4cLCiKdoqxKtLV0RA3hWXGH"
            + "E0qUNUJWVN3vz3NOI7ccEHBJHzbDk24NYxsW7M6zNfBfTc6ZrJr5XENy7emscODn"
            + "8HJ2Qf1UkMUeze5EirJ2lsB9Zzo1GIw9ZU65W9HWWcgS5sL9eHlDRbVLmgph7jRz"
            + "kQJGm2hOeyiE+ufUOWkBQH49BhKaNGfjZ8BOJ1WRsbIIVtwhS7m+HSIKmglboG+o"
            + "nNd5LYAmngbkCuhwjJajBQayxkeBeumvRQACC1+mKC5KaW40JmVRKFFHDcf892t6"
            + "GX6c7PaVWPqvf2l6nYRbYT9nl4fQK1aUTiCqrPf2+WjEH1JIEwTfFZKTwpTtlr3e"
            + "jGJMT7wH2L4uFbpguKawTo4lYHWN3IsryDfUVvNbb7l8KMqiuDIy+5R6WezajsCY"
            + "I/GzvLGCYO1EnRTDFdEmipfbNT2/D91OPKNGmZLVUkVVlL0z+1iQtwfRamn2oRNH"
            + "zMYMAplGikxrQld/IPUIbKjgtLWPDnfskoWvuCIDQdRzMpxAXa3O/cq5uQRpu2o8"
            + "xZ8RYWixxrIGc1/8m+QQLy7DwcmVd0dGU29S+fnfOzWr43KWlyWfGsBLFxUkltjY"
            + "6gx6oB6tsQVC3Cy5Eku8FdcCAwEAAQ==";

    private final AdminApi hydraAdmin = new AdminApi();
    private String adminCredentialPath = null;

    @BeforeClass(timeOut = 20000)
    @Override
    protected void setup() throws Exception {
        hydraAdmin.setCustomBaseUrl("http://localhost:4445");
        adminCredentialPath = createOAuthClient(ADMIN_USER, ADMIN_SECRET);

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
        properties.setProperty("tokenPublicKey", TOKEN_PUBLIC_KEY);
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

    private String createOAuthClient(String clientId, String clientSecret) throws ApiException, IOException {
        final OAuth2Client oAuth2Client = new OAuth2Client()
                .audience(Collections.singletonList(SaslOauthKopHandlersTest.AUDIENCE))
                .clientId(clientId)
                .clientSecret(clientSecret)
                .grantTypes(Collections.singletonList("client_credentials"))
                .responseTypes(Collections.singletonList("code"))
                .tokenEndpointAuthMethod("client_secret_post");
        try {
            hydraAdmin.createOAuth2Client(oAuth2Client);
        } catch (ApiException e) {
            if (e.getCode() != 409) {
                throw e;
            }
        }
        return writeCredentialsFile(clientId, clientSecret, clientId + ".json");
    }

    private String writeCredentialsFile(String clientId, String clientSecret, String basename) throws IOException {
        final String content = "{\n"
                + "    \"client_id\": \"" + clientId + "\",\n"
                + "    \"client_secret\": \"" + clientSecret + "\"\n"
                + "}\n";

        File file = new File(SaslOauthKopHandlersTest.class.getResource("/").getFile() + "/" + basename);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(content);
        }
        return "file://" + file.getAbsolutePath();
    }

    @Test(timeOut = 15000)
    public void testSimpleProduceConsume() throws Exception {
        super.testSimpleProduceConsume();
    }

    @Test(timeOut = 15000)
    public void testGrantAndRevokePermission() throws Exception {
        final String namespace = conf.getKafkaTenant() + "/" + conf.getKafkaNamespace();
        final String topic = "test-grant-and-revoke-permission";
        final String role = "normal-role-" + System.currentTimeMillis();
        final String clientCredentialPath = createOAuthClient(role, "secret");

        admin.namespaces().grantPermissionOnNamespace(namespace, role, Collections.singleton(AuthAction.produce));
        final Properties consumerProps = newKafkaConsumerProperties();
        internalConfigureOauth2(consumerProps, clientCredentialPath);
        @Cleanup
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(topic));
        Assert.assertThrows(TopicAuthorizationException.class, () -> consumer.poll(Duration.ofSeconds(5)));

        final Properties producerProps = newKafkaProducerProperties();
        internalConfigureOauth2(producerProps, clientCredentialPath);
        @Cleanup
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
    }

    @Test(timeOut = 15000)
    public void testWrongSecret() throws IOException {
        final Properties producerProps = newKafkaProducerProperties();
        internalConfigureOauth2(producerProps,
                writeCredentialsFile(ADMIN_USER, ADMIN_SECRET + "-wrong", "test-wrong-secret.json"));
        try {
            new KafkaProducer<>(producerProps);
        } catch (Exception e) {
            Assert.assertNotNull(e.getCause());
            Assert.assertTrue(e.getCause().getCause() instanceof LoginException);
        }
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

        @Override
        public CompletableFuture<Boolean> isSuperUser(String role,
                                                      AuthenticationDataSource authenticationData,
                                                      ServiceConfiguration serviceConfiguration) {
            return CompletableFuture.completedFuture(role.equals(ADMIN_USER));
        }
    }
}
