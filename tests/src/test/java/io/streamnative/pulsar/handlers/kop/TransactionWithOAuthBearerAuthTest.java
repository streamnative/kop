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
import java.util.Collections;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import sh.ory.hydra.ApiException;
import sh.ory.hydra.api.AdminApi;
import sh.ory.hydra.model.OAuth2Client;

@Slf4j
public class TransactionWithOAuthBearerAuthTest extends TransactionTest {

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

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        hydraAdmin.setCustomBaseUrl("http://localhost:4445");
        adminCredentialPath = createOAuthClient(ADMIN_USER, ADMIN_SECRET);

        super.resetConfig();
        conf.setKafkaTransactionCoordinatorEnabled(true);
        conf.setBrokerDeduplicationEnabled(true);
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setAuthorizationProvider(SaslOauthKopHandlersTest.OauthMockAuthorizationProvider.class.getName());
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
        log.info("success internal setup");
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.cleanup();
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
                .audience(Collections.singletonList(AUDIENCE))
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

    @Override
    protected void addCustomizeProps(Properties properties) {
        properties.setProperty("sasl.login.callback.handler.class", OauthLoginCallbackHandler.class.getName());
        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
        properties.setProperty("sasl.mechanism", "OAUTHBEARER");

        final String jaasTemplate = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required"
                + " oauth.issuer.url=\"%s\""
                + " oauth.credentials.url=\"%s\""
                + " oauth.audience=\"%s\";";
        properties.setProperty("sasl.jaas.config", String.format(jaasTemplate,
                ISSUER_URL,
                adminCredentialPath,
                AUDIENCE
        ));
    }

}
