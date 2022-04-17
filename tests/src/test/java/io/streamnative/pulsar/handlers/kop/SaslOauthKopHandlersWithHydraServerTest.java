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
import io.streamnative.pulsar.handlers.kop.security.auth.KafkaMockAuthorizationProvider;
import io.streamnative.pulsar.handlers.kop.security.oauth.OauthLoginCallbackHandler;
import io.streamnative.pulsar.handlers.kop.security.oauth.OauthValidatorCallbackHandler;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Properties;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
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
public class SaslOauthKopHandlersWithHydraServerTest extends SaslOauthBearerTestBase {

    private static final String ADMIN_USER = "simple_client_id";
    private static final String ISSUER_URL = "http://localhost:4444";
    private static final String CREDENTIALS_URL = "file://"
            + Paths.get("./src/test/resources/oauth_credentials/simple_credentials_file.json").toAbsolutePath();
    private static final String AUDIENCE = "http://example.com/api/v2/";

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

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.resetConfig();
        // Broker's config
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setAuthorizationProvider(OauthMockAuthorizationProvider.class.getName());
        conf.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationOAuth2.class.getName());
        conf.setBrokerClientAuthenticationParameters(String.format("{\"type\":\"client_credentials\","
                        + "\"privateKey\":\"%s\",\"issuerUrl\":\"%s\",\"audience\":\"%s\"}",
                CREDENTIALS_URL, ISSUER_URL, AUDIENCE));
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
                                new URL(ISSUER_URL), new URL(CREDENTIALS_URL), AUDIENCE))
                .build();
    }

    @Test(timeOut = 15000)
    public void testSimpleProduceConsume() throws Exception {
        super.testSimpleProduceConsume();
    }

    @Test(timeOut = 15000)
    public void testProduceWithoutAuth() throws Exception {
        super.testProduceWithoutAuth();
    }

    @Override
    protected void configureOauth2(final Properties props) {
        props.setProperty("sasl.login.callback.handler.class", OauthLoginCallbackHandler.class.getName());
        props.setProperty("security.protocol", "SASL_PLAINTEXT");
        props.setProperty("sasl.mechanism", "OAUTHBEARER");

        final String jassTemplate = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required"
                + " oauth.issuer.url=\"%s\""
                + " oauth.credentials.url=\"%s\""
                + " oauth.audience=\"%s\";";
        props.setProperty("sasl.jaas.config", String.format(jassTemplate,
                ISSUER_URL,
                CREDENTIALS_URL,
                AUDIENCE
        ));
    }

    public static class OauthMockAuthorizationProvider extends KafkaMockAuthorizationProvider {

        @Override
        public boolean roleAuthorized(String role) {
            return role.equals(ADMIN_USER);
        }
    }
}
