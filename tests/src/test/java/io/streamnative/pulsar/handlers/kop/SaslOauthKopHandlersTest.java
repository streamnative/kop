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
import io.streamnative.pulsar.handlers.kop.security.OauthLoginCallbackHandler;
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
 * <p>The public key, issuer url, credentials url and audience are from an example in https://auth0.com</p>
 *
 * @see OauthLoginCallbackHandler
 * @see OauthValidatorCallbackHandler
 */
public class SaslOauthKopHandlersTest extends SaslOauthBearerTestBase {

    private static final String ADMIN_USER = "Xd23RHsUnvUlP7wchjNYOaIfazgeHd9x@clients";
    private static final String ISSUER_URL = "https://dev-kt-aa9ne.us.auth0.com";
    private static final String CREDENTIALS_URL = "file://"
            + Paths.get("./src/test/resources/credentials_file.json").toAbsolutePath();
    private static final String AUDIENCE = "https://dev-kt-aa9ne.us.auth0.com/api/v2/";

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.resetConfig();
        // Broker's config
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        conf.setSuperUserRoles(Sets.newHashSet(ADMIN_USER));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationOAuth2.class.getName());
        conf.setBrokerClientAuthenticationParameters(String.format("{\"type\":\"client_credentials\","
                + "\"privateKey\":\"%s\",\"issuerUrl\":\"%s\",\"audience\":\"%s\"}",
                CREDENTIALS_URL, ISSUER_URL, AUDIENCE));
        final Properties properties = new Properties();
        properties.setProperty("tokenPublicKey", "data:;base64,MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2tZd/4gJda3U"
                + "2Pc3tpgRAN7JPGWx/Gn17v/0IiZlNNRbP/Mmf0Vc6G1qsnaRaWNWOR+t6/a6ekFHJMikQ1N2X6yfz4UjMc8/G2FDPRmWjA+GURzA"
                + "RjVhxc/BBEYGoD0Kwvbq/u9CZm2QjlKrYaLfg3AeB09j0btNrDJ8rBsNzU6AuzChRvXj9IdcE/A/4N/UQ+S9cJ4UXP6NJbToLwaj"
                + "Q5km+CnxdGE6nfB7LWHvOFHjn9C2Rb9e37CFlmeKmIVFkagFM0gbmGOb6bnGI8Bp/VNGV0APef4YaBvBTqwoZ1Z4aDHy5eRxXfAM"
                + "dtBkBupmBXqL6bpd15XRYUbu/7ck9QIDAQAB");
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
                "https://dev-kt-aa9ne.us.auth0.com",
                "file://" + Paths.get("./src/test/resources/credentials_file.json").toAbsolutePath(),
                "https://dev-kt-aa9ne.us.auth0.com/api/v2/"
        ));
    }
}
