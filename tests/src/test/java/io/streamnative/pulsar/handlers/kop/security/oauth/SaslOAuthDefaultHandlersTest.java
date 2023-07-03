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
package io.streamnative.pulsar.handlers.kop.security.oauth;

import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.crypto.SecretKey;
import java.util.Optional;
import java.util.Properties;

import static org.mockito.Mockito.spy;

/**
 * Testing the SASL-OAUTHBEARER features on KoP with default login and validate callback handlers.
 *
 * @see org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler
 * @see org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredValidatorCallbackHandler
 */
@Slf4j
public class SaslOAuthDefaultHandlersTest extends SaslOAuthBearerTestBase {

    private static final String ADMIN_USER = "admin_user";
    private static final String USER = "user";

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
        conf.setKopOauth2ConfigFile("src/test/resources/kop-default-oauth2.properties");
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

    @Test(timeOut = 15000)
    public void testSimpleProduceConsume() throws Exception {
        super.testSimpleProduceConsume();
    }

    @Test(timeOut = 15000)
    public void testProduceWithoutAuth() throws Exception {
        super.testProduceWithoutAuth();
    }

    @Override
    protected void configureOAuth2(final Properties props) {
        final String jaasTemplate = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule"
                + " required unsecuredLoginStringClaim_sub=\"%s\";";
        props.setProperty("security.protocol", "SASL_PLAINTEXT");
        props.setProperty("sasl.mechanism", "OAUTHBEARER");
        props.setProperty("sasl.jaas.config", String.format(jaasTemplate, USER));
    }
}
