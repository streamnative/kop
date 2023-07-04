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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import sh.ory.hydra.ApiException;

@Slf4j
public class SaslOAuthKopHandlersWithGroupIdTest extends SaslOAuthBearerTestBase {

    private static final String ADMIN_USER = "simple_client_id";
    private static final String ADMIN_SECRET = "admin_secret";
    private static final String ISSUER_URL = "http://localhost:4444";
    private static final String AUDIENCE = "http://example.com/api/v2/";

    private String adminCredentialPath = null;

    private String tenant = "my-tenant";

    @BeforeClass(timeOut = 20000)
    @Override
    protected void setup() throws Exception {
        String tokenPublicKey = HydraOAuthUtils.getPublicKeyStr();
        adminCredentialPath = HydraOAuthUtils.createOAuthClient(ADMIN_USER, ADMIN_SECRET);
        super.resetConfig();
        // Broker's config
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setKafkaEnableMultiTenantMetadata(true);
        conf.setAuthorizationProvider(SaslOAuthKopHandlersTest.OAuthMockAuthorizationProvider.class.getName());
        conf.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationOAuth2.class.getName());
        conf.setBrokerClientAuthenticationParameters(String.format("{\"type\":\"client_credentials\","
                        + "\"privateKey\":\"%s\",\"issuerUrl\":\"%s\",\"audience\":\"%s\"}",
                adminCredentialPath, ISSUER_URL, AUDIENCE));
        conf.setKafkaEnableAuthorizationForceGroupIdCheck(true);
        final Properties properties = new Properties();
        properties.setProperty("tokenPublicKey", tokenPublicKey);
        conf.setProperties(properties);

        // KoP's config
        conf.setSaslAllowedMechanisms(Sets.newHashSet("OAUTHBEARER"));
        conf.setKopOauth2AuthenticateCallbackHandler(OauthValidatorCallbackHandler.class.getName());
        conf.setKopOauth2ConfigFile("src/test/resources/kop-handler-oauth2.properties");

        super.internalSetup();

        admin.tenants().createTenant(tenant,
                TenantInfo.builder()
                        .adminRoles(Collections.singleton(ADMIN_USER))
                        .allowedClusters(Collections.singleton(configClusterName))
                        .build());
        TenantInfo tenantInfo = admin.tenants().getTenantInfo(tenant);
        log.info("TenantInfo for {} {} in test", tenant, tenantInfo);
        assertNotNull(tenantInfo);
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

    @Test(timeOut = 30000)
    public void testGrantAndRevokePermissionWithGroupId() throws Exception {
        SaslOAuthKopHandlersTest.OAuthMockAuthorizationProvider.NULL_ROLE_STACKS.clear();

        final String namespace = tenant + "/" + "test-grant-and-revoke-permission-with-group-id-ns";
        admin.namespaces().createNamespace(namespace);
        final String topic = "persistent://" + namespace + "/test-grant-and-revoke-permission-with-group-id";
        final String role = "normal-role-" + System.currentTimeMillis();
        final String clientCredentialPath = HydraOAuthUtils.createOAuthClient(role, "secret", tenant);

        admin.namespaces().grantPermissionOnNamespace(namespace, role, Collections.singleton(AuthAction.produce));

        final Properties consumerProps = newKafkaConsumerProperties();
        internalConfigureOAuth2(consumerProps, clientCredentialPath);
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(topic));

        admin.namespaces().grantPermissionOnNamespace(namespace, role, Sets.newHashSet(AuthAction.consume));
        // Only have consume permission, can't consume without subscription permission.
        Assert.assertThrows(GroupAuthorizationException.class, () -> consumer.poll(Duration.ofSeconds(5)));

        consumer.close();

        // Pass subscription permission, can consume now.
        final String roleWithGroupId = "role-with-groupId" + System.currentTimeMillis();
        final String clientCredentialPathWithGroupId =
                HydraOAuthUtils.createOAuthClient(roleWithGroupId, "secret", tenant, DEFAULT_GROUP_ID);
        final Properties consumerPropsWithGroupId = newKafkaConsumerProperties();
        internalConfigureOAuth2(consumerPropsWithGroupId, clientCredentialPathWithGroupId);
        final KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(consumerPropsWithGroupId);
        consumer2.subscribe(Collections.singleton(topic));

        admin.namespaces().grantPermissionOnNamespace(namespace, roleWithGroupId, Sets.newHashSet(AuthAction.consume));
        admin.namespaces().grantPermissionOnSubscription(namespace, DEFAULT_GROUP_ID, Sets.newHashSet(roleWithGroupId));

        consumer2.poll(Duration.ofSeconds(5));

        assertEquals(SaslOAuthKopHandlersTest.OAuthMockAuthorizationProvider.NULL_ROLE_STACKS.size(), 0);
    }

    @Test(timeOut = 30000, expectedExceptions = org.apache.kafka.common.errors.GroupAuthorizationException.class)
    public void testDifferentGroupId() throws PulsarAdminException, IOException, ApiException {
        SaslOAuthKopHandlersTest.OAuthMockAuthorizationProvider.NULL_ROLE_STACKS.clear();

        final String namespace = tenant + "/" + "test-different-group-id-ns";
        admin.namespaces().createNamespace(namespace);
        final String topic = "persistent://" + namespace + "/test-grant-and-revoke-permission";
        // Pass subscription permission, can consume now.
        final String roleWithGroupId = "role-with-groupId" + System.currentTimeMillis();
        final String clientCredentialPathWithGroupId =
                HydraOAuthUtils.createOAuthClient(roleWithGroupId, "secret", tenant, DEFAULT_GROUP_ID);
        admin.namespaces().grantPermissionOnNamespace(namespace, roleWithGroupId, Sets.newHashSet(AuthAction.consume));
        admin.namespaces().grantPermissionOnSubscription(namespace, DEFAULT_GROUP_ID, Sets.newHashSet(roleWithGroupId));

        final Properties consumerPropsWithGroupId = newKafkaConsumerProperties();
        internalConfigureOAuth2(consumerPropsWithGroupId, clientCredentialPathWithGroupId);
        consumerPropsWithGroupId.put(ConsumerConfig.GROUP_ID_CONFIG, "different-group-id");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerPropsWithGroupId);
        consumer.subscribe(Collections.singleton(topic));

        consumer.poll(Duration.ofSeconds(5));

        assertEquals(SaslOAuthKopHandlersTest.OAuthMockAuthorizationProvider.NULL_ROLE_STACKS.size(), 0);
    }

    private void internalConfigureOAuth2(final Properties props, final String credentialPath,
                                         Class<? extends AuthenticateCallbackHandler> callbackHandler) {
        props.setProperty("sasl.login.callback.handler.class", callbackHandler.getName());
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

    private void internalConfigureOAuth2(final Properties props, final String credentialPath) {
        internalConfigureOAuth2(props, credentialPath, OauthLoginCallbackHandler.class);
    }

    @Override
    protected void configureOAuth2(final Properties props) {
        internalConfigureOAuth2(props, adminCredentialPath);
    }

}
