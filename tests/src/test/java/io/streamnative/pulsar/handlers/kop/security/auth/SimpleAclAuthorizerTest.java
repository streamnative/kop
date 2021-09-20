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
package io.streamnative.pulsar.handlers.kop.security.auth;


import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import io.streamnative.pulsar.handlers.kop.security.KafkaPrincipal;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import javax.crypto.SecretKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test SimpleAclAuthorizer.
 */
@Slf4j
public class SimpleAclAuthorizerTest extends KopProtocolHandlerTestBase {

    private SimpleAclAuthorizer simpleAclAuthorizer;

    private static final String SIMPLE_USER = "muggle_user";
    private static final String PRODUCE_USER = "produce_user";
    private static final String CONSUMER_USER = "consumer_user";
    private static final String ANOTHER_USER = "death_eater_user";
    private static final String ADMIN_USER = "admin_user";
    private static final String TENANT_ADMIN_USER = "tenant_admin_user";
    private static final String TOPIC_LEVEL_PERMISSIONS_USER = "topic_level_permission_user";

    private static final String TENANT = "SimpleAcl";
    private static final String NAMESPACE = "ns1";
    private static final String TOPIC = "persistent://" + TENANT + "/" + NAMESPACE + "/topic1";
    private static final String NOT_EXISTS_TENANT_TOPIC = "persistent://not_exists/" + NAMESPACE + "/topic1";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        log.info("success internal setup");
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));
        ServiceConfiguration authConf = new ServiceConfiguration();
        authConf.setProperties(properties);
        provider.initialize(authConf);

        String adminToken = AuthTokenUtils.createToken(secretKey, ADMIN_USER, Optional.empty());

        super.resetConfig();
        conf.setKopAllowedNamespaces(Collections.singleton(TENANT + "/" + NAMESPACE));
        conf.setSaslAllowedMechanisms(Sets.newHashSet("PLAIN"));
        conf.setKafkaMetadataTenant("internal");
        conf.setKafkaMetadataNamespace("__kafka");

        conf.setClusterName(super.configClusterName);
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationAllowWildcardsMatching(true);
        conf.setSuperUserRoles(Sets.newHashSet(ADMIN_USER));
        conf.setAuthenticationProviders(
                Sets.newHashSet("org.apache.pulsar.broker.authentication."
                        + "AuthenticationProviderToken"));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters("token:" + adminToken);
        conf.setProperties(properties);

        super.internalSetup();

        admin.tenants().createTenant(TENANT,
                TenantInfo.builder()
                        .adminRoles(Collections.singleton(TENANT_ADMIN_USER))
                        .allowedClusters(Collections.singleton(configClusterName))
                        .build());
        admin.namespaces().createNamespace(TENANT + "/" + NAMESPACE);
        admin.topics().createPartitionedTopic(TOPIC, 1);
        admin.namespaces().grantPermissionOnNamespace(TENANT + "/" + NAMESPACE, SIMPLE_USER,
                Sets.newHashSet(AuthAction.consume, AuthAction.produce));
        admin.namespaces().grantPermissionOnNamespace(TENANT + "/" + NAMESPACE, PRODUCE_USER,
                Sets.newHashSet(AuthAction.produce));
        admin.namespaces().grantPermissionOnNamespace(TENANT + "/" + NAMESPACE, CONSUMER_USER,
                Sets.newHashSet(AuthAction.consume));

        simpleAclAuthorizer = new SimpleAclAuthorizer(pulsar);
    }

    @Override
    protected void createAdmin() throws Exception {
        super.admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(this.conf.getBrokerClientAuthenticationPlugin(),
                        this.conf.getBrokerClientAuthenticationParameters()).build());
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testAuthorizeProduce() throws ExecutionException, InterruptedException {
        Boolean isAuthorized = simpleAclAuthorizer.canProduceAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, SIMPLE_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertTrue(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canProduceAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, PRODUCE_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertTrue(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canProduceAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, CONSUMER_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertFalse(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canProduceAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, ANOTHER_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertFalse(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canProduceAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, SIMPLE_USER, null),
                Resource.of(ResourceType.TOPIC, NOT_EXISTS_TENANT_TOPIC)).get();
        assertFalse(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canProduceAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, ADMIN_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertTrue(isAuthorized);
    }

    @Test
    public void testAuthorizeConsume() throws ExecutionException, InterruptedException {
        Boolean isAuthorized = simpleAclAuthorizer.canConsumeAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, SIMPLE_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertTrue(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canConsumeAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, PRODUCE_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertFalse(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canConsumeAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, CONSUMER_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertTrue(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canConsumeAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, ANOTHER_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertFalse(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canConsumeAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, SIMPLE_USER, null),
                Resource.of(ResourceType.TOPIC, NOT_EXISTS_TENANT_TOPIC)).get();
        assertFalse(isAuthorized);
    }

    @Test
    public void testAuthorizeLookup() throws ExecutionException, InterruptedException {
        Boolean isAuthorized = simpleAclAuthorizer.canLookupAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, SIMPLE_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertTrue(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canLookupAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, PRODUCE_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertTrue(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canLookupAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, CONSUMER_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertTrue(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canLookupAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, ANOTHER_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertFalse(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canLookupAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, SIMPLE_USER, null),
                Resource.of(ResourceType.TOPIC, NOT_EXISTS_TENANT_TOPIC)).get();
        assertFalse(isAuthorized);
    }

    @Test
    public void testAuthorizeTenantAdmin() throws ExecutionException, InterruptedException {

        // TENANT_ADMIN_USER can't produce don't exist tenant's topic,
        // because tenant admin depend on exist tenant.
        Boolean isAuthorized = simpleAclAuthorizer.canProduceAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, TENANT_ADMIN_USER, null),
                Resource.of(ResourceType.TOPIC, NOT_EXISTS_TENANT_TOPIC)).get();
        assertFalse(isAuthorized);

        // ADMIN_USER can produce don't exist tenant's topic, because is superuser.
        isAuthorized = simpleAclAuthorizer.canProduceAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, ADMIN_USER, null),
                Resource.of(ResourceType.TOPIC, NOT_EXISTS_TENANT_TOPIC)).get();
        assertTrue(isAuthorized);

        // TENANT_ADMIN_USER can produce.
        isAuthorized = simpleAclAuthorizer.canProduceAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, TENANT_ADMIN_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertTrue(isAuthorized);

    }

    @Test
    public void testTopicLevelPermissions() throws PulsarAdminException, ExecutionException, InterruptedException {
        String topic = "persistent://" + TENANT + "/" + NAMESPACE + "/topic_level_permission_test_topic";
        admin.topics().createPartitionedTopic(topic, 1);
        admin.topics().grantPermission(topic, TOPIC_LEVEL_PERMISSIONS_USER, Sets.newHashSet(AuthAction.produce));

        Boolean isAuthorized = simpleAclAuthorizer.canLookupAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, TOPIC_LEVEL_PERMISSIONS_USER, null),
                Resource.of(ResourceType.TOPIC, topic)).get();
        assertTrue(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canProduceAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, TOPIC_LEVEL_PERMISSIONS_USER, null),
                Resource.of(ResourceType.TOPIC, topic)).get();
        assertTrue(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canConsumeAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, TOPIC_LEVEL_PERMISSIONS_USER, null),
                Resource.of(ResourceType.TOPIC, topic)).get();
        assertFalse(isAuthorized);

        isAuthorized = simpleAclAuthorizer.canProduceAsync(
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, TOPIC_LEVEL_PERMISSIONS_USER, null),
                Resource.of(ResourceType.TOPIC, TOPIC)).get();
        assertFalse(isAuthorized);
    }
}