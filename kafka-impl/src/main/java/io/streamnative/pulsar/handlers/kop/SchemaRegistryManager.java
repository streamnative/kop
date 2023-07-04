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

import com.google.common.annotations.VisibleForTesting;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.streamnative.pulsar.handlers.kop.schemaregistry.DummyOptionsCORSProcessor;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryChannelInitializer;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryHandler;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryRequestAuthenticator;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorageAccessor;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl.PulsarSchemaStorageAccessor;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl.SchemaStorageException;
import io.streamnative.pulsar.handlers.kop.schemaregistry.resources.CompatibilityResource;
import io.streamnative.pulsar.handlers.kop.schemaregistry.resources.ConfigResource;
import io.streamnative.pulsar.handlers.kop.schemaregistry.resources.SchemaResource;
import io.streamnative.pulsar.handlers.kop.schemaregistry.resources.SubjectResource;
import io.streamnative.pulsar.handlers.kop.security.KafkaPrincipal;
import io.streamnative.pulsar.handlers.kop.security.auth.Authorizer;
import io.streamnative.pulsar.handlers.kop.security.auth.Resource;
import io.streamnative.pulsar.handlers.kop.security.auth.ResourceType;
import io.streamnative.pulsar.handlers.kop.security.auth.SimpleAclAuthorizer;
import io.streamnative.pulsar.handlers.kop.utils.MetadataUtils;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.naming.AuthenticationException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.policies.data.ClusterData;

@Slf4j
public class SchemaRegistryManager {
    private final KafkaServiceConfiguration kafkaConfig;
    private final PulsarService pulsar;
    private final SchemaRegistryRequestAuthenticator schemaRegistryRequestAuthenticator;
    private final PulsarClient pulsarClient;
    @Getter
    @VisibleForTesting
    private SchemaStorageAccessor schemaStorage;

    public SchemaRegistryManager(KafkaServiceConfiguration kafkaConfig,
                                 PulsarService pulsar,
                                 AuthenticationService authenticationService) {
        this.kafkaConfig = kafkaConfig;
        this.pulsarClient = SystemTopicClient.createPulsarClient(pulsar, kafkaConfig, (___) -> {});
        this.pulsar = pulsar;
        Authorizer authorizer = new SimpleAclAuthorizer(pulsar, kafkaConfig);
        this.schemaRegistryRequestAuthenticator = new HttpRequestAuthenticator(this.kafkaConfig,
                authenticationService, authorizer);
    }

    @AllArgsConstructor
    private static final class UsernamePasswordPair {
        final String username;
        final String password;
    }

    @AllArgsConstructor
    public static class HttpRequestAuthenticator implements SchemaRegistryRequestAuthenticator {

        private final KafkaServiceConfiguration kafkaConfig;
        private final AuthenticationService authenticationService;
        private final Authorizer authorizer;

        @Override
        public String authenticate(FullHttpRequest request) throws SchemaStorageException {
            if (!kafkaConfig.isAuthenticationEnabled()) {
                return kafkaConfig.getKafkaMetadataTenant();
            }
            String authenticationHeader = request.headers().get(HttpHeaderNames.AUTHORIZATION, "");
            UsernamePasswordPair usernamePasswordPair = parseUsernamePassword(authenticationHeader);
            String username = usernamePasswordPair.username;
            String password = usernamePasswordPair.password;

            AuthenticationProvider authenticationProvider = authenticationService
                    .getAuthenticationProvider("token");
            if (authenticationProvider == null) {
                throw new SchemaStorageException("Pulsar is not configured for Token auth");
            }
            try {
                AuthData authData = AuthData.of(password.getBytes(StandardCharsets.UTF_8));
                final AuthenticationState authState = authenticationProvider
                        .newAuthState(authData, null, null);
                authState.authenticateAsync(authData).get(kafkaConfig.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
                final String role = authState.getAuthRole();

                final String tenant;
                if (kafkaConfig.isKafkaEnableMultiTenantMetadata()) {
                    // the tenant is the username
                    log.debug("SchemaRegistry Authenticated username {} role {} using tenant {} for data",
                            username, role, username);
                    tenant = username;
                } else {
                    // use system tenant
                    log.debug("SchemaRegistry Authenticated username {} role {} using system tenant {} for data",
                            username, role, kafkaConfig.getKafkaMetadataTenant());
                    tenant = kafkaConfig.getKafkaMetadataTenant();
                }

                performAuthorizationValidation(username, role, tenant);
                return tenant;
            } catch (ExecutionException | InterruptedException | TimeoutException | AuthenticationException err) {
                throw new SchemaStorageException(err);
            }

        }

        private void performAuthorizationValidation(String username, String role, String tenant)
                throws SchemaStorageException {
            if (kafkaConfig.isAuthorizationEnabled() && kafkaConfig.isKafkaEnableMultiTenantMetadata()) {
                KafkaPrincipal kafkaPrincipal =
                        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, role, username, null, null);
                String topicName = MetadataUtils.constructSchemaRegistryTopicName(tenant, kafkaConfig);
                try {
                    Boolean tenantExists =
                            authorizer.canAccessTenantAsync(kafkaPrincipal, Resource.of(ResourceType.TENANT, tenant))
                                    .get();
                    if (tenantExists == null || !tenantExists) {
                        log.debug("SchemaRegistry username {} role {} tenant {} does not exist",
                                username, role, tenant, topicName);
                        throw new SchemaStorageException("Role " + role + " cannot access topic " + topicName + " "
                                + "tenant " + tenant + " does not exist (wrong username?)",
                                HttpResponseStatus.FORBIDDEN);
                    }
                    Boolean hasPermission = authorizer
                            .canProduceAsync(kafkaPrincipal, Resource.of(ResourceType.TOPIC, topicName))
                            .get();
                    if (hasPermission == null || !hasPermission) {
                        log.debug("SchemaRegistry username {} role {} tenant {} cannot access topic {}",
                                username, role, tenant, topicName);
                        throw new SchemaStorageException("Role " + role + " cannot access topic " + topicName,
                                HttpResponseStatus.FORBIDDEN);
                    }
                } catch (ExecutionException err) {
                    throw new SchemaStorageException(err.getCause());
                } catch (InterruptedException err) {
                    throw new SchemaStorageException(err);
                }
            }
        }

        private UsernamePasswordPair parseUsernamePassword(String authenticationHeader)
                throws SchemaStorageException {
            if (authenticationHeader.isEmpty()) {
                // no auth
                throw new SchemaStorageException("Missing AUTHORIZATION header",
                        HttpResponseStatus.UNAUTHORIZED);
            }

            if (!authenticationHeader.startsWith("Basic ")) {
                throw new SchemaStorageException("Bad authentication scheme, only Basic is supported",
                        HttpResponseStatus.UNAUTHORIZED);
            }
            String strippedAuthenticationHeader = authenticationHeader.substring("Basic ".length());

            String usernamePassword = new String(Base64.getDecoder()
                    .decode(strippedAuthenticationHeader), StandardCharsets.UTF_8);
            int colon = usernamePassword.indexOf(":");
            if (colon <= 0) {
                throw new SchemaStorageException("Bad authentication header", HttpResponseStatus.BAD_REQUEST);
            }
            String rawUsername = usernamePassword.substring(0, colon);
            String rawPassword = usernamePassword.substring(colon + 1);
            if (rawPassword.startsWith("token:")) {
                return new UsernamePasswordPair(rawUsername, rawPassword.substring("token:".length()));
            } else {
                return new UsernamePasswordPair(rawUsername, rawPassword);
            }
        }
    }

    public InetSocketAddress getAddress() {
        return new InetSocketAddress(kafkaConfig.getKopSchemaRegistryPort());
    }

    public Optional<SchemaRegistryChannelInitializer> build() throws Exception {
        if (!kafkaConfig.isKopSchemaRegistryEnable()) {
            return Optional.empty();
        }
        PulsarAdmin pulsarAdmin = pulsar.getAdminClient();
        SchemaRegistryHandler handler = new SchemaRegistryHandler();
        schemaStorage = new PulsarSchemaStorageAccessor((tenant) -> {
            try {
                BrokerService brokerService = pulsar.getBrokerService();
                final ClusterData clusterData = ClusterData.builder()
                        .serviceUrl(brokerService.getPulsar().getWebServiceAddress())
                        .serviceUrlTls(brokerService.getPulsar().getWebServiceAddressTls())
                        .brokerServiceUrl(brokerService.getPulsar().getBrokerServiceUrl())
                        .brokerServiceUrlTls(brokerService.getPulsar().getBrokerServiceUrlTls())
                        .build();
                MetadataUtils.createSchemaRegistryMetadataIfMissing(tenant,
                        pulsarAdmin,
                        clusterData,
                        kafkaConfig);
                return pulsarClient;
            } catch (Exception err) {
                throw new IllegalStateException(err);
            }
        },
                kafkaConfig.getKopSchemaRegistryNamespace(), kafkaConfig.getKopSchemaRegistryTopicName());
        new SchemaResource(schemaStorage, schemaRegistryRequestAuthenticator).register(handler);
        new SubjectResource(schemaStorage, schemaRegistryRequestAuthenticator).register(handler);
        new ConfigResource(schemaStorage, schemaRegistryRequestAuthenticator).register(handler);
        new CompatibilityResource(schemaStorage, schemaRegistryRequestAuthenticator).register(handler);
        handler.addProcessor(new DummyOptionsCORSProcessor());

        return Optional.of(new SchemaRegistryChannelInitializer(handler));
    }

    public void close() {
        try {
            pulsarClient.close();
        } catch (PulsarClientException err) {
            log.error("Error while shutting down", err);
        }
    }
}
