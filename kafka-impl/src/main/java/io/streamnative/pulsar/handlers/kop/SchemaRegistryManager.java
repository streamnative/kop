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

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryChannelInitializer;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryHandler;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryRequestAuthenticator;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorageAccessor;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl.PulsarSchemaStorageAccessor;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl.SchemaStorageException;
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
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.policies.data.ClusterData;

@Slf4j
public class SchemaRegistryManager {
    private final KafkaServiceConfiguration kafkaConfig;
    private final PulsarService pulsar;
    private final AuthenticationService authenticationService;
    private final SchemaRegistryRequestAuthenticator schemaRegistryRequestAuthenticator;
    private final Authorizer authorizer;

    public SchemaRegistryManager(KafkaServiceConfiguration kafkaConfig,
                                 PulsarService pulsar,
                                 AuthenticationService authenticationService) {
        this.kafkaConfig = kafkaConfig;
        this.pulsar = pulsar;
        this.authenticationService = authenticationService;
        this.schemaRegistryRequestAuthenticator = new HttpRequestAuthenticator();
        this.authorizer = new SimpleAclAuthorizer(pulsar);
    }

    @AllArgsConstructor
    private static final class UsernamePasswordPair {
        final String username;
        final String password;
    }

    private class HttpRequestAuthenticator implements SchemaRegistryRequestAuthenticator {
        @Override
        public String authenticate(FullHttpRequest request) throws Exception {
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
            final AuthenticationState authState = authenticationProvider
                    .newAuthState(AuthData.of(password.getBytes(StandardCharsets.UTF_8)), null, null);
            final String role = authState.getAuthRole();

            final String tenant;
            if (kafkaConfig.isKafkaEnableMultiTenantMetadata()) {
                // the tenant is the username
                log.debug("SchemaRegistry Authenticated username {} role {} using tenant {} for data",
                        username, role, username);
                tenant =  username;
            } else {
                // use system tenant
                log.debug("SchemaRegistry Authenticated username {} role {} using system tenant {} for data",
                        username, role, kafkaConfig.getKafkaMetadataTenant());
                tenant =  kafkaConfig.getKafkaMetadataTenant();
            }

            performAuthorizationValidation(username, role, tenant);

            return tenant;
        }

        private void performAuthorizationValidation(String username, String role, String tenant)
                throws InterruptedException, ExecutionException, SchemaStorageException {
            if (kafkaConfig.isAuthorizationEnabled() && kafkaConfig.isKafkaEnableMultiTenantMetadata()) {
                KafkaPrincipal kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, role, username);
                String topicName = MetadataUtils.constructSchemaRegistryTopicName(tenant, kafkaConfig);
                Boolean hasPermission = authorizer
                        .canProduceAsync(kafkaPrincipal, Resource.of(ResourceType.TOPIC, topicName))
                        .get();
                if (hasPermission == null || !hasPermission) {
                    log.debug("SchemaRegistry username {} role {} tenant {} cannot access topic {}",
                            username, role, tenant, topicName);
                    throw new SchemaStorageException("Role " + role + " cannot access topic " + topicName,
                            HttpResponseStatus.FORBIDDEN.code());
                }
            }
        }

        private UsernamePasswordPair parseUsernamePassword(String authenticationHeader)
                throws SchemaStorageException {
            if (authenticationHeader.isEmpty()) {
                // no auth
                throw new SchemaStorageException("Missing AUTHORIZATION header",
                        HttpResponseStatus.UNAUTHORIZED.code());
            }

            if (!authenticationHeader.startsWith("Basic ")) {
                throw new SchemaStorageException("Bad authentication scheme, only Basic is supported",
                        HttpResponseStatus.UNAUTHORIZED.code());
            }
            String strippedAuthenticationHeader = authenticationHeader.substring("Basic ".length());

            String usernamePassword = new String(Base64.getDecoder()
                    .decode(strippedAuthenticationHeader), StandardCharsets.UTF_8);
            int colon = usernamePassword.indexOf(":");
            if (colon <= 0) {
                throw new SchemaStorageException("Bad authentication header", HttpResponseStatus.BAD_REQUEST.code());
            }
            String rawUsername = usernamePassword.substring(0, colon);
            String rawPassword = usernamePassword.substring(colon + 1);
            if (!rawPassword.startsWith("token:")) {
                throw new SchemaStorageException("Password must start with 'token:'",
                        HttpResponseStatus.UNAUTHORIZED.code());
            }
            String token = rawPassword.substring("token:".length());

            UsernamePasswordPair usernamePasswordPair = new UsernamePasswordPair(rawUsername, token);
            return usernamePasswordPair;
        }
    }

    public InetSocketAddress getAddress() {
        return new InetSocketAddress(kafkaConfig.getKopSchemaRegistryPort());
    }

    public Optional<SchemaRegistryChannelInitializer> build() throws Exception {
        if (!kafkaConfig.isKopSchemaRegistryEnable()) {
            return Optional.empty();
        }
        PulsarClient pulsarClient = pulsar.getClient();
        PulsarAdmin pulsarAdmin = pulsar.getAdminClient();
        SchemaRegistryHandler handler = new SchemaRegistryHandler();
        SchemaStorageAccessor schemaStorage = new PulsarSchemaStorageAccessor((tenant) -> {
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
                kafkaConfig.getKafkaMetadataNamespace(), kafkaConfig.getKopSchemaRegistryTopicName());
        new SchemaResource(schemaStorage, schemaRegistryRequestAuthenticator).register(handler);
        new SubjectResource(schemaStorage, schemaRegistryRequestAuthenticator).register(handler);
        return Optional.of(new SchemaRegistryChannelInitializer(handler));
    }
}
