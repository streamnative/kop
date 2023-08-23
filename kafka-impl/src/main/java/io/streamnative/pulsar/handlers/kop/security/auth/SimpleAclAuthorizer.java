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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.security.KafkaPrincipal;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;

/**
 * Simple acl authorizer.
 */
@Slf4j
public class SimpleAclAuthorizer implements Authorizer {

    private final PulsarService pulsarService;

    private final AuthorizationService authorizationService;

    private final boolean forceCheckGroupId;
    // Cache the authorization results to avoid authorizing PRODUCE or FETCH requests each time.
    // key is (topic, role)
    private final LoadingCache<Pair<TopicName, String>, Boolean> produceCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(Duration.ofMinutes(5))
            .refreshAfterWrite(Duration.ofMinutes(1))
            .build(__ -> null);
    // key is (topic, role, group)
    private final LoadingCache<Triple<TopicName, String, String>, Boolean> fetchCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(Duration.ofMinutes(5))
            .refreshAfterWrite(Duration.ofMinutes(1))
            .build(__ -> null);

    public SimpleAclAuthorizer(PulsarService pulsarService, KafkaServiceConfiguration config) {
        this.pulsarService = pulsarService;
        this.authorizationService = pulsarService.getBrokerService().getAuthorizationService();
        this.forceCheckGroupId = config.isKafkaEnableAuthorizationForceGroupIdCheck();
    }

    protected PulsarService getPulsarService() {
        return this.pulsarService;
    }

    private CompletableFuture<Boolean> authorizeTenantPermission(KafkaPrincipal principal, Resource resource) {
        CompletableFuture<Boolean> permissionFuture = new CompletableFuture<>();
        // we can only check if the tenant exists
        String tenant = resource.getName();
        getPulsarService()
                .getPulsarResources()
                .getTenantResources()
                .getTenantAsync(tenant)
                .thenAccept(tenantInfo -> {
                    permissionFuture.complete(tenantInfo.isPresent());
                }).exceptionally(ex -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Client with Principal - {} failed to get permissions for resource - {}. {}",
                                principal, resource, ex.getMessage());
                    }
                    permissionFuture.completeExceptionally(ex);
                    return null;
                });
        return permissionFuture;
    }

    @Override
    public CompletableFuture<Boolean> canAccessTenantAsync(KafkaPrincipal principal, Resource resource) {
        checkResourceType(resource, ResourceType.TENANT);
        CompletableFuture<Boolean> canAccessFuture = new CompletableFuture<>();
        authorizeTenantPermission(principal, resource).whenComplete((hasPermission, ex) -> {
                if (ex != null) {
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Resource [{}] Principal [{}] exception occurred while trying to "
                                        + "check Tenant permissions. {}",
                                resource, principal, ex.getMessage());
                    }
                    canAccessFuture.completeExceptionally(ex);
                    return;
                }
            canAccessFuture.complete(hasPermission);
        });
        return canAccessFuture;
    }

    @Override
    public CompletableFuture<Boolean> canCreateTopicAsync(KafkaPrincipal principal, Resource resource) {
        checkResourceType(resource, ResourceType.TOPIC);
        TopicName topicName = TopicName.get(resource.getName());
        return authorizationService.allowNamespaceOperationAsync(
                topicName.getNamespaceObject(),
                NamespaceOperation.CREATE_TOPIC,
                principal.getName(),
                principal.getAuthenticationData());
    }

    @Override
    public CompletableFuture<Boolean> canDeleteTopicAsync(KafkaPrincipal principal, Resource resource) {
        checkResourceType(resource, ResourceType.TOPIC);
        TopicName topicName = TopicName.get(resource.getName());
        return authorizationService.allowNamespaceOperationAsync(
                topicName.getNamespaceObject(),
                NamespaceOperation.DELETE_TOPIC,
                principal.getName(),
                principal.getAuthenticationData());
    }

    @Override
    public CompletableFuture<Boolean> canAlterTopicAsync(KafkaPrincipal principal, Resource resource) {
        checkResourceType(resource, ResourceType.TOPIC);
        TopicName topicName = TopicName.get(resource.getName());
        return authorizationService.allowTopicPolicyOperationAsync(
                topicName, PolicyName.PARTITION, PolicyOperation.WRITE,
                principal.getName(), principal.getAuthenticationData());
    }

    @Override
    public CompletableFuture<Boolean> canManageTenantAsync(KafkaPrincipal principal, Resource resource) {
        checkResourceType(resource, ResourceType.TOPIC);
        TopicName topicName = TopicName.get(resource.getName());
        return authorizationService.allowTopicOperationAsync(
                topicName, TopicOperation.LOOKUP, principal.getName(), principal.getAuthenticationData());
    }

    @Override
    public CompletableFuture<Boolean> canLookupAsync(KafkaPrincipal principal, Resource resource) {
        checkResourceType(resource, ResourceType.TOPIC);
        TopicName topicName = TopicName.get(resource.getName());
        return authorizationService.canLookupAsync(topicName, principal.getName(), principal.getAuthenticationData());
    }

    @Override
    public CompletableFuture<Boolean> canGetTopicList(KafkaPrincipal principal, Resource resource) {
        checkResourceType(resource, ResourceType.NAMESPACE);
        return authorizationService.allowNamespaceOperationAsync(
                NamespaceName.get(resource.getName()),
                NamespaceOperation.GET_TOPICS,
                principal.getName(),
                principal.getAuthenticationData());
    }

    @Override
    public CompletableFuture<Boolean> canProduceAsync(KafkaPrincipal principal, Resource resource) {
        checkResourceType(resource, ResourceType.TOPIC);
        TopicName topicName = TopicName.get(resource.getName());
        final Pair<TopicName, String> key = Pair.of(topicName, principal.getName());
        final Boolean authorized = produceCache.get(key);
        if (authorized != null) {
            return CompletableFuture.completedFuture(authorized);
        }
        return authorizationService.canProduceAsync(topicName, principal.getName(), principal.getAuthenticationData())
                .thenApply(__ -> {
                    produceCache.put(key, __);
                    return __;
                });
    }

    @Override
    public CompletableFuture<Boolean> canConsumeAsync(KafkaPrincipal principal, Resource resource) {
        checkResourceType(resource, ResourceType.TOPIC);
        TopicName topicName = TopicName.get(resource.getName());
        if (forceCheckGroupId && StringUtils.isBlank(principal.getGroupId())) {
            return CompletableFuture.completedFuture(false);
        }
        final Triple<TopicName, String, String> key = Triple.of(topicName, principal.getName(), principal.getGroupId());
        final Boolean authorized = fetchCache.get(key);
        if (authorized != null) {
            return CompletableFuture.completedFuture(authorized);
        }
        return authorizationService.canConsumeAsync(
                topicName, principal.getName(), principal.getAuthenticationData(), principal.getGroupId())
                .thenApply(__ -> {
                    fetchCache.put(key, __);
                    return __;
                });
    }

    @Override
    public CompletableFuture<Boolean> canDescribeConsumerGroup(KafkaPrincipal principal, Resource resource) {
        if (!forceCheckGroupId) {
            return CompletableFuture.completedFuture(true);
        }
        if (StringUtils.isBlank(principal.getGroupId())) {
            return CompletableFuture.completedFuture(false);
        }
        boolean isSameGroup = Objects.equals(principal.getGroupId(), resource.getName());
        if (log.isDebugEnabled()) {
            log.debug("Principal [{}] for resource [{}] isSameGroup [{}]", principal, resource, isSameGroup);
        }
        return CompletableFuture.completedFuture(isSameGroup);

    }

    private void checkResourceType(Resource actual, ResourceType expected) {
        if (actual.getResourceType() != expected) {
            throw new IllegalArgumentException(
                    String.format("Expected resource type is [%s], but have [%s]",
                            expected, actual.getResourceType()));
        }
    }

}
