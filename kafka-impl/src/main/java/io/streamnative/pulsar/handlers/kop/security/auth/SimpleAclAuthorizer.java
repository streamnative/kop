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


import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Joiner;
import io.streamnative.pulsar.handlers.kop.security.KafkaPrincipal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;

/**
 * Simple acl authorizer.
 */
@Slf4j
public class SimpleAclAuthorizer implements Authorizer {

    private static final String POLICY_ROOT = "/admin/policies/";

    private final PulsarService pulsarService;

    private final ServiceConfiguration conf;

    public SimpleAclAuthorizer(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
        this.conf = pulsarService.getConfiguration();
    }

    protected PulsarService getPulsarService() {
        return this.pulsarService;
    }

    private CompletableFuture<Boolean> authorize(KafkaPrincipal principal, AuthAction action, Resource resource) {

        switch (resource.getResourceType()) {
            case TOPIC:
                return authorizeTopicPermission(principal, action, resource);
            case TENANT:
                return authorizeTenantPermission(principal, resource);
            default:
                return CompletableFuture.completedFuture(false);
        }
    }

    private CompletableFuture<Boolean> authorizeTopicPermission(KafkaPrincipal principal, AuthAction action,
                                                                Resource resource) {
        CompletableFuture<Boolean> permissionFuture = new CompletableFuture<>();
        TopicName topicName = TopicName.get(resource.getName());
        NamespaceName namespace = topicName.getNamespaceObject();
        if (namespace == null) {
            permissionFuture.completeExceptionally(
                    new IllegalArgumentException("Resource name must contains namespace."));
            return permissionFuture;
        }
        String policiesPath = path(namespace.toString());
        String tenantName = namespace.getTenant();
        isSuperUserOrTenantAdmin(tenantName, principal.getName()).whenComplete((isSuperUserOrAdmin, exception) -> {
            if (exception != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Verify if role {} is allowed to {} to resource {}: isSuperUserOrAdmin={}",
                            principal.getName(), action, resource.getName(), isSuperUserOrAdmin);
                }
                isSuperUserOrAdmin = false;
            }
            if (isSuperUserOrAdmin) {
                permissionFuture.complete(true);
                return;
            }
            getPulsarService()
                    .getPulsarResources()
                    .getNamespaceResources()
                    .getAsync(policiesPath)
                    .thenAccept(policies -> {
                        if (!policies.isPresent()) {
                            if (log.isDebugEnabled()) {
                                log.debug("Policies node couldn't be found for namespace : {}", principal);
                            }
                            permissionFuture.complete(false);
                            return;
                        }
                        authoriseTopicOverNamespacePolicies(principal, action, permissionFuture, topicName,
                                policies.get());
                    }).exceptionally(ex -> {
                        if (log.isDebugEnabled()) {
                            log.debug("Client with Principal - {} failed to get permissions for resource - {}. {}",
                                    principal, resource, ex.getMessage());
                        }
                        permissionFuture.completeExceptionally(ex);
                        return null;
                    });

        });
        return permissionFuture;
    }

    private void authoriseTopicOverNamespacePolicies(KafkaPrincipal principal, AuthAction action,
                                                     CompletableFuture<Boolean> permissionFuture,
                                                     TopicName topicName, Policies policies) {
        String role = principal.getName();
        // Check Topic level policies
        Map<String, Set<AuthAction>> topicRoles = policies
                .auth_policies
                .getTopicAuthentication()
                .get(topicName.toString());
        if (topicRoles != null && role != null) {
            // Topic has custom policy
            Set<AuthAction> topicActions = topicRoles.get(role);
            if (topicActions != null && topicActions.contains(action)) {
                permissionFuture.complete(true);
                return;
            }
        }

        // Check Namespace level policies
        Map<String, Set<AuthAction>> namespaceRoles = policies.auth_policies
                .getNamespaceAuthentication();
        Set<AuthAction> namespaceActions = namespaceRoles.get(role);
        if (namespaceActions != null && namespaceActions.contains(action)) {
            permissionFuture.complete(true);
            return;
        }

        // Check wildcard policies
        if (conf.isAuthorizationAllowWildcardsMatching()
                && checkWildcardPermission(role, action, namespaceRoles)) {
            // The role has namespace level permission by wildcard match
            permissionFuture.complete(true);
            return;
        }
        permissionFuture.complete(false);
    }

    private CompletableFuture<Boolean> authorizeTenantPermission(KafkaPrincipal principal, Resource resource) {
        CompletableFuture<Boolean> permissionFuture = new CompletableFuture<>();
        // we can only check if the tenant exists
        String tenant = resource.getName();
        getPulsarService()
                .getPulsarResources()
                .getTenantResources()
                .getAsync(path(tenant))
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

    private boolean checkWildcardPermission(String checkedRole, AuthAction checkedAction,
                                            Map<String, Set<AuthAction>> permissionMap) {
        for (Map.Entry<String, Set<AuthAction>> permissionData : permissionMap.entrySet()) {
            String permittedRole = permissionData.getKey();
            Set<AuthAction> permittedActions = permissionData.getValue();

            // Prefix match
            if (checkedRole != null) {
                if (permittedRole.charAt(permittedRole.length() - 1) == '*'
                        && checkedRole.startsWith(permittedRole.substring(0, permittedRole.length() - 1))
                        && permittedActions.contains(checkedAction)) {
                    return true;
                }

                // Suffix match
                if (permittedRole.charAt(0) == '*' && checkedRole.endsWith(permittedRole.substring(1))
                        && permittedActions.contains(checkedAction)) {
                    return true;
                }
            }
        }
        return false;
    }

    private CompletableFuture<Boolean> isSuperUser(String role) {
        Set<String> superUserRoles = conf.getSuperUserRoles();
        return CompletableFuture.completedFuture(role != null && superUserRoles.contains(role));
    }

    /**
     * Check if specified role is an admin of the tenant or superuser.
     *
     * @param tenant the tenant to check
     * @param role the role to check
     * @return a CompletableFuture containing a boolean in which true means the role is an admin user
     * and false if it is not
     */
    private CompletableFuture<Boolean> isSuperUserOrTenantAdmin(String tenant, String role) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        isSuperUser(role).whenComplete((isSuperUser, ex) -> {
            if (ex != null || !isSuperUser) {
                pulsarService.getPulsarResources()
                        .getTenantResources()
                        .getAsync(path(tenant))
                        .thenAccept(tenantInfo -> {
                            if (!tenantInfo.isPresent()) {
                                future.complete(false);
                                return;
                            }
                            TenantInfo info = tenantInfo.get();
                            future.complete(role != null
                                    && info.getAdminRoles() != null
                                    && info.getAdminRoles().contains(role));
                        });
                return;
            }
            future.complete(true);
        });
        return future;
    }

    private static String path(String... parts) {
        StringBuilder sb = new StringBuilder();
        sb.append(POLICY_ROOT);
        Joiner.on('/').appendTo(sb, parts);
        return sb.toString();
    }

    @Override
    public CompletableFuture<Boolean> canAccessTenantAsync(KafkaPrincipal principal, Resource resource) {
        checkArgument(resource.getResourceType() == ResourceType.TENANT,
                String.format("Expected resource type is TENANT, but have [%s]", resource.getResourceType()));

        CompletableFuture<Boolean> canAccessFuture = new CompletableFuture<>();
        authorize(principal, null, resource).whenComplete((hasPermission, ex) -> {
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
    public CompletableFuture<Boolean> canLookupAsync(KafkaPrincipal principal, Resource resource) {
        checkArgument(resource.getResourceType() == ResourceType.TOPIC,
                String.format("Expected resource type is TOPIC, but have [%s]", resource.getResourceType()));

        CompletableFuture<Boolean> canLookupFuture = new CompletableFuture<>();
        authorize(principal, AuthAction.consume, resource).whenComplete((hasProducePermission, ex) -> {
            if (ex != null) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Resource [{}] Principal [{}] exception occurred while trying to "
                                    + "check Consume permissions. {}",
                            resource, principal, ex.getMessage());
                }
                hasProducePermission = false;
            }
            if (hasProducePermission) {
                canLookupFuture.complete(true);
                return;
            }
            authorize(principal, AuthAction.produce, resource).whenComplete((hasConsumerPermission, e) -> {
                if (e != null) {
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Resource [{}] Principal [{}] exception occurred while trying to "
                                        + "check Produce permissions. {}",
                                resource, principal, e.getMessage());
                    }
                    canLookupFuture.completeExceptionally(e);
                    return;
                }
                canLookupFuture.complete(hasConsumerPermission);
            });
        });
        return canLookupFuture;
    }

    @Override
    public CompletableFuture<Boolean> canProduceAsync(KafkaPrincipal principal, Resource resource) {
        checkArgument(resource.getResourceType() == ResourceType.TOPIC,
                String.format("Expected resource type is TOPIC, but have [%s]", resource.getResourceType()));
        return authorize(principal, AuthAction.produce, resource);
    }

    @Override
    public CompletableFuture<Boolean> canConsumeAsync(KafkaPrincipal principal, Resource resource) {
        checkArgument(resource.getResourceType() == ResourceType.TOPIC,
                String.format("Expected resource type is TOPIC, but have [%s]", resource.getResourceType()));
        return authorize(principal, AuthAction.consume, resource);
    }

}