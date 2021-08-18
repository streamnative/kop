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

import io.streamnative.pulsar.handlers.kop.security.KafkaPrincipal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;

/**
 * Simple acl authorizer.
 */
@Slf4j
public class SimpleAclAuthorizer implements Authorizer {

    private static final String POLICY_ROOT = "/admin/policies";

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
        CompletableFuture<Boolean> permissionFuture = new CompletableFuture<>();
        String namespaceName = "";
        if (resource.getResourceType() == ResourceType.NAMESPACE) {
            namespaceName = resource.getName();
        } else if (resource.getResourceType() == ResourceType.TOPIC) {
            namespaceName = TopicName.get(resource.getName()).getNamespace();
        }
        String policiesPath = String.format("%s/%s", POLICY_ROOT, namespaceName);
        isSuperUser(principal.getName()).whenComplete((isSuper, exception) -> {
            if (exception != null) {
                log.error("Check super user error: {}", exception.getMessage());
                return;
            }
            if (isSuper) {
                permissionFuture.complete(true);
            } else {
                getPulsarService()
                        .getPulsarResources()
                        .getNamespaceResources()
                        .getAsync(policiesPath)
                        .thenAccept(policies -> {
                            if (!policies.isPresent()) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Policies node couldn't be found for namespace : {}", principal);
                                }
                            } else {
                                String role = principal.getName();

                                // Check Topic level policies
                                if (resource.getResourceType() == ResourceType.TOPIC) {
                                    TopicName topicName = TopicName.get(resource.getName());
                                    Map<String, Set<AuthAction>> topicRoles = policies.get()
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
                                }

                                // Check Namespace level policies
                                Map<String, Set<AuthAction>> namespaceRoles = policies.get().auth_policies
                                        .getNamespaceAuthentication();
                                Set<AuthAction> namespaceActions = namespaceRoles.get(role);
                                if (namespaceActions != null && namespaceActions.contains(action)) {
                                    permissionFuture.complete(true);
                                    return;
                                }

                                // Check wildcard policies
                                if (conf.isAuthorizationAllowWildcardsMatching()) {
                                    if (checkWildcardPermission(role, action, namespaceRoles)) {
                                        // The role has namespace level permission by wildcard match
                                        permissionFuture.complete(true);
                                        return;
                                    }
                                }
                            }
                            permissionFuture.complete(false);
                        }).exceptionally(ex -> {
                            log.warn("Client with Principal - {} failed to get permissions for resource - {}. {}",
                                    principal, resource, ex.getMessage());
                            permissionFuture.completeExceptionally(ex);
                            return null;
                        });
            }
        });

        return permissionFuture;
    }

    private CompletableFuture<Boolean> isSuperUser(String role) {
        Set<String> superUserRoles = conf.getSuperUserRoles();
        return CompletableFuture.completedFuture(role != null && superUserRoles.contains(role));
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

    @Override
    public CompletableFuture<Boolean> canLookupAsync(KafkaPrincipal principal, Resource resource) {
        CompletableFuture<Boolean> canLookupFuture = new CompletableFuture<>();
        authorize(principal, AuthAction.produce, resource).whenComplete((hasProducePermission, ex) -> {
            if (ex != null) {
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Resource [{}] Principal [{}] exception occurred while trying to "
                                    + "check Produce permissions. {}",
                            resource, principal, ex.getMessage());
                }
                return;
            }
            if (hasProducePermission) {
                canLookupFuture.complete(true);
                return;
            }
            authorize(principal, AuthAction.consume, resource).whenComplete((hasConsumerPermission, e) -> {
                if (e != null) {
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Resource [{}] Principal [{}] exception occurred while trying to "
                                        + "check Consume permissions. {}",
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

    @Override
    public CompletableFuture<Boolean> canManageTopicAsync(KafkaPrincipal principal, Resource resource) {
        checkArgument(resource.getResourceType() == ResourceType.NAMESPACE,
                String.format("Expected resource type is NAMESPACE, but have [%s]", resource.getResourceType()));
        return authorize(principal, AuthAction.packages, resource);
    }

}