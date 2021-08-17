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


import io.streamnative.pulsar.handlers.kop.security.KafkaPrincipal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;

/**
 * Simple acl authorizer.
 */
@Slf4j
public class SimpleAclAuthorizer implements Authorizer {

    private static final String POLICY_ROOT = "/admin/policies";

    private final PulsarService pulsarService;

    public SimpleAclAuthorizer(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
    }

    protected PulsarService getPulsarService() {
        return this.pulsarService;
    }

    private CompletableFuture<Boolean> authorize(KafkaPrincipal principal, AuthAction action, Resource resource) {
        CompletableFuture<Boolean> permissionFuture = new CompletableFuture<>();
        TopicName topicName = TopicName.get(resource.getName());
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
                        .getAsync(String.format("%s/%s", POLICY_ROOT, topicName.getNamespace()))
                        .thenAccept(policies -> {
                            if (!policies.isPresent()) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Policies node couldn't be found for namespace : {}", principal);
                                }
                            } else {
                                String role = principal.getName();
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

                                Map<String, Set<AuthAction>> namespaceRoles = policies.get().auth_policies
                                        .getNamespaceAuthentication();
                                Set<AuthAction> namespaceActions = namespaceRoles.get(role);
                                if (namespaceActions != null && namespaceActions.contains(action)) {
                                    permissionFuture.complete(true);
                                    return;
                                }
                            }
                            permissionFuture.complete(false);
                        }).exceptionally(ex -> {
                            log.warn("Client with Principal - {} failed to get permissions for resource - {}. {}",
                                    principal, topicName, ex.getMessage());
                            permissionFuture.completeExceptionally(ex);
                            return null;
                        });
            }
        });

        return permissionFuture;
    }

    private CompletableFuture<Boolean> isSuperUser(String role) {
        Set<String> superUserRoles = getPulsarService().getConfiguration().getSuperUserRoles();
        return CompletableFuture.completedFuture(role != null && superUserRoles.contains(role));
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
        return authorize(principal, AuthAction.produce, resource);
    }

    @Override
    public CompletableFuture<Boolean> canConsumeAsync(KafkaPrincipal principal, Resource resource) {
        return authorize(principal, AuthAction.consume, resource);
    }

}