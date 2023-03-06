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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authorization.AuthorizationProvider;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.testng.Assert;


@Slf4j
public class KafkaMockAuthorizationProvider implements AuthorizationProvider {

    @Override
    public void close() {
        // no-op
    }

    @Override
    public CompletableFuture<Boolean> isSuperUser(String role,
                                                  AuthenticationDataSource authenticationData,
                                                  ServiceConfiguration serviceConfiguration) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> isSuperUser(String role, ServiceConfiguration serviceConfiguration) {
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> isTenantAdmin(String tenant, String role, TenantInfo tenantInfo,
                                                    AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> canProduceAsync(TopicName topicName, String role,
                                                      AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> canConsumeAsync(TopicName topicName, String role,
                                                      AuthenticationDataSource authenticationData,
                                                      String subscription) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> canLookupAsync(TopicName topicName, String role,
                                                     AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> allowFunctionOpsAsync(NamespaceName namespaceName, String role,
                                                            AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> allowSourceOpsAsync(NamespaceName namespaceName, String role,
                                                          AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Boolean> allowSinkOpsAsync(NamespaceName namespaceName, String role,
                                                        AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorizedAsync(role);
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(NamespaceName namespace, Set<AuthAction> actions, String role,
                                                        String authDataJson) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> grantSubscriptionPermissionAsync(NamespaceName namespace,
                                                                    String subscriptionName, Set<String> roles,
                                                                    String authDataJson) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> revokeSubscriptionPermissionAsync(NamespaceName namespace, String subscriptionName,
                                                                     String role, String authDataJson) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(TopicName topicName, Set<AuthAction> actions, String role,
                                                        String authDataJson) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> allowTenantOperationAsync(String tenantName, String role,
                                                                TenantOperation operation,
                                                                AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorizedAsync(role);
    }

    @Override
    public Boolean allowTenantOperation(String tenantName, String role, TenantOperation operation,
                                        AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorized(role);
    }

    @Override
    public CompletableFuture<Boolean> allowNamespaceOperationAsync(NamespaceName namespaceName,
                                                                   String role,
                                                                   NamespaceOperation operation,
                                                                   AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorizedAsync(role);
    }

    @Override
    public Boolean allowNamespaceOperation(NamespaceName namespaceName,
                                           String role,
                                           NamespaceOperation operation,
                                           AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorized(role);
    }

    @Override
    public CompletableFuture<Boolean> allowNamespacePolicyOperationAsync(NamespaceName namespaceName,
                                                                         PolicyName policy,
                                                                         PolicyOperation operation,
                                                                         String role,
                                                                         AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorizedAsync(role);
    }

    @Override
    public Boolean allowNamespacePolicyOperation(NamespaceName namespaceName,
                                                 PolicyName policy,
                                                 PolicyOperation operation,
                                                 String role,
                                                 AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorized(role);
    }

    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic,
                                                               String role,
                                                               TopicOperation operation,
                                                               AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorizedAsync(role);
    }

    @Override
    public Boolean allowTopicOperation(TopicName topicName,
                                       String role,
                                       TopicOperation operation,
                                       AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorized(role);
    }

    CompletableFuture<Boolean> roleAuthorizedAsync(String role) {
        CompletableFuture<Boolean> promise = new CompletableFuture<>();
        try {
            promise.complete(roleAuthorized(role));
        } catch (Exception e) {
            promise.completeExceptionally(e);
        }
        return promise;
    }

    @Override
    public CompletableFuture<Boolean> allowTopicPolicyOperationAsync(
            TopicName topic,
            String role,
            PolicyName policy,
            PolicyOperation operation,
            AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorizedAsync(role);
    }

    @Override
    public Boolean allowTopicPolicyOperation(
            TopicName topicName,
            String role,
            PolicyName policy,
            PolicyOperation operation,
            AuthenticationDataSource authenticationData) {
        Assert.assertNotNull(authenticationData);
        return roleAuthorized(role);
    }

    public boolean roleAuthorized(String role) {
        String[] parts = role.split("\\.");
        if (parts.length == 2) {
            switch (parts[1]) {
                case "pass":
                    return true;
                case "fail":
                    return false;
                case "error":
                    throw new IllegalStateException("Error in authn");
                default:
                   return false;
            }
        }
        throw new IllegalArgumentException(
                "Not a valid principle. Should be [pass|fail|error].[pass|fail|error], found " + role);
    }
}
