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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.auth.MockAuthorizationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;


public class KafkaMockAuthorizationProvider extends MockAuthorizationProvider {
    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(
            TopicName topic, String role, TopicOperation operation, AuthenticationDataSource authData) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public Boolean allowTopicOperation(
            TopicName topicName,
            String role,
            TopicOperation operation,
            AuthenticationDataSource authData) {
        return true;
    }

    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(
            TopicName topic,
            String originalRole,
            String role,
            TopicOperation operation,
            AuthenticationDataSource authData) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public Boolean allowTopicOperation(
            TopicName topicName,
            String originalRole,
            String role,
            TopicOperation operation,
            AuthenticationDataSource authData) {
        return true;
    }

    @Override
    public CompletableFuture<Boolean> allowTopicPolicyOperationAsync(
            TopicName topic,
            String role,
            PolicyName policy,
            PolicyOperation operation,
            AuthenticationDataSource authData) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public Boolean allowTopicPolicyOperation(
            TopicName topicName,
            String role,
            PolicyName policy,
            PolicyOperation operation,
            AuthenticationDataSource authData) {
        return true;
    }
}
