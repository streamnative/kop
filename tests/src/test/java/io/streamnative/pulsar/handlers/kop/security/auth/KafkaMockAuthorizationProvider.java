package io.streamnative.pulsar.handlers.kop.security.auth;

import org.apache.pulsar.broker.auth.MockAuthorizationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;

import java.util.concurrent.CompletableFuture;

public class KafkaMockAuthorizationProvider extends MockAuthorizationProvider {
    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic, String role, TopicOperation operation, AuthenticationDataSource authData) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public Boolean allowTopicOperation(TopicName topicName, String role, TopicOperation operation, AuthenticationDataSource authData) {
        return true;
    }

    @Override
    public CompletableFuture<Boolean> allowTopicOperationAsync(TopicName topic, String originalRole, String role, TopicOperation operation, AuthenticationDataSource authData) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public Boolean allowTopicOperation(TopicName topicName, String originalRole, String role, TopicOperation operation, AuthenticationDataSource authData) {
        return true;
    }

    @Override
    public CompletableFuture<Boolean> allowTopicPolicyOperationAsync(TopicName topic, String role, PolicyName policy, PolicyOperation operation, AuthenticationDataSource authData) {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public Boolean allowTopicPolicyOperation(TopicName topicName, String role, PolicyName policy, PolicyOperation operation, AuthenticationDataSource authData) {
        return true;
    }
}
