/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop.migration.workflow;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.Channel;
import io.streamnative.pulsar.handlers.kop.AdminManager;
import io.streamnative.pulsar.handlers.kop.KafkaTopicLookupService;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.pulsar.broker.PulsarService;

/**
 * A MigrationWorkflowManager that uses Managed Ledger properties for metadata storage.
 */
@Slf4j
public class ManagedLedgerPropertiesWorkflowManager implements MigrationWorkflowManager {
    private static final String KAFKA_CLUSTER_ADDRESS = "migrationKafkaClusterAddress";
    private static final String TOPIC_MIGRATION_STATUS = "migrationTopicMigrationStatus";

    private final Map<String, Integer> numOutstandingRequests = new HashMap<>();
    private final AdminManager adminManager;
    private final KafkaTopicLookupService topicLookupService;

    public ManagedLedgerPropertiesWorkflowManager(PulsarService pulsarService, AdminManager adminManager) {
        this.adminManager = adminManager;
        this.topicLookupService = new KafkaTopicLookupService(pulsarService.getBrokerService());
    }

    private CompletableFuture<Map<String, String>> getManagedLedgerProperties(String topic, Channel channel) {
        return topicLookupService.getTopic(topic, channel).thenApply(persistentTopic -> {
            if (!persistentTopic.isPresent()) {
                throw new RuntimeException("Cannot get topic " + topic);
            }
            return persistentTopic.get().getManagedLedger().getProperties();
        });
    }

    @Override
    public CompletableFuture<String> getKafkaClusterAddress(String topic, Channel channel) {
        return getManagedLedgerProperties(topic, channel).thenApply(
                properties -> properties.get(KAFKA_CLUSTER_ADDRESS));
    }

    @Override
    public CompletableFuture<MigrationStatus> getMigrationStatus(String topic, Channel channel) {
        return getManagedLedgerProperties(topic, channel).thenApply(
                properties -> MigrationStatus.valueOf(properties.get(TOPIC_MIGRATION_STATUS)));
    }

    @Override
    public void startProxyRequest(String topic) {
        numOutstandingRequests.put(topic, numOutstandingRequests.getOrDefault(topic, 0) + 1);
    }

    @Override
    public void finishProxyRequest(String topic) {
        numOutstandingRequests.put(topic, numOutstandingRequests.get(topic) - 1);
    }

    private CompletableFuture<Void> setMigrationMetadata(String topic, String key, String value, Channel channel) {
        return getManagedLedgerProperties(topic, channel).thenAccept(properties -> properties.put(key, value));
    }

    private CompletableFuture<Void> setMigrationStatus(String topic, MigrationStatus status, Channel channel) {
        return setMigrationMetadata(topic, TOPIC_MIGRATION_STATUS, status.name(), channel);
    }

    @Override
    public CompletableFuture<Void> createWithMigration(String topic, String kafkaClusterAddress, Channel channel) {
        // TODO: use currentNamespacePrefix()
        String namespacePrefix = "public/default";

        // TODO: Check authorization

        // TODO: Check if topic exists in Kafka

        // TODO: Get configuration from Kafka

        return adminManager.createTopicsAsync(
                        ImmutableMap.of(topic, new CreateTopicsRequest.TopicDetails(1, (short) 1)),
                        // FIXME: support more than 1 partitions
                        1000, namespacePrefix)
                .thenCompose(validResult -> CompletableFuture.allOf(validResult.entrySet().stream().map(entry -> {
                    String key = entry.getKey();
                    ApiError value = entry.getValue();
                    log.info("Created topic partition: " + key + " with result " + value);
                    if (!entry.getValue().equals(ApiError.NONE)) {
                        throw new IllegalStateException("Cannot create topic " + key);
                    }
                    setMigrationMetadata(topic, KAFKA_CLUSTER_ADDRESS, kafkaClusterAddress, channel);
                    setMigrationStatus(topic, MigrationStatus.NOT_STARTED, channel);
                    return CompletableFuture.completedFuture(null);
                }).toArray(CompletableFuture[]::new)));
    }

    @Override
    public void startMigration(String topic, Channel channel) {
        // TODO: actually start the migration
        setMigrationStatus(topic, MigrationStatus.STARTED, channel);
    }

    @Override
    public void close() {
    }
}
