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
package io.streamnative.pulsar.handlers.kop.migration.workflow;

import io.netty.channel.Channel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class InMemoryMigrationWorkflowManager implements MigrationWorkflowManager {
    private static final String KAFKA_CLUSTER_ADDRESS = "migrationKafkaClusterAddress";
    private static final String TOPIC_MIGRATION_STATUS = "migrationTopicMigrationStatus";
    Map<String, Integer> numOutstandingRequests = new HashMap<>();
    Map<String, Map<String, String>> topicMetadata = new HashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    @Override
    public CompletableFuture<String> getKafkaClusterAddress(String topic, Channel channel) {
        return CompletableFuture.completedFuture(topicMetadata.get(topic).get(KAFKA_CLUSTER_ADDRESS));
    }

    @Override
    public CompletableFuture<MigrationStatus> getMigrationStatus(String topic, Channel channel) {
        return CompletableFuture.completedFuture(
                MigrationStatus.valueOf(topicMetadata.get(topic).get(TOPIC_MIGRATION_STATUS)));
    }

    @Override
    public void startProxyRequest(String topic) {
        numOutstandingRequests.put(topic, numOutstandingRequests.getOrDefault(topic, 0) + 1);
    }

    @Override
    public void finishProxyRequest(String topic) {
        numOutstandingRequests.put(topic, numOutstandingRequests.get(topic) - 1);
    }

    @Override
    public CompletableFuture<Void> createWithMigration(String topic, String kafkaClusterAddress, Channel channel) {
        setMigrationStatus(topic, MigrationStatus.NOT_STARTED);
        topicMetadata.computeIfAbsent(topic, ignored -> new HashMap<>())
                .put(KAFKA_CLUSTER_ADDRESS, kafkaClusterAddress);
        return CompletableFuture.completedFuture(null);
    }

    private void setMigrationStatus(String topic, MigrationStatus status) {
        topicMetadata.get(topic).put(TOPIC_MIGRATION_STATUS, status.name());
    }

    @Override
    public void startMigration(String topic, Channel channel) {
        setMigrationStatus(topic, MigrationStatus.STARTED);
    }

    @Override
    public void close() {
        scheduler.shutdown();
    }
}
