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
import java.util.concurrent.CompletableFuture;

/**
 * The interface for managing the workflow of Kafka to KoP topic migration.
 */
public interface MigrationWorkflowManager {
    /**
     * Gets the Kafka cluster address for a topic.
     *
     * @param topic the topic
     * @return the address of the Kafka cluster containing the topic
     */
    CompletableFuture<String> getKafkaClusterAddress(String topic, Channel channel);

    /**
     * Returns the migration status of the topic
     *
     * @param topic the topic
     * @return the migration status
     */
    CompletableFuture<MigrationStatus> getMigrationStatus(String topic, Channel channel);

    /**
     * This marks the start of a request proxied through to Kafka for a topic before the migration.
     *
     * @param topic the topic
     */
    void startProxyRequest(String topic);

    /**
     * This marks the end of a request proxied through to Kafka for a topic before the migration.
     *
     * @param topic the topic
     */
    void finishProxyRequest(String topic);

    /**
     * Create a KoP topic with Kafka migration configuration.
     *
     * @param topic               the topic
     * @param kafkaClusterAddress the address of the Kafka cluster containing the topic
     */
    CompletableFuture<Void> createWithMigration(String topic, String kafkaClusterAddress, Channel channel);

    /**
     * Start migrating a topic from Kafka.
     *
     * @param topic the topic
     */
    void startMigration(String topic, Channel channel);

    /**
     * Close this and clean up all resource usages.
     */
    void close();
}
