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
package io.streamnative.pulsar.handlers.kop.migration.metadata;

import io.netty.channel.Channel;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

/**
 * The interface for managing metadata and state related to topic migrations from Kafka.
 *
 * TODO: Find a way to cleanup all the channel arguments since they are only for logging.
 */
public interface MigrationMetadataManager {
    /**
     * Get a Kafka producer for the given topic.
     *
     * @param topic               the topic
     * @param kafkaClusterAddress address of the Kafka cluster for the topic
     * @param namespacePrefix     namespace prefix of the topic
     * @return a Kafka producer
     */
    Producer<String, ByteBuffer> getKafkaProducerForTopic(String topic, String namespacePrefix,
                                                          String kafkaClusterAddress);

    /**
     * Get a Kafka consumer for the given topic.
     *
     * @param topic               the topic
     * @param kafkaClusterAddress address of the Kafka cluster for the topic
     * @param namespacePrefix     namespace prefix of the topic
     * @return a Kafka consumer
     */
    Consumer<String, ByteBuffer> getKafkaConsumerForTopic(String topic, String namespacePrefix,
                                                          String kafkaClusterAddress);

    /**
     * Get an AdminClient for a Kafka instance.
     *
     * @param kafkaClusterAddress address of the kafka instance
     * @return the admin client
     */
    AdminClient getAdminClientForKafka(String kafkaClusterAddress);

    /**
     * Gets the migration metadata for a topic.
     *
     * @param topic           the topic
     * @param channel         the channel where the original request came from
     * @param namespacePrefix namespace prefix of the topic
     * @return the migration metadata of the topic, null if the topic isn't configured for migration
     */
    CompletableFuture<MigrationMetadata> getMigrationMetadata(String topic, String namespacePrefix, Channel channel);

    /**
     * Gets the Kafka cluster address for a topic.
     *
     * @param topic           the topic
     * @param channel         the channel where the original request came from
     * @param namespacePrefix namespace prefix of the topic
     * @return the address of the Kafka cluster containing the topic, null if the topic isn't configured for migration
     */
    CompletableFuture<String> getKafkaClusterAddress(String topic, String namespacePrefix, Channel channel);

    /**
     * Returns the migration status of the topic.
     *
     * @param topic           the topic
     * @param channel         the channel where the original request came from
     * @param namespacePrefix namespace prefix of the topic
     * @return the migration status, null if the topic isn't configured for migration
     */
    CompletableFuture<MigrationStatus> getMigrationStatus(String topic, String namespacePrefix, Channel channel);

    /**
     * This marks the start of a request proxied through to Kafka for a topic before the migration.
     *
     * @param topic           the topic
     * @param namespacePrefix namespace prefix of the topic
     */
    void startProxyRequest(String topic, String namespacePrefix);

    /**
     * This marks the end of a request proxied through to Kafka for a topic before the migration.
     *
     * @param topic           the topic
     * @param namespacePrefix namespace prefix of the topic
     */
    void finishProxyRequest(String topic, String namespacePrefix);

    /**
     * Create a KoP topic with Kafka migration configuration.
     *
     * @param topic               the topic
     * @param namespacePrefix     namespace prefix of the topic
     * @param kafkaClusterAddress the address of the Kafka cluster containing the topic
     * @param channel             the channel where the original request came from
     */
    CompletableFuture<Void> createWithMigration(String topic, String namespacePrefix, String kafkaClusterAddress,
                                                Channel channel);

    /**
     * Migrate a topic from Kafka.
     *
     * @param topic           the topic
     * @param namespacePrefix namespace prefix of the topic
     * @param channel         the channel where the original request came from
     */
    CompletableFuture<Void> migrate(String topic, String namespacePrefix, Channel channel);

    /**
     * Close this and clean up all resource usages.
     */
    void close();
}
