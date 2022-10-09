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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.Channel;
import io.streamnative.pulsar.handlers.kop.AdminManager;
import io.streamnative.pulsar.handlers.kop.KafkaTopicLookupService;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * A MigrationMetadata Manager that uses Managed Ledger properties for metadata storage.
 */
@Slf4j
public class ManagedLedgerPropertiesMigrationMetadataManager implements MigrationMetadataManager {
    @VisibleForTesting
    static final String KAFKA_CLUSTER_ADDRESS = "migrationKafkaClusterAddress";
    @VisibleForTesting
    static final String TOPIC_MIGRATION_STATUS = "migrationTopicMigrationStatus";

    @VisibleForTesting
    final Map<String, Integer> numOutstandingRequests = new ConcurrentHashMap<>();
    @VisibleForTesting
    final Map<String, KafkaProducer<String, ByteBuffer>> kafkaProducers = new ConcurrentHashMap<>();
    @VisibleForTesting
    final Map<String, KafkaConsumer<String, ByteBuffer>> kafkaConsumers = new ConcurrentHashMap<>();
    @VisibleForTesting
    final Map<String, AdminClient> adminClients = new ConcurrentHashMap<>();

    // Cache topics that are not configured with migration so save lookup costs
    private final Set<String> nonMigratoryTopics = new ConcurrentSkipListSet<>();

    private final AdminManager adminManager;
    private final KafkaTopicLookupService topicLookupService;

    public ManagedLedgerPropertiesMigrationMetadataManager(KafkaTopicLookupService topicLookupService,
                                                           AdminManager adminManager) {
        this.adminManager = adminManager;
        this.topicLookupService = topicLookupService;
    }

    @Override
    public KafkaProducer<String, ByteBuffer> getKafkaProducerForTopic(String topic, String namespacePrefix,
                                                                      String kafkaClusterAddress) {
        String fullTopicName = new KopTopic(topic, namespacePrefix).getFullName();
        return kafkaProducers.computeIfAbsent(fullTopicName, key -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaClusterAddress);
            props.put("key.serializer", StringSerializer.class);
            props.put("value.serializer", ByteBufferSerializer.class);
            return new KafkaProducer<>(props);
        });
    }

    @Override
    public KafkaConsumer<String, ByteBuffer> getKafkaConsumerForTopic(String topic, String namespacePrefix,
                                                                      String kafkaClusterAddress) {
        String fullTopicName = new KopTopic(topic, namespacePrefix).getFullName();
        return kafkaConsumers.computeIfAbsent(fullTopicName, key -> {
            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaClusterAddress);
            props.put("key.deserializer", StringDeserializer.class);
            props.put("value.deserializer", ByteBufferDeserializer.class);
            return new KafkaConsumer<>(props);
        });
    }

    @Override
    public AdminClient getAdminClientForKafka(String kafkaClusterAddress) {
        return adminClients.computeIfAbsent(kafkaClusterAddress, key -> {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaClusterAddress);
            return AdminClient.create(props);
        });
    }

    private CompletableFuture<Map<String, String>> getManagedLedgerProperties(String topic, String namespacePrefix,
                                                                              Channel channel) {
        String fullPartitionName = KopTopic.toString(topic, 0, namespacePrefix);
        return topicLookupService.getTopic(fullPartitionName, channel).thenApply(persistentTopic -> {
            if (!persistentTopic.isPresent()) {
                throw new IllegalArgumentException("Cannot get topic " + fullPartitionName);
            }
            return persistentTopic.get().getManagedLedger().getProperties();
        });
    }

    @Override
    public CompletableFuture<MigrationMetadata> getMigrationMetadata(String topic, String namespacePrefix,
                                                                     Channel channel) {
        if (nonMigratoryTopics.contains(topic)) {
            return null;
        }
        return getManagedLedgerProperties(topic, namespacePrefix, channel).thenApply(properties -> {
            String status = properties.get(TOPIC_MIGRATION_STATUS);
            if (status == null) {
                nonMigratoryTopics.add(topic);
                return null;
            }

            String kafkaClusterAddress = properties.get(KAFKA_CLUSTER_ADDRESS);
            if (kafkaClusterAddress == null) {
                log.error("Topic {} migration misconfigured", topic);
                nonMigratoryTopics.add(topic);
                return null;
            }

            return new MigrationMetadata(kafkaClusterAddress, MigrationStatus.valueOf(status));
        });
    }

    @Override
    public CompletableFuture<String> getKafkaClusterAddress(String topic, String namespacePrefix, Channel channel) {
        return getMigrationMetadata(topic, namespacePrefix, channel).thenApply(migrationMetadata -> {
            if (migrationMetadata == null) {
                return null;
            }
            return migrationMetadata.getKafkaClusterAddress();
        });
    }

    @Override
    public CompletableFuture<MigrationStatus> getMigrationStatus(String topic, String namespacePrefix,
                                                                 Channel channel) {
        return getMigrationMetadata(topic, namespacePrefix, channel).thenApply(migrationMetadata -> {
            if (migrationMetadata == null) {
                return null;
            }
            return migrationMetadata.getMigrationStatus();
        });
    }

    @Override
    public void startProxyRequest(String topic, String namespacePrefix) {
        String fullTopicName = new KopTopic(topic, namespacePrefix).getFullName();
        numOutstandingRequests.put(fullTopicName, numOutstandingRequests.getOrDefault(topic, 0) + 1);
    }

    @Override
    public void finishProxyRequest(String topic, String namespacePrefix) {
        String fullTopicName = new KopTopic(topic, namespacePrefix).getFullName();
        numOutstandingRequests.compute(fullTopicName, (key, value) -> {
            if (value == null) {
                log.error("Cannot finish request for topic {}; no request has been proxied", topic);
                return null;
            }
            if (value == 0) {
                log.error("Cannot finish more requests than started for topic {}", topic);
                return 0;
            }
            return value - 1;
        });
    }

    private CompletableFuture<Void> setMigrationMetadata(String topic, String namespacePrefix, String key, String value,
                                                         Channel channel) {
        return getManagedLedgerProperties(topic, namespacePrefix, channel).thenAccept(
                properties -> properties.put(key, value));
    }

    private CompletableFuture<Void> setMigrationStatus(String topic, String namespacePrefix, MigrationStatus status,
                                                       Channel channel) {
        return setMigrationMetadata(topic, namespacePrefix, TOPIC_MIGRATION_STATUS, status.name(), channel);
    }

    @Override
    public CompletableFuture<Void> createWithMigration(String topic, String namespacePrefix, String kafkaClusterAddress,
                                                       Channel channel) {
        // TODO: Check authorization

        AdminClient adminClient = getAdminClientForKafka(kafkaClusterAddress);
        KafkaFuture<TopicDescription> future =
                new ArrayList<>(adminClient.describeTopics(Collections.singleton(topic)).values().values()).get(0);

        // https://gist.github.com/bmaggi/8e42a16a02f18d3bff9b0b742a75bfe7
        CompletableFuture<Void> wrappingFuture = new CompletableFuture<>();

        future.thenApply(topicDescription -> {
            log.error(topicDescription.toString());
            int numPartitions = topicDescription.partitions().size();
            int replicationFactor = topicDescription.partitions().get(0).replicas().size();
            adminManager.createTopicsAsync(ImmutableMap.of(topic,
                                    new CreateTopicsRequest.TopicDetails(numPartitions, (short) replicationFactor)),
                            1000,
                            namespacePrefix)
                    .thenCompose(validResult -> CompletableFuture.allOf(validResult.entrySet().stream().map(entry -> {
                        String key = entry.getKey();
                        ApiError value = entry.getValue();
                        if (!value.equals(ApiError.NONE)) {
                            throw value.exception();
                        }
                        log.info("Created topic partition: " + key + " with result " + value);
                        return setMigrationMetadata(topic, namespacePrefix, KAFKA_CLUSTER_ADDRESS, kafkaClusterAddress,
                                channel).thenCompose(
                                ignored -> setMigrationStatus(
                                        topic,
                                        namespacePrefix,
                                        MigrationStatus.NOT_STARTED,
                                        channel));
                    }).toArray(CompletableFuture[]::new))).join();
            return null;
        }).whenComplete((value, throwable) -> {
            if (throwable != null) {
                wrappingFuture.completeExceptionally(throwable);
            } else {
                wrappingFuture.complete(null);
            }
        });
        return wrappingFuture;
    }

    @Override
    public CompletableFuture<Void> migrate(String topic, String namespacePrefix, Channel channel) {
        // TODO: actually start the migration
        setMigrationStatus(topic, namespacePrefix, MigrationStatus.STARTED, channel);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
        kafkaProducers.values().forEach(Producer::close);
        kafkaConsumers.values().forEach(Consumer::close);
        adminClients.values().forEach(AdminClient::close);
    }
}
