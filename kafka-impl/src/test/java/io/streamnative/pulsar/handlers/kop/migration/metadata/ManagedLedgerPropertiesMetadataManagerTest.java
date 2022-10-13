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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.Channel;
import io.streamnative.pulsar.handlers.kop.AdminManager;
import io.streamnative.pulsar.handlers.kop.KafkaTopicLookupService;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.testng.annotations.Test;

public class ManagedLedgerPropertiesMetadataManagerTest {
    private static final String NAMESPACE_PREFIX = "public/default";

    private String fullPartitionName(String topic, int partition) {
        return String.format("%s-partition-%d", fullTopicName(topic), partition);
    }

    private String fullTopicName(String topic) {
        return String.format("persistent://%s/%s", NAMESPACE_PREFIX, topic);
    }

    @Test
    public void testGetKafkaProducerForTopic() {
        ManagedLedgerPropertiesMigrationMetadataManager metadataManager =
                new ManagedLedgerPropertiesMigrationMetadataManager(mock(KafkaTopicLookupService.class),
                        mock(AdminManager.class));
        String topic = "topic";
        String address = "127.0.01:9091";
        KafkaProducer<String, ByteBuffer> producer =
                metadataManager.getKafkaProducerForTopic(topic, NAMESPACE_PREFIX, address);
        assertEquals(metadataManager.kafkaProducers.size(), 1);

        // Make sure the producer is cached
        assertSame(producer, metadataManager.getKafkaProducerForTopic(topic, NAMESPACE_PREFIX, address));
        assertEquals(metadataManager.kafkaProducers.size(), 1);

        metadataManager.close();
    }

    @Test
    public void testGetKafkaConsumerForTopic() {
        ManagedLedgerPropertiesMigrationMetadataManager metadataManager =
                new ManagedLedgerPropertiesMigrationMetadataManager(mock(KafkaTopicLookupService.class),
                        mock(AdminManager.class));
        String topic = "topic";
        String address = "127.0.01:9091";
        KafkaConsumer<String, ByteBuffer> consumer =
                metadataManager.getKafkaConsumerForTopic(topic, NAMESPACE_PREFIX, address);
        assertEquals(metadataManager.kafkaConsumers.size(), 1);

        // Make sure the consumer is cached
        assertSame(consumer, metadataManager.getKafkaConsumerForTopic(topic, NAMESPACE_PREFIX, address));
        assertEquals(metadataManager.kafkaConsumers.size(), 1);

        metadataManager.close();
    }

    private ManagedLedgerPropertiesMigrationMetadataManager setUpMetadata(String topic, String address,
                                                                          MigrationStatus migrationStatus) {
        KafkaTopicLookupService topicLookupService = mock(KafkaTopicLookupService.class);
        PersistentTopic persistentTopic = mock(PersistentTopic.class);
        ManagedLedger managedLedger = mock(ManagedLedger.class);

        when(topicLookupService.getTopic(eq(fullPartitionName(topic, 0)), nullable(Channel.class))).thenReturn(
                CompletableFuture.completedFuture(Optional.of(persistentTopic)));
        when(persistentTopic.getManagedLedger()).thenReturn(managedLedger);
        when(managedLedger.getProperties()).thenReturn(
                ImmutableMap.of(ManagedLedgerPropertiesMigrationMetadataManager.KAFKA_CLUSTER_ADDRESS, address,
                        ManagedLedgerPropertiesMigrationMetadataManager.TOPIC_MIGRATION_STATUS,
                        migrationStatus.name()));

        return new ManagedLedgerPropertiesMigrationMetadataManager(topicLookupService, mock(AdminManager.class));
    }

    @Test
    public void testGetMigrationMetadata() {
        String address = "127.0.0.1:9091";
        String topic = "topic";
        MigrationStatus migrationStatus = MigrationStatus.NOT_STARTED;
        ManagedLedgerPropertiesMigrationMetadataManager metadataManager =
                setUpMetadata(topic, address, migrationStatus);
        assertEquals(metadataManager.getMigrationMetadata(topic, NAMESPACE_PREFIX, mock(Channel.class)).join(),
                new MigrationMetadata(address, migrationStatus));

        metadataManager.close();
    }

    @Test
    public void testGetKafkaClusterAddress() {
        String address = "127.0.0.1:9091";
        String topic = "topic";
        MigrationStatus migrationStatus = MigrationStatus.NOT_STARTED;
        ManagedLedgerPropertiesMigrationMetadataManager metadataManager =
                setUpMetadata(topic, address, migrationStatus);
        assertEquals(metadataManager.getKafkaClusterAddress(topic, NAMESPACE_PREFIX, mock(Channel.class)).join(),
                address);

        metadataManager.close();
    }

    @Test
    public void testGetMigrationStatus() {
        String address = "127.0.0.1:9091";
        String topic = "topic";
        MigrationStatus migrationStatus = MigrationStatus.STARTED;
        ManagedLedgerPropertiesMigrationMetadataManager metadataManager =
                setUpMetadata(topic, address, migrationStatus);
        assertEquals(metadataManager.getMigrationStatus(topic, NAMESPACE_PREFIX, mock(Channel.class)).join(),
                migrationStatus);

        metadataManager.close();
    }

    @Test
    public void testStartFinishProxyRequest() {
        ManagedLedgerPropertiesMigrationMetadataManager metadataManager =
                new ManagedLedgerPropertiesMigrationMetadataManager(mock(KafkaTopicLookupService.class),
                        mock(AdminManager.class));
        String topic = "topic";
        metadataManager.startProxyRequest(topic, NAMESPACE_PREFIX);

        assertEquals(metadataManager.numOutstandingRequests.get(fullTopicName(topic)).intValue(), 1);
        metadataManager.finishProxyRequest(topic, NAMESPACE_PREFIX);
        assertEquals(metadataManager.numOutstandingRequests.get(fullTopicName(topic)).intValue(), 0);
    }

    @Test
    public void testCreateWithMigration() {
        // TODO
    }
}
