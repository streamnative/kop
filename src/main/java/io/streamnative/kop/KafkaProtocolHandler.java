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
package io.streamnative.kop;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.kop.coordinator.group.GroupConfig;
import io.streamnative.kop.coordinator.group.GroupCoordinator;
import io.streamnative.kop.coordinator.group.OffsetConfig;
import io.streamnative.kop.offset.OffsetAndMetadata;
import io.streamnative.kop.utils.timer.SystemTimer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;

/**
 * Kafka Protocol Handler load and run by Pulsar Service.
 */
@Slf4j
public class KafkaProtocolHandler implements ProtocolHandler {

    public static final String PROTOCOL_NAME = "kafka";

    @Getter
    private KafkaServiceConfiguration kafkaConfig;
    @Getter
    private BrokerService brokerService;
    @Getter
    private KafkaTopicManager kafkaTopicManager;
    @Getter
    private GroupCoordinator groupCoordinator;

    @Override
    public String protocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return PROTOCOL_NAME.equals(protocol.toLowerCase());
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        // no-op
        kafkaConfig = (KafkaServiceConfiguration) conf;
    }

    @Override
    public String getProtocolDataToAdvertise() {
        return "mock-data-for-kafka";
    }

    @Override
    public void start(BrokerService service) {
        brokerService = service;
        // a topic Manager
        kafkaTopicManager = new KafkaTopicManager(service);

        // start group coordinator
        if (kafkaConfig.isEnableGroupCoordinator()) {
            try {
                startGroupCoordinator(service);
            } catch (Exception e) {
                log.error("KafkaProtocolHandler start failed with", e);
            }
        }
    }

    // this is called after start, and with kafkaTopicManager, kafkaConfig, brokerService all set.
    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        //checkstate

        Optional<Integer> port = kafkaConfig.getKafkaServicePort();
        InetSocketAddress addr = new InetSocketAddress(brokerService.pulsar().getBindAddress(), port.get());

        try {
            Map<InetSocketAddress, ChannelInitializer<SocketChannel>> initializerMap =
                ImmutableMap.<InetSocketAddress, ChannelInitializer<SocketChannel>>builder()
                    .put(addr,
                        (new KafkaChannelInitializer(brokerService.pulsar(),
                            kafkaConfig,
                            kafkaTopicManager,
                            groupCoordinator,
                            false)))
                    .build();
            return initializerMap;
        } catch (Exception e){
            log.error("KafkaProtocolHandler newChannelInitializers failed with", e);
            return null;
        }
    }

    @Override
    public void close() {
        if (groupCoordinator != null) {
            groupCoordinator.shutdown();
        }
    }

    // TODO: make group coordinator running in a distributed mode
    //      https://github.com/streamnative/kop/issues/32
    public void startGroupCoordinator(BrokerService service) throws Exception {
        GroupConfig groupConfig = new GroupConfig(
            kafkaConfig.getGroupMinSessionTimeoutMs(),
            kafkaConfig.getGroupMaxSessionTimeoutMs(),
            kafkaConfig.getGroupInitialRebalanceDelayMs()
        );

        OffsetConfig offsetConfig = OffsetConfig.builder()
            .offsetsTopicCompressionType(CompressionType.valueOf(kafkaConfig.getOffsetsTopicCompressionCodec()))
            .maxMetadataSize(kafkaConfig.getOffsetMetadataMaxSize())
            .offsetsRetentionCheckIntervalMs(kafkaConfig.getOffsetsRetentionCheckIntervalMs())
            .offsetsRetentionMs(TimeUnit.MINUTES.toMillis(kafkaConfig.getOffsetsRetentionMinutes()))
            .build();

        createKafkaMetadataNamespaceIfNeeded(service);
        String offsetsTopic = createKafkaOffsetsTopic(service);

        TopicName offsetsTopicName = TopicName.get(offsetsTopic);
        String offsetsTopicPtn0 = offsetsTopicName.getPartition(0).toString();

        Producer<ByteBuffer> groupCoordinatorTopicProducer = service.pulsar().getClient().newProducer(Schema.BYTEBUFFER)
            .topic(offsetsTopicPtn0)
            // TODO: make it configurable
            .maxPendingMessages(100000)
            .create();
        Reader<ByteBuffer> groupCoordinatorTopicReader = service.pulsar().getClient().newReader(Schema.BYTEBUFFER)
            .topic(offsetsTopicPtn0)
            .startMessageId(MessageId.earliest)
            .create();
        this.groupCoordinator = GroupCoordinator.of(
            groupCoordinatorTopicProducer,
            groupCoordinatorTopicReader,
            groupConfig,
            offsetConfig,
            SystemTimer.builder()
                .executorName("group-coordinator-timer")
                .build(),
            Time.SYSTEM
        );

        this.groupCoordinator.startup(false);
    }

    private void createKafkaMetadataNamespaceIfNeeded(BrokerService service) throws PulsarServerException, PulsarAdminException {
        String cluster = kafkaConfig.getClusterName();
        String kafkaMetadataTenant = kafkaConfig.getKafkaMetadataTenant();
        String kafkaMetadataNamespace = kafkaMetadataTenant + "/" + kafkaConfig.getKafkaMetadataNamespace();

        try {
            ClusterData clusterData = new ClusterData(service.pulsar().getWebServiceAddress(),
                null /* serviceUrlTls */,
                service.pulsar().getBrokerServiceUrl(),
                null /* brokerServiceUrlTls */);
            if (!service.pulsar().getAdminClient().clusters().getClusters().contains(cluster)) {
                service.pulsar().getAdminClient().clusters().createCluster(cluster, clusterData);
            } else {
                service.pulsar().getAdminClient().clusters().updateCluster(cluster, clusterData);
            }

            if (!service.pulsar().getAdminClient().tenants().getTenants().contains(kafkaMetadataTenant)) {
                service.pulsar().getAdminClient().tenants().createTenant(kafkaMetadataTenant,
                    new TenantInfo(Sets.newHashSet(kafkaConfig.getSuperUserRoles()), Sets.newHashSet(cluster)));
            }
            if (!service.pulsar().getAdminClient().namespaces().getNamespaces(kafkaMetadataTenant).contains(kafkaMetadataNamespace)) {
                Set<String> clusters = Sets.newHashSet(kafkaConfig.getClusterName());
                service.pulsar().getAdminClient().namespaces().createNamespace(kafkaMetadataNamespace, clusters);
                service.pulsar().getAdminClient().namespaces().setNamespaceReplicationClusters(kafkaMetadataNamespace, clusters);
                service.pulsar().getAdminClient().namespaces().setRetention(kafkaMetadataNamespace,
                    new RetentionPolicies(-1, -1));
            }
        } catch (PulsarAdminException e) {
            log.error("Failed to get retention policy for kafka metadata namespace {}",
                kafkaMetadataNamespace, e);
            throw e;
        }
    }

    private String createKafkaOffsetsTopic(BrokerService service) throws PulsarServerException, PulsarAdminException {
        String offsetsTopic = kafkaConfig.getKafkaTenant() + "/" + kafkaConfig.getKafkaMetadataNamespace()
            + "/" + Topic.GROUP_METADATA_TOPIC_NAME;

        PartitionedTopicMetadata offsetsTopicMetadata =
            service.pulsar().getAdminClient().topics().getPartitionedTopicMetadata(offsetsTopic);
        if (offsetsTopicMetadata.partitions <= 0) {
            log.info("Kafka group metadata topic {} doesn't exist. Creating it ...",
                offsetsTopic);
            service.pulsar().getAdminClient().topics().createPartitionedTopic(
                offsetsTopic,
                KafkaServiceConfiguration.DefaultOffsetsTopicNumPartitions
            );
            log.info("Successfully created group metadata topic {}.", offsetsTopic);
        }

        return offsetsTopic;
    }
}
