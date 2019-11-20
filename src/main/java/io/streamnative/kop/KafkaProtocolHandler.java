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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.kop.coordinator.group.GroupConfig;
import io.streamnative.kop.coordinator.group.GroupCoordinator;
import io.streamnative.kop.coordinator.group.OffsetConfig;
import io.streamnative.kop.utils.ConfigurationUtils;
import io.streamnative.kop.utils.timer.SystemTimer;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdmin;
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
    public static final String SSL_PREFIX = "SSL://";
    public static final String PLAINTEXT_PREFIX = "PLAINTEXT://";
    public static final String LISTENER_DEL = ",";
    public static final String TLS_HANDLER = "tls";
    public static final String LISTENER_PATTEN = "^(PLAINTEXT?|SSL)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-0-9+]";

    /**
     * Kafka Listener Type.
     */
    public enum ListenerType {
        PLAINTEXT,
        SSL
    }

    @Getter
    private KafkaServiceConfiguration kafkaConfig;
    @Getter
    private BrokerService brokerService;
    @Getter
    private KafkaTopicManager kafkaTopicManager;
    @Getter
    private GroupCoordinator groupCoordinator;
    @Getter
    private String bindAddress;


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
        // init config
        if (conf instanceof KafkaServiceConfiguration) {
            // in unit test, passed in conf will be KafkaServiceConfiguration
            kafkaConfig = (KafkaServiceConfiguration) conf;
        } else {
            // when loaded with PulsarService as NAR, `conf` will be type of ServiceConfiguration
            kafkaConfig = ConfigurationUtils.create(conf.getProperties(), KafkaServiceConfiguration.class);
        }
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(kafkaConfig.getBindAddress());
    }

    // This method is called after initialize
    @Override
    public String getProtocolDataToAdvertise() {
        if (log.isDebugEnabled()) {
            log.debug("Get configured listeners", kafkaConfig.getListeners());
        }
        return kafkaConfig.getListeners();
    }

    @Override
    public void start(BrokerService service) {
        brokerService = service;

        // a topic Manager
        kafkaTopicManager = new KafkaTopicManager(service);

        // init and start group coordinator
        if (kafkaConfig.isEnableGroupCoordinator()) {
            try {
                initGroupCoordinator(brokerService);
                startGroupCoordinator();
            } catch (Exception e) {
                log.error("initGroupCoordinator failed with", e);
            }
        }
    }

    // this is called after initialize, and with kafkaTopicManager, kafkaConfig, brokerService all set.
    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkState(kafkaConfig != null);
        checkState(kafkaConfig.getListeners() != null);
        checkState(brokerService != null);
        checkState(kafkaTopicManager != null);
        if (kafkaConfig.isEnableGroupCoordinator()) {
            checkState(groupCoordinator != null);
        }

        String listeners = kafkaConfig.getListeners();
        String[] parts = listeners.split(LISTENER_DEL);

        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                ImmutableMap.<InetSocketAddress, ChannelInitializer<SocketChannel>>builder();

            for (String listener: parts) {
                if (listener.startsWith(PLAINTEXT_PREFIX)) {
                    builder.put(
                        // TODO: consider using the address in the listener as the bind address.
                        //          https://github.com/streamnative/kop/issues/46
                        new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                        new KafkaChannelInitializer(brokerService.pulsar(),
                            kafkaConfig,
                            kafkaTopicManager,
                            groupCoordinator,
                            false));
                } else if (listener.startsWith(SSL_PREFIX)) {
                    builder.put(
                        new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                        new KafkaChannelInitializer(brokerService.pulsar(),
                            kafkaConfig,
                            kafkaTopicManager,
                            groupCoordinator,
                            true));
                } else {
                    log.error("Kafka listener {} not supported. supports {} and {}",
                        listener, PLAINTEXT_PREFIX, SSL_PREFIX);
                }
            }

            return builder.build();
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

    public void initGroupCoordinator(BrokerService service) throws Exception {
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
    }

    // TODO: make group coordinator running in a distributed mode
    //      https://github.com/streamnative/kop/issues/32
    public void startGroupCoordinator() throws Exception {
        if (this.groupCoordinator != null) {
            this.groupCoordinator.startup(false);
        } else {
            log.error("Failed to start group coordinator. Need init it first.");
        }
    }

    private void createKafkaMetadataNamespaceIfNeeded(BrokerService service)
        throws PulsarServerException, PulsarAdminException {
        String cluster = kafkaConfig.getClusterName();
        String kafkaMetadataTenant = kafkaConfig.getKafkaMetadataTenant();
        String kafkaMetadataNamespace = kafkaMetadataTenant + "/" + kafkaConfig.getKafkaMetadataNamespace();
        PulsarAdmin pulsarAdmin = service.pulsar().getAdminClient();

        try {
            ClusterData clusterData = new ClusterData(service.pulsar().getWebServiceAddress(),
                null /* serviceUrlTls */,
                service.pulsar().getBrokerServiceUrl(),
                null /* brokerServiceUrlTls */);
            if (!pulsarAdmin.clusters().getClusters().contains(cluster)) {
                pulsarAdmin.clusters().createCluster(cluster, clusterData);
            } else {
                pulsarAdmin.clusters().updateCluster(cluster, clusterData);
            }

            if (!pulsarAdmin.tenants().getTenants().contains(kafkaMetadataTenant)) {
                pulsarAdmin.tenants().createTenant(kafkaMetadataTenant,
                    new TenantInfo(Sets.newHashSet(kafkaConfig.getSuperUserRoles()), Sets.newHashSet(cluster)));
            }
            if (!pulsarAdmin.namespaces().getNamespaces(kafkaMetadataTenant).contains(kafkaMetadataNamespace)) {
                Set<String> clusters = Sets.newHashSet(kafkaConfig.getClusterName());
                pulsarAdmin.namespaces().createNamespace(kafkaMetadataNamespace, clusters);
                pulsarAdmin.namespaces().setNamespaceReplicationClusters(kafkaMetadataNamespace, clusters);
                pulsarAdmin.namespaces().setRetention(kafkaMetadataNamespace,
                    new RetentionPolicies(-1, -1));
            }
        } catch (PulsarAdminException e) {
            log.error("Failed to get retention policy for kafka metadata namespace {}",
                kafkaMetadataNamespace, e);
            throw e;
        }
    }

    private String createKafkaOffsetsTopic(BrokerService service) throws PulsarServerException, PulsarAdminException {
        String offsetsTopic = kafkaConfig.getKafkaMetadataTenant() + "/" + kafkaConfig.getKafkaMetadataNamespace()
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

    public static int getListenerPort(String listener) {
        checkState(listener.matches(LISTENER_PATTEN), "listener not match patten");

        int lastIndex = listener.lastIndexOf(':');
        return Integer.parseInt(listener.substring(lastIndex + 1));
    }

    public static int getListenerPort(String listeners, ListenerType type) {
        String[] parts = listeners.split(LISTENER_DEL);

        for (String listener: parts) {
            if (type == ListenerType.PLAINTEXT && listener.startsWith(PLAINTEXT_PREFIX)) {
                return getListenerPort(listener);
            }
            if (type == ListenerType.SSL && listener.startsWith(SSL_PREFIX)) {
                return getListenerPort(listener);
            }
        }

        log.error("KafkaProtocolHandler listeners {} not contains type {}", listeners, type);
        return -1;
    }

    public static String getBrokerUrl(String listeners, Boolean tlsEnabled) {
        String[] parts = listeners.split(LISTENER_DEL);

        for (String listener: parts) {
            if (tlsEnabled && listener.startsWith(SSL_PREFIX)) {
                return listener;
            }
            if (!tlsEnabled && listener.startsWith(PLAINTEXT_PREFIX)) {
                return listener;
            }
        }

        log.error("listener {} not contains a valid SSL or PLAINTEXT address", listeners);
        return null;
    }
}
