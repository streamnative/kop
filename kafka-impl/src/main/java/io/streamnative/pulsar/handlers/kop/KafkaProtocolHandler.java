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
package io.streamnative.pulsar.handlers.kop;

import static com.google.common.base.Preconditions.checkState;
import static io.streamnative.pulsar.handlers.kop.KopServerStats.SERVER_SCOPE;
import static io.streamnative.pulsar.handlers.kop.utils.TopicNameUtils.getKafkaTopicNameFromPulsarTopicname;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupConfig;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.group.OffsetConfig;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionConfig;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.stats.PrometheusMetricsProvider;
import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import io.streamnative.pulsar.handlers.kop.utils.ConfigurationUtils;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.MetadataUtils;
import io.streamnative.pulsar.handlers.kop.utils.ZooKeeperUtils;
import io.streamnative.pulsar.handlers.kop.utils.timer.SystemTimer;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.Lookup;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;

/**
 * Kafka Protocol Handler load and run by Pulsar Service.
 */
@Slf4j
public class KafkaProtocolHandler implements ProtocolHandler {

    public static final String PROTOCOL_NAME = "kafka";
    public static final String TLS_HANDLER = "tls";
    private static final Map<PulsarService, LookupClient> LOOKUP_CLIENT_MAP = new ConcurrentHashMap<>();

    private StatsLogger rootStatsLogger;
    private PrometheusMetricsProvider statsProvider;
    private KopBrokerLookupManager kopBrokerLookupManager;
    private AdminManager adminManager = null;
    private MetadataCache<LocalBrokerData> localBrokerDataCache;

    @Getter
    private KafkaServiceConfiguration kafkaConfig;
    @Getter
    private BrokerService brokerService;
    @Getter
    private GroupCoordinator groupCoordinator;
    @Getter
    private TransactionCoordinator transactionCoordinator;

    /**
     * Listener for the changing of topic that stores offsets of consumer group.
     */
    public static class OffsetAndTopicListener implements NamespaceBundleOwnershipListener {

        final BrokerService service;
        final NamespaceName kafkaMetaNs;
        final NamespaceName kafkaTopicNs;
        final GroupCoordinator groupCoordinator;
        final String brokerUrl;

        public OffsetAndTopicListener(BrokerService service,
                                   KafkaServiceConfiguration kafkaConfig,
                                   GroupCoordinator groupCoordinator) {
            this.service = service;
            this.kafkaMetaNs = NamespaceName
                .get(kafkaConfig.getKafkaMetadataTenant(), kafkaConfig.getKafkaMetadataNamespace());
            this.groupCoordinator = groupCoordinator;
            this.kafkaTopicNs = NamespaceName
                    .get(kafkaConfig.getKafkaTenant(), kafkaConfig.getKafkaNamespace());
            this.brokerUrl = service.pulsar().getBrokerServiceUrl();
        }

        @Override
        public void onLoad(NamespaceBundle bundle) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] onLoad bundle: {}", brokerUrl, bundle);
            }
            // 1. get new partitions owned by this pulsar service.
            // 2. load partitions by GroupCoordinator.handleGroupImmigration.
            service.pulsar().getNamespaceService().getOwnedTopicListForNamespaceBundle(bundle)
                .whenComplete((topics, ex) -> {
                    if (ex == null) {
                        log.info("get owned topic list when onLoad bundle {}, topic size {} ", bundle, topics.size());
                        for (String topic : topics) {
                            TopicName name = TopicName.get(topic);
                            // already filtered namespace, check the local name without partition
                            if (Topic.GROUP_METADATA_TOPIC_NAME.equals(getKafkaTopicNameFromPulsarTopicname(name))) {
                                checkState(name.isPartitioned(),
                                    "OffsetTopic should be partitioned in onLoad, but get " + name);

                                if (log.isDebugEnabled()) {
                                    log.debug("New offset partition load:  {}, broker: {}",
                                        name, service.pulsar().getBrokerServiceUrl());
                                }
                                groupCoordinator.handleGroupImmigration(name.getPartitionIndex());
                            }
                            // deReference topic when unload
                            KopBrokerLookupManager.removeTopicManagerCache(topic);
                            KafkaTopicManager.deReference(topic);

                            // For non-partitioned topic.
                            if (!name.isPartitioned()) {
                                String partitionedZeroTopicName = name.getPartition(0).toString();
                                KafkaTopicManager.deReference(partitionedZeroTopicName);
                                KopBrokerLookupManager.removeTopicManagerCache(partitionedZeroTopicName);
                            }
                        }
                    } else {
                        log.error("Failed to get owned topic list for "
                            + "OffsetAndTopicListener when triggering on-loading bundle {}.",
                            bundle, ex);
                    }
                });
        }

        @Override
        public void unLoad(NamespaceBundle bundle) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] unLoad bundle: {}", brokerUrl, bundle);
            }
            // 1. get partitions owned by this pulsar service.
            // 2. remove partitions by groupCoordinator.handleGroupEmigration.
            service.pulsar().getNamespaceService().getOwnedTopicListForNamespaceBundle(bundle)
                .whenComplete((topics, ex) -> {
                    if (ex == null) {
                        log.info("get owned topic list when unLoad bundle {}, topic size {} ", bundle, topics.size());
                        for (String topic : topics) {
                            TopicName name = TopicName.get(topic);

                            // already filtered namespace, check the local name without partition
                            if (Topic.GROUP_METADATA_TOPIC_NAME.equals(getKafkaTopicNameFromPulsarTopicname(name))) {
                                checkState(name.isPartitioned(),
                                    "OffsetTopic should be partitioned in unLoad, but get " + name);

                                if (log.isDebugEnabled()) {
                                    log.debug("Offset partition unload:  {}, broker: {}",
                                        name, service.pulsar().getBrokerServiceUrl());
                                }
                                groupCoordinator.handleGroupEmigration(name.getPartitionIndex());
                            }
                            // deReference topic when unload
                            KopBrokerLookupManager.removeTopicManagerCache(topic);
                            KafkaTopicManager.deReference(topic);

                            // For non-partitioned topic.
                            if (!name.isPartitioned()) {
                                String partitionedZeroTopicName = name.getPartition(0).toString();
                                KafkaTopicManager.deReference(partitionedZeroTopicName);
                                KopBrokerLookupManager.removeTopicManagerCache(partitionedZeroTopicName);
                            }

                        }
                    } else {
                        log.error("Failed to get owned topic list for "
                            + "OffsetAndTopicListener when triggering un-loading bundle {}.",
                            bundle, ex);
                    }
                });
        }

        // verify that this bundle is served by this broker,
        // and namespace is related to kafka metadata namespace
        @Override
        public boolean test(NamespaceBundle namespaceBundle) {
            return namespaceBundle.getNamespaceObject().equals(kafkaMetaNs)
                    || namespaceBundle.getNamespaceObject().equals(kafkaTopicNs);
        }

    }


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

            // some of the configs value in conf.properties may not updated.
            // So need to get latest value from conf itself
            kafkaConfig.setAdvertisedAddress(conf.getAdvertisedAddress());
            kafkaConfig.setBindAddress(conf.getBindAddress());
        }
        KopTopic.initialize(kafkaConfig.getKafkaTenant() + "/" + kafkaConfig.getKafkaNamespace());

        // Validate the namespaces
        for (String fullNamespace : kafkaConfig.getKopAllowedNamespaces()) {
            final String[] tokens = fullNamespace.split("/");
            if (tokens.length != 2) {
                throw new IllegalArgumentException(
                        "Invalid namespace '" + fullNamespace + "' in kopAllowedNamespaces config");
            }
            NamespaceName.validateNamespaceName(tokens[0], tokens[1]);
        }

        statsProvider = new PrometheusMetricsProvider();
        rootStatsLogger = statsProvider.getStatsLogger("");
    }

    // This method is called after initialize
    @Override
    public String getProtocolDataToAdvertise() {
        return kafkaConfig.getKafkaAdvertisedListeners();
    }

    @Override
    public void start(BrokerService service) {
        brokerService = service;
        kopBrokerLookupManager = new KopBrokerLookupManager(
                brokerService.getPulsar(), false, kafkaConfig.getKafkaAdvertisedListeners());

        log.info("Starting KafkaProtocolHandler, kop version is: '{}'", KopVersion.getVersion());
        log.info("Git Revision {}", KopVersion.getGitSha());
        log.info("Built by {} on {} at {}",
            KopVersion.getBuildUser(),
            KopVersion.getBuildHost(),
            KopVersion.getBuildTime());

        // Currently each time getMetadataCache() is called, a new MetadataCache<T> instance will be created, even for
        // the same type. So we must reuse the same MetadataCache<LocalBrokerData> to avoid creating a lot of instances.
        localBrokerDataCache = brokerService.pulsar().getLocalMetadataStore().getMetadataCache(LocalBrokerData.class);

        ZooKeeperUtils.tryCreatePath(brokerService.pulsar().getZkClient(),
                kafkaConfig.getGroupIdZooKeeperPath(), new byte[0]);

        PulsarAdmin pulsarAdmin;
        try {
            pulsarAdmin = brokerService.getPulsar().getAdminClient();
            adminManager = new AdminManager(pulsarAdmin, kafkaConfig);
        } catch (PulsarServerException e) {
            log.error("Failed to get pulsarAdmin", e);
            throw new IllegalStateException(e);
        }

        // Create PulsarClient for topic lookup, the listenerName will be set if kafkaListenerName is configured.
        // After it's created successfully, this method won't throw any exception.
        LOOKUP_CLIENT_MAP.put(brokerService.pulsar(), new LookupClient(brokerService.pulsar(), kafkaConfig));

        final ClusterData clusterData = ClusterData.builder()
                .serviceUrl(brokerService.getPulsar().getWebServiceAddress())
                .serviceUrlTls(brokerService.getPulsar().getWebServiceAddressTls())
                .brokerServiceUrl(brokerService.getPulsar().getBrokerServiceUrl())
                .brokerServiceUrlTls(brokerService.getPulsar().getBrokerServiceUrlTls())
                .build();

        // Use the builtin PulsarClient for creating producers and readers in group coordinator
        PulsarClient pulsarClient;
        try {
            pulsarClient = brokerService.getPulsar().getClient();
        } catch (PulsarServerException e) {
            log.error("Failed to create builtin PulsarClient", e);
            throw new IllegalStateException(e);
        }
        try {
            MetadataUtils.createOffsetMetadataIfMissing(pulsarAdmin, clusterData, kafkaConfig);
        } catch (PulsarAdminException e) {
            log.error("Failed to create offset metadata", e);
            throw new IllegalStateException(e);
        }

        // init and start group coordinator
        startGroupCoordinator(pulsarClient);
        // and listener for Offset topics load/unload
        brokerService.pulsar()
                .getNamespaceService()
                .addNamespaceBundleOwnershipListener(
                        new OffsetAndTopicListener(brokerService, kafkaConfig, groupCoordinator));

        // init kafka namespaces
        try {
            MetadataUtils.createKafkaNamespaceIfMissing(pulsarAdmin, clusterData, kafkaConfig);
        } catch (PulsarAdminException e) {
            // no need to throw exception since we can create kafka namespace later
            log.warn("init kafka failed, need to create it manually later", e);
        }

        if (kafkaConfig.isEnableTransactionCoordinator()) {
            try {
                initTransactionCoordinator(pulsarAdmin, clusterData);
                startTransactionCoordinator();
            } catch (Exception e) {
                log.error("Initialized transaction coordinator failed.", e);
                throw new IllegalStateException(e);
            }
        }

        Configuration conf = new PropertiesConfiguration();
        conf.addProperty("prometheusStatsLatencyRolloverSeconds",
            kafkaConfig.getKopPrometheusStatsLatencyRolloverSeconds());
        statsProvider.start(conf);
        brokerService.pulsar().addPrometheusRawMetricsProvider(statsProvider);
    }

    // this is called after initialize, and with kafkaConfig, brokerService all set.
    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkState(kafkaConfig != null);
        checkState(brokerService != null);

        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                ImmutableMap.<InetSocketAddress, ChannelInitializer<SocketChannel>>builder();

            final Map<SecurityProtocol, EndPoint> advertisedEndpointMap =
                    EndPoint.parseListeners(kafkaConfig.getKafkaAdvertisedListeners());
            EndPoint.parseListeners(kafkaConfig.getListeners()).forEach((protocol, endPoint) -> {
                EndPoint advertisedEndPoint = advertisedEndpointMap.get(protocol);
                if (advertisedEndPoint == null) {
                    // Use the bind endpoint as the advertised endpoint.
                    advertisedEndPoint = endPoint;
                }
                switch (protocol) {
                    case PLAINTEXT:
                    case SASL_PLAINTEXT:
                        builder.put(endPoint.getInetAddress(), new KafkaChannelInitializer(brokerService.getPulsar(),
                                kafkaConfig, groupCoordinator, transactionCoordinator, adminManager, false,
                                advertisedEndPoint, rootStatsLogger.scope(SERVER_SCOPE), localBrokerDataCache));
                        break;
                    case SSL:
                    case SASL_SSL:
                        builder.put(endPoint.getInetAddress(), new KafkaChannelInitializer(brokerService.getPulsar(),
                                kafkaConfig, groupCoordinator, transactionCoordinator, adminManager, true,
                                advertisedEndPoint, rootStatsLogger.scope(SERVER_SCOPE), localBrokerDataCache));
                        break;
                }
            });
            return builder.build();
        } catch (Exception e){
            log.error("KafkaProtocolHandler newChannelInitializers failed with ", e);
            return null;
        }
    }

    @Override
    public void close() {
        Optional.ofNullable(LOOKUP_CLIENT_MAP.remove(brokerService.pulsar())).ifPresent(LookupClient::close);
        adminManager.shutdown();
        groupCoordinator.shutdown();
        KafkaTopicManager.LOOKUP_CACHE.clear();
        KopBrokerLookupManager.clear();
        KafkaTopicManager.closeKafkaTopicConsumerManagers();
        KafkaTopicManager.getReferences().clear();
        KafkaTopicManager.getTopics().clear();
        statsProvider.stop();
    }

    public void startGroupCoordinator(PulsarClient pulsarClient) {
        GroupConfig groupConfig = new GroupConfig(
            kafkaConfig.getGroupMinSessionTimeoutMs(),
            kafkaConfig.getGroupMaxSessionTimeoutMs(),
            kafkaConfig.getGroupInitialRebalanceDelayMs()
        );

        OffsetConfig offsetConfig = OffsetConfig.builder()
            .offsetsTopicName(kafkaConfig.getKafkaMetadataTenant() + "/"
                + kafkaConfig.getKafkaMetadataNamespace()
                + "/" + Topic.GROUP_METADATA_TOPIC_NAME)
            .offsetsTopicNumPartitions(kafkaConfig.getOffsetsTopicNumPartitions())
            .offsetsTopicCompressionType(CompressionType.valueOf(kafkaConfig.getOffsetsTopicCompressionCodec()))
            .maxMetadataSize(kafkaConfig.getOffsetMetadataMaxSize())
            .offsetsRetentionCheckIntervalMs(kafkaConfig.getOffsetsRetentionCheckIntervalMs())
            .offsetsRetentionMs(TimeUnit.MINUTES.toMillis(kafkaConfig.getOffsetsRetentionMinutes()))
            .build();

        this.groupCoordinator = GroupCoordinator.of(
            (PulsarClientImpl) pulsarClient,
            groupConfig,
            offsetConfig,
            SystemTimer.builder()
                .executorName("group-coordinator-timer")
                .build(),
            Time.SYSTEM
        );
        // always enable metadata expiration
        this.groupCoordinator.startup(true);
    }

    public void initTransactionCoordinator(PulsarAdmin pulsarAdmin, ClusterData clusterData) throws Exception {
        TransactionConfig transactionConfig = TransactionConfig.builder()
                .transactionLogNumPartitions(kafkaConfig.getTxnLogTopicNumPartitions())
                .transactionMetadataTopicName(MetadataUtils.constructTxnLogTopicBaseName(kafkaConfig))
                .build();

        MetadataUtils.createTxnMetadataIfMissing(pulsarAdmin, clusterData, kafkaConfig);

        this.transactionCoordinator = TransactionCoordinator.of(
                transactionConfig,
                kafkaConfig.getBrokerId(),
                brokerService.getPulsar().getZkClient(),
                kopBrokerLookupManager);

        loadTxnLogTopics(transactionCoordinator);
    }

    public void startTransactionCoordinator() throws Exception {
        if (this.transactionCoordinator != null) {
            this.transactionCoordinator.startup().get();
        } else {
            log.error("Failed to start transaction coordinator. Need init it first.");
        }
    }

    /**
     * This method discovers ownership of offset topic partitions and attempts to load offset topics
     * assigned to this broker.
     */
    private void loadTxnLogTopics(TransactionCoordinator txnCoordinator) throws Exception {
        Lookup lookupService = brokerService.pulsar().getAdminClient().lookups();
        String currentBroker = brokerService.pulsar().getBrokerServiceUrl();
        String topicBase = MetadataUtils.constructTxnLogTopicBaseName(kafkaConfig);
        int numPartitions = kafkaConfig.getTxnLogTopicNumPartitions();

        Map<String, List<Integer>> mapBrokerToPartition = new HashMap<>();

        for (int i = 0; i < numPartitions; i++) {
            String broker = lookupService.lookupTopic(topicBase + PARTITIONED_TOPIC_SUFFIX + i);
            mapBrokerToPartition.putIfAbsent(broker, new ArrayList<>());
            mapBrokerToPartition.get(broker).add(i);
        }

        mapBrokerToPartition.forEach(
                (key, value) -> log.info("Discovered broker: {} owns txn log topic partitions: {} ", key, value));

        List<Integer> partitionsOwnedByCurrentBroker = mapBrokerToPartition.get(currentBroker);

        if (null != partitionsOwnedByCurrentBroker && !partitionsOwnedByCurrentBroker.isEmpty()) {
            List<CompletableFuture<Void>> lists = partitionsOwnedByCurrentBroker.stream().map(
                (partition) -> txnCoordinator.loadTransactionMetadata(partition)).collect(Collectors.toList());

            FutureUtil.waitForAll(lists).get();
        } else {
            log.info("Current broker: {} does not own any of the txn log topic partitions", currentBroker);
        }
    }

    public static @NonNull LookupClient getLookupClient(final PulsarService pulsarService) {
        return LOOKUP_CLIENT_MAP.computeIfAbsent(pulsarService, ignored -> new LookupClient(pulsarService));
    }
}
