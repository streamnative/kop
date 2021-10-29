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
import static io.streamnative.pulsar.handlers.kop.utils.TopicNameUtils.getKafkaTopicNameFromPulsarTopicName;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.annotations.VisibleForTesting;
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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.CompressionType;
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
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Kafka Protocol Handler load and run by Pulsar Service.
 */
@Slf4j
public class KafkaProtocolHandler implements ProtocolHandler, TenantContextManager {

    public static final String PROTOCOL_NAME = "kafka";
    public static final String TLS_HANDLER = "tls";
    private static final Map<PulsarService, LookupClient> LOOKUP_CLIENT_MAP = new ConcurrentHashMap<>();

    private StatsLogger rootStatsLogger;
    private StatsLogger scopeStatsLogger;
    private PrometheusMetricsProvider statsProvider;
    @Getter
    private KopBrokerLookupManager kopBrokerLookupManager;
    private AdminManager adminManager = null;
    private SystemTopicClient txnTopicClient;
    @VisibleForTesting
    @Getter
    private Map<InetSocketAddress, ChannelInitializer<SocketChannel>> channelInitializerMap;

    @Getter
    @VisibleForTesting
    protected SystemTopicClient offsetTopicClient;

    @Getter
    private KafkaServiceConfiguration kafkaConfig;
    @Getter
    private BrokerService brokerService;
    @Getter
    private KopEventManager kopEventManager;

    private final Map<String, GroupCoordinator> groupCoordinatorsByTenant = new ConcurrentHashMap<>();
    private final Map<String, TransactionCoordinator> transactionCoordinatorByTenant = new ConcurrentHashMap<>();

    @Override
    public GroupCoordinator getGroupCoordinator(String tenant) {
        return groupCoordinatorsByTenant.computeIfAbsent(tenant, this::createAndBootGroupCoordinator);
    }

    @VisibleForTesting
    public Map<String, GroupCoordinator> getGroupCoordinators() {
        return groupCoordinatorsByTenant;
    }

    @Override
    public TransactionCoordinator getTransactionCoordinator(String tenant) {
        return transactionCoordinatorByTenant.computeIfAbsent(tenant, this::createAndBootTransactionCoordinator);
    }

    /**
     * Listener for invalidating the global Broker ownership cache.
     */
    @AllArgsConstructor
    public static class CacheInvalidator implements NamespaceBundleOwnershipListener {
        final BrokerService service;

        @Override
        public boolean test(NamespaceBundle namespaceBundle) {
            // we are interested in every topic,
            // because we do not know which topics are served by KOP
            return true;
        }

        private void invalidateBundleCache(NamespaceBundle bundle) {
            log.info("invalidateBundleCache for namespaceBundle {}", bundle);
            service.pulsar().getNamespaceService().getOwnedTopicListForNamespaceBundle(bundle)
                    .whenComplete((topics, ex) -> {
                        if (ex == null) {
                            for (String topic : topics) {
                                TopicName name = TopicName.get(topic);

                                if (log.isDebugEnabled()) {
                                    log.debug("invalidateBundleCache for topic {}", topic);
                                }

                                KafkaTopicManager.deReference(topic);

                                // For non-partitioned topic.
                                if (!name.isPartitioned()) {
                                    String partitionedZeroTopicName = name.getPartition(0).toString();
                                    KafkaTopicManager.deReference(partitionedZeroTopicName);
                                }
                            }
                        } else {
                            log.error("Failed to get owned topic list for "
                                            + "CacheInvalidator when triggering bundle ownership change {}.",
                                    bundle, ex);
                        }
                    }
                    );
        }
        @Override
        public void onLoad(NamespaceBundle bundle) {
            invalidateBundleCache(bundle);
        }
        @Override
        public void unLoad(NamespaceBundle bundle) {
            invalidateBundleCache(bundle);
        }
    }

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
                                      String tenant,
                                      KafkaServiceConfiguration kafkaConfig,
                                      GroupCoordinator groupCoordinator) {
            this.service = service;
            this.kafkaMetaNs = NamespaceName
                .get(tenant, kafkaConfig.getKafkaMetadataNamespace());
            this.groupCoordinator = groupCoordinator;
            this.kafkaTopicNs = NamespaceName
                    .get(tenant, kafkaConfig.getKafkaNamespace());
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
                            String kafkaTopicName = getKafkaTopicNameFromPulsarTopicName(name);

                            // already filtered namespace, check the local name without partition
                            if (Topic.GROUP_METADATA_TOPIC_NAME.equals(kafkaTopicName)) {
                                checkState(name.isPartitioned(),
                                    "OffsetTopic should be partitioned in onLoad, but get " + name);

                                if (log.isDebugEnabled()) {
                                    log.debug("New offset partition load:  {}, broker: {}",
                                        name, service.pulsar().getBrokerServiceUrl());
                                }
                                groupCoordinator.handleGroupImmigration(name.getPartitionIndex());
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
                            String kafkaTopicName = getKafkaTopicNameFromPulsarTopicName(name);

                            // already filtered namespace, check the local name without partition
                            if (Topic.GROUP_METADATA_TOPIC_NAME.equals(kafkaTopicName)) {
                                checkState(name.isPartitioned(),
                                    "OffsetTopic should be partitioned in unLoad, but get " + name);

                                if (log.isDebugEnabled()) {
                                    log.debug("Offset partition unload:  {}, broker: {}",
                                        name, service.pulsar().getBrokerServiceUrl());
                                }
                                groupCoordinator.handleGroupEmigration(name.getPartitionIndex());
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

    /**
     * Listener for the changing of transaction topic when namespace bundle load or unload.
     */
    public static class TransactionStateRecover implements NamespaceBundleOwnershipListener {
        private final BrokerService service;
        private final NamespaceName kafkaMetaNs;
        private final NamespaceName kafkaTopicNs;
        private final String brokerUrl;
        private final TransactionCoordinator txnCoordinator;

        public TransactionStateRecover(
                BrokerService service,
                String tenant,
                KafkaServiceConfiguration kafkaConfig,
                TransactionCoordinator txnCoordinator) {
            this.service = service;
            this.kafkaMetaNs = NamespaceName
                    .get(tenant, kafkaConfig.getKafkaMetadataNamespace());
            this.kafkaTopicNs = NamespaceName
                    .get(tenant, kafkaConfig.getKafkaNamespace());
            this.brokerUrl = service.pulsar().getBrokerServiceUrl();
            this.txnCoordinator = txnCoordinator;
        }

        @Override
        public void onLoad(NamespaceBundle bundle) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] onLoad bundle: {}", brokerUrl, bundle);
            }
            // 1. get new partitions owned by this pulsar service.
            // 2. load partitions by TransactionCoordinator.handleTxnImmigration.
            service.pulsar().getNamespaceService().getOwnedTopicListForNamespaceBundle(bundle)
                    .whenComplete((topics, ex) -> {
                        if (ex == null) {
                            log.info("get owned topic list when onLoad bundle {}, topic size {} ",
                                    bundle, topics.size());
                            for (String topic : topics) {
                                TopicName name = TopicName.get(topic);
                                String kafkaTopicName = getKafkaTopicNameFromPulsarTopicName(name);

                                if (Topic.TRANSACTION_STATE_TOPIC_NAME.equals(kafkaTopicName)) {
                                    checkState(name.isPartitioned(),
                                            "TxnTopic should be partitioned in onLoad, but get " + name);

                                    if (log.isDebugEnabled()) {
                                        log.debug("New transaction partition load:  {}, broker: {}",
                                                name, service.pulsar().getBrokerServiceUrl());
                                    }
                                    txnCoordinator.handleTxnImmigration(name.getPartitionIndex());
                                }
                            }
                        } else {
                            log.error("Failed to get owned topic list for "
                                            + "TransactionStateRecover when triggering on-loading bundle {}.",
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
                            log.info("get owned topic list when unLoad bundle {}, topic size {} ",
                                    bundle, topics.size());
                            for (String topic : topics) {
                                TopicName name = TopicName.get(topic);
                                String kafkaTopicName = getKafkaTopicNameFromPulsarTopicName(name);

                                // Filter TRANSACTION_STATE_TOPIC
                                if (Topic.TRANSACTION_STATE_TOPIC_NAME.equals(kafkaTopicName)
                                        && txnCoordinator != null) {
                                    checkState(name.isPartitioned(),
                                            "TxnTopic should be partitioned in unLoad, but get " + name);

                                    if (log.isDebugEnabled()) {
                                        log.debug("Txn partition unload: {}, broker: {}",
                                                name, service.pulsar().getBrokerServiceUrl());
                                    }
                                    txnCoordinator.handleTxnEmigration(name.getPartitionIndex());
                                }
                            }
                        } else {
                            log.error("Failed to get owned topic list for "
                                            + "TransactionStateRecover when triggering un-loading bundle {}.",
                                    bundle, ex);
                        }
                    });
        }

        // Verify that this bundle is served by this broker,
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
            NamespaceName.validateNamespaceName(
                    tokens[0].replace(KafkaServiceConfiguration.TENANT_PLACEHOLDER, kafkaConfig.getKafkaTenant()),
                    tokens[1].replace("*", kafkaConfig.getKafkaNamespace()));
        }

        statsProvider = new PrometheusMetricsProvider();
        rootStatsLogger = statsProvider.getStatsLogger("");
        scopeStatsLogger = rootStatsLogger.scope(SERVER_SCOPE);
    }

    // This method is called after initialize
    @Override
    public String getProtocolDataToAdvertise() {
        return kafkaConfig.getKafkaAdvertisedListeners();
    }

    @Override
    public void start(BrokerService service) {
        log.info("Starting KafkaProtocolHandler, kop version is: '{}'", KopVersion.getVersion());
        log.info("Git Revision {}", KopVersion.getGitSha());
        log.info("Built by {} on {} at {}",
            KopVersion.getBuildUser(),
            KopVersion.getBuildHost(),
            KopVersion.getBuildTime());

        brokerService = service;
        PulsarAdmin pulsarAdmin;
        try {
            pulsarAdmin = brokerService.getPulsar().getAdminClient();
            adminManager = new AdminManager(pulsarAdmin, kafkaConfig);
        } catch (PulsarServerException e) {
            log.error("Failed to get pulsarAdmin", e);
            throw new IllegalStateException(e);
        }

        LOOKUP_CLIENT_MAP.put(brokerService.pulsar(), new LookupClient(brokerService.pulsar(), kafkaConfig));
        offsetTopicClient = new SystemTopicClient(brokerService.pulsar(), kafkaConfig);
        txnTopicClient = new SystemTopicClient(brokerService.pulsar(), kafkaConfig);

        try {
            kopBrokerLookupManager = new KopBrokerLookupManager(kafkaConfig, brokerService.getPulsar());
        } catch (Exception ex) {
            log.error("Failed to get kopBrokerLookupManager", ex);
            throw new IllegalStateException(ex);
        }

        brokerService.pulsar()
                .getNamespaceService()
                .addNamespaceBundleOwnershipListener(
                        new CacheInvalidator(brokerService));

        // initialize default Group Coordinator
        getGroupCoordinator(kafkaConfig.getKafkaMetadataTenant());

        // init KopEventManager
        kopEventManager = new KopEventManager(adminManager,
                brokerService.getPulsar().getLocalMetadataStore(),
                scopeStatsLogger,
                groupCoordinatorsByTenant);
        kopEventManager.start();

        if (kafkaConfig.isEnableTransactionCoordinator()) {
            TransactionCoordinator transactionCoordinator =
                    getTransactionCoordinator(kafkaConfig.getKafkaMetadataTenant());
            try {
                loadTxnLogTopics(kafkaConfig.getKafkaMetadataTenant(), transactionCoordinator);
            } catch (Exception e) {
                log.error("Failed to load transaction log", e);
            }
        }

        Configuration conf = new PropertiesConfiguration();
        conf.addProperty("prometheusStatsLatencyRolloverSeconds",
            kafkaConfig.getKopPrometheusStatsLatencyRolloverSeconds());
        statsProvider.start(conf);
        brokerService.pulsar().addPrometheusRawMetricsProvider(statsProvider);
    }

    private TransactionCoordinator createAndBootTransactionCoordinator(String tenant) {
        log.info("createAndBootTransactionCoordinator {}", tenant);
        final ClusterData clusterData = ClusterData.builder()
                .serviceUrl(brokerService.getPulsar().getWebServiceAddress())
                .serviceUrlTls(brokerService.getPulsar().getWebServiceAddressTls())
                .brokerServiceUrl(brokerService.getPulsar().getBrokerServiceUrl())
                .brokerServiceUrlTls(brokerService.getPulsar().getBrokerServiceUrlTls())
                .build();

        try {
            TransactionCoordinator transactionCoordinator =
                    initTransactionCoordinator(tenant, brokerService.getPulsar().getAdminClient(), clusterData);
            // Listening transaction topic load/unload
            brokerService.pulsar()
                    .getNamespaceService()
                    .addNamespaceBundleOwnershipListener(
                            new TransactionStateRecover(brokerService, tenant, kafkaConfig, transactionCoordinator));

            return transactionCoordinator;
        } catch (Exception e) {
            log.error("Initialized transaction coordinator failed.", e);
            throw new IllegalStateException(e);
        }
    }

    private GroupCoordinator createAndBootGroupCoordinator(String tenant) {
        log.info("createAndBootGroupCoordinator {}", tenant);
        final ClusterData clusterData = ClusterData.builder()
                .serviceUrl(brokerService.getPulsar().getWebServiceAddress())
                .serviceUrlTls(brokerService.getPulsar().getWebServiceAddressTls())
                .brokerServiceUrl(brokerService.getPulsar().getBrokerServiceUrl())
                .brokerServiceUrlTls(brokerService.getPulsar().getBrokerServiceUrlTls())
                .build();

        GroupCoordinator groupCoordinator;
        try {
            MetadataUtils.createOffsetMetadataIfMissing(tenant, brokerService.getPulsar().getAdminClient(),
                    clusterData, kafkaConfig);

            // init and start group coordinator
            groupCoordinator = startGroupCoordinator(tenant, offsetTopicClient);

            // and listener for Offset topics load/unload
            brokerService.pulsar()
                    .getNamespaceService()
                    .addNamespaceBundleOwnershipListener(
                            new OffsetAndTopicListener(brokerService, tenant, kafkaConfig, groupCoordinator));
        } catch (Exception e) {
            log.error("Failed to create offset metadata", e);
            throw new IllegalStateException(e);
        }

        // init kafka namespaces
        try {
            MetadataUtils.createKafkaNamespaceIfMissing(brokerService.getPulsar().getAdminClient(),
                    clusterData, kafkaConfig);
        } catch (Exception e) {
            // no need to throw exception since we can create kafka namespace later
            log.warn("init kafka failed, need to create it manually later", e);
        }

        return groupCoordinator;
    }

    private KafkaChannelInitializer newKafkaChannelInitializer(final EndPoint endPoint) {
        return new KafkaChannelInitializer(
                brokerService.getPulsar(),
                kafkaConfig,
                this,
                kopBrokerLookupManager,
                adminManager,
                endPoint.isTlsEnabled(),
                endPoint,
                scopeStatsLogger);
    }

    // this is called after initialize, and with kafkaConfig, brokerService all set.
    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkState(kafkaConfig != null);
        checkState(brokerService != null);

        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                    ImmutableMap.builder();

            EndPoint.parseListeners(kafkaConfig.getListeners(), kafkaConfig.getKafkaProtocolMap()).
                    forEach((listener, endPoint) ->
                            builder.put(endPoint.getInetAddress(), newKafkaChannelInitializer(endPoint))
                    );
            channelInitializerMap = builder.build();
            return channelInitializerMap;
        } catch (Exception e){
            log.error("KafkaProtocolHandler newChannelInitializers failed with ", e);
            return null;
        }
    }

    @Override
    public void close() {
        Optional.ofNullable(LOOKUP_CLIENT_MAP.remove(brokerService.pulsar())).ifPresent(LookupClient::close);
        offsetTopicClient.close();
        txnTopicClient.close();
        adminManager.shutdown();
        groupCoordinatorsByTenant.values().forEach(GroupCoordinator::shutdown);
        kopEventManager.close();
        transactionCoordinatorByTenant.values().forEach(TransactionCoordinator::shutdown);
        KopBrokerLookupManager.clear();
        KafkaTopicManager.cancelCursorExpireTask();
        KafkaTopicConsumerManagerCache.getInstance().close();
        KafkaTopicManager.getReferences().clear();
        KafkaTopicManager.getTopics().clear();
        kopBrokerLookupManager.close();
        statsProvider.stop();
    }

    @VisibleForTesting
    protected GroupCoordinator startGroupCoordinator(String tenant, SystemTopicClient client) {
        GroupConfig groupConfig = new GroupConfig(
            kafkaConfig.getGroupMinSessionTimeoutMs(),
            kafkaConfig.getGroupMaxSessionTimeoutMs(),
            kafkaConfig.getGroupInitialRebalanceDelayMs()
        );

        String topicName = tenant + "/" + kafkaConfig.getKafkaMetadataNamespace()
                + "/" + Topic.GROUP_METADATA_TOPIC_NAME;

        PulsarAdmin pulsarAdmin;
        int offsetTopicNumPartitions;
        try {
            pulsarAdmin = brokerService.getPulsar().getAdminClient();
            offsetTopicNumPartitions = pulsarAdmin.topics().getPartitionedTopicMetadata(topicName).partitions;
            if (offsetTopicNumPartitions == 0) {
                log.error("Offset topic should not be a non-partitioned topic.");
                throw new IllegalStateException("Offset topic should not be a non-partitioned topic.");
            }
        }  catch (PulsarServerException | PulsarAdminException e) {
            log.error("Failed to get offset topic partition metadata .", e);
            throw new IllegalStateException(e);
        }


        OffsetConfig offsetConfig = OffsetConfig.builder()
            .offsetsTopicName(topicName)
            .offsetsTopicNumPartitions(offsetTopicNumPartitions)
            .offsetsTopicCompressionType(CompressionType.valueOf(kafkaConfig.getOffsetsTopicCompressionCodec()))
            .maxMetadataSize(kafkaConfig.getOffsetMetadataMaxSize())
            .offsetsRetentionCheckIntervalMs(kafkaConfig.getOffsetsRetentionCheckIntervalMs())
            .offsetsRetentionMs(TimeUnit.MINUTES.toMillis(kafkaConfig.getOffsetsRetentionMinutes()))
            .build();

        GroupCoordinator groupCoordinator = GroupCoordinator.of(
            client,
            groupConfig,
            offsetConfig,
            SystemTimer.builder()
                .executorName("group-coordinator-timer")
                .build(),
            Time.SYSTEM
        );
        // always enable metadata expiration
        groupCoordinator.startup(true);

        return groupCoordinator;
    }

    public TransactionCoordinator initTransactionCoordinator(String tenant, PulsarAdmin pulsarAdmin,
                                                             ClusterData clusterData) throws Exception {
        TransactionConfig transactionConfig = TransactionConfig.builder()
                .transactionLogNumPartitions(kafkaConfig.getTxnLogTopicNumPartitions())
                .transactionMetadataTopicName(MetadataUtils.constructTxnLogTopicBaseName(tenant, kafkaConfig))
                .abortTimedOutTransactionsIntervalMs(kafkaConfig.getTxnAbortTimedOutTransactionCleanupIntervalMs())
                .brokerId(kafkaConfig.getBrokerId())
                .build();

        MetadataUtils.createTxnMetadataIfMissing(tenant, pulsarAdmin, clusterData, kafkaConfig);

        TransactionCoordinator transactionCoordinator = TransactionCoordinator.of(
                transactionConfig,
                txnTopicClient,
                brokerService.getPulsar().getLocalMetadataStore(),
                kopBrokerLookupManager,
                OrderedScheduler.newSchedulerBuilder().name("transaction-log-manager").numThreads(1).build(),
                Time.SYSTEM);

        transactionCoordinator.startup().get();

        return transactionCoordinator;
    }

    /**
     * This method discovers ownership of offset topic partitions and attempts to load transaction topics
     * assigned to this broker.
     */
    private void loadTxnLogTopics(String tenant, TransactionCoordinator txnCoordinator) throws Exception {
        Lookup lookupService = brokerService.pulsar().getAdminClient().lookups();
        String currentBroker = brokerService.pulsar().getBrokerServiceUrl();
        String topicBase = MetadataUtils.constructTxnLogTopicBaseName(tenant, kafkaConfig);
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
                    txnCoordinator::handleTxnImmigration).collect(Collectors.toList());

            FutureUtil.waitForAll(lists).get();
        } else {
            log.info("Current broker: {} does not own any of the txn log topic partitions", currentBroker);
        }
    }

    public static @NonNull LookupClient getLookupClient(final PulsarService pulsarService) {
        return LOOKUP_CLIENT_MAP.computeIfAbsent(pulsarService, ignored -> new LookupClient(pulsarService));
    }
}
