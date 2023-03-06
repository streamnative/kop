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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupConfig;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.group.OffsetConfig;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionConfig;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.http.HttpChannelInitializer;
import io.streamnative.pulsar.handlers.kop.migration.MigrationManager;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryChannelInitializer;
import io.streamnative.pulsar.handlers.kop.stats.PrometheusMetricsProvider;
import io.streamnative.pulsar.handlers.kop.stats.StatsLogger;
import io.streamnative.pulsar.handlers.kop.storage.ReplicaManager;
import io.streamnative.pulsar.handlers.kop.utils.ConfigurationUtils;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.MetadataUtils;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperation;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationPurgatory;
import io.streamnative.pulsar.handlers.kop.utils.timer.SystemTimer;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
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
    @Getter
    private RequestStats requestStats;
    private PrometheusMetricsProvider statsProvider;
    @Getter
    private KopBrokerLookupManager kopBrokerLookupManager;
    @VisibleForTesting
    @Getter
    private AdminManager adminManager = null;
    private SystemTopicClient txnTopicClient;
    private DelayedOperationPurgatory<DelayedOperation> producePurgatory;
    private DelayedOperationPurgatory<DelayedOperation> fetchPurgatory;
    private LookupClient lookupClient;
    @VisibleForTesting
    @Getter
    private Map<InetSocketAddress, ChannelInitializer<SocketChannel>> channelInitializerMap;

    @Getter
    @VisibleForTesting
    protected SystemTopicClient offsetTopicClient;

    @Getter
    private KafkaServiceConfiguration kafkaConfig;
    private BrokerService brokerService;
    private KafkaTopicManagerSharedState kafkaTopicManagerSharedState;

    @Getter
    private KopEventManager kopEventManager;
    private OrderedScheduler sendResponseScheduler;
    private NamespaceBundleOwnershipListenerImpl bundleListener;
    private SchemaRegistryManager schemaRegistryManager;
    private MigrationManager migrationManager;
    private ReplicaManager replicaManager;

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

    public ReplicaManager getReplicaManager() {
        return replicaManager;
    }

    @Override
    public String protocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return PROTOCOL_NAME.equalsIgnoreCase(protocol);
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
        StatsLogger rootStatsLogger = statsProvider.getStatsLogger("");
        requestStats = new RequestStats(rootStatsLogger.scope(SERVER_SCOPE));
        sendResponseScheduler = OrderedScheduler.newSchedulerBuilder()
                .name("send-response")
                .numThreads(kafkaConfig.getNumSendKafkaResponseThreads())
                .build();
    }

    // This method is called after initialize
    @Override
    public String getProtocolDataToAdvertise() {
        String result =  kafkaConfig.getKafkaAdvertisedListeners();
        log.info("Advertised addresses for the 'kafka' endpoint: {}", result);
        return result;
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
        kafkaTopicManagerSharedState = new KafkaTopicManagerSharedState(brokerService);
        PulsarAdmin pulsarAdmin;
        try {
            pulsarAdmin = brokerService.getPulsar().getAdminClient();
            adminManager = new AdminManager(pulsarAdmin, kafkaConfig);
        } catch (PulsarServerException e) {
            log.error("Failed to get pulsarAdmin", e);
            throw new IllegalStateException(e);
        }

        lookupClient = new LookupClient(brokerService.pulsar(), kafkaConfig);
        offsetTopicClient = new SystemTopicClient(brokerService.pulsar(), kafkaConfig);
        txnTopicClient = new SystemTopicClient(brokerService.pulsar(), kafkaConfig);

        try {
            kopBrokerLookupManager = new KopBrokerLookupManager(kafkaConfig, brokerService.getPulsar(), lookupClient);
        } catch (Exception ex) {
            log.error("Failed to get kopBrokerLookupManager", ex);
            throw new IllegalStateException(ex);
        }

        final NamespaceService namespaceService = brokerService.pulsar().getNamespaceService();
        bundleListener = new NamespaceBundleOwnershipListenerImpl(namespaceService,
                brokerService.pulsar().getBrokerServiceUrl());
        // Listener for invalidating the global Broker ownership cache
        bundleListener.addTopicOwnershipListener(new TopicOwnershipListener() {
            @Override
            public void whenLoad(TopicName topicName) {
                invalidateBundleCache(topicName);
            }

            @Override
            public void whenUnload(TopicName topicName) {
                invalidateBundleCache(topicName);
                invalidatePartitionLog(topicName);
            }

            @Override
            public String name() {
                return "CacheInvalidator";
            }

            private void invalidateBundleCache(TopicName topicName) {
                kafkaTopicManagerSharedState.deReference(topicName.toString());
                if (!topicName.isPartitioned()) {
                    String nonPartitionedTopicName = topicName.getPartition(0).toString();
                    kafkaTopicManagerSharedState.deReference(nonPartitionedTopicName);
                }
            }

            private void invalidatePartitionLog(TopicName topicName) {
                getReplicaManager().removePartitionLog(topicName.toString());
                if (!topicName.isPartitioned()) {
                    getReplicaManager().removePartitionLog(topicName.getPartition(0).toString());
                }
            }
        });
        namespaceService.addNamespaceBundleOwnershipListener(bundleListener);

        if (kafkaConfig.isKafkaManageSystemNamespaces()) {
            // initialize default Group Coordinator
            getGroupCoordinator(kafkaConfig.getKafkaMetadataTenant());
        }

        // init KopEventManager
        kopEventManager = new KopEventManager(adminManager,
                brokerService.getPulsar().getLocalMetadataStore(),
                requestStats.getStatsLogger(),
                kafkaConfig,
                groupCoordinatorsByTenant);
        kopEventManager.start();

        if (kafkaConfig.isKafkaTransactionCoordinatorEnabled() && kafkaConfig.isKafkaManageSystemNamespaces()) {
            getTransactionCoordinator(kafkaConfig.getKafkaMetadataTenant());
        }

        Configuration conf = new PropertiesConfiguration();
        conf.addProperty("prometheusStatsLatencyRolloverSeconds",
            kafkaConfig.getKopPrometheusStatsLatencyRolloverSeconds());
        conf.addProperty("cluster", kafkaConfig.getClusterName());
        statsProvider.start(conf);
        brokerService.pulsar().addPrometheusRawMetricsProvider(statsProvider);
        schemaRegistryManager = new SchemaRegistryManager(kafkaConfig, brokerService.getPulsar(),
                brokerService.getAuthenticationService());
        migrationManager = new MigrationManager(kafkaConfig, brokerService.getPulsar());
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
            final NamespaceName kafkaMetaNs = NamespaceName.get(tenant, kafkaConfig.getKafkaMetadataNamespace());
            final String metadataNamespace = kafkaConfig.getKafkaMetadataNamespace();
            bundleListener.addTopicOwnershipListener(new TopicOwnershipListener() {
                @Override
                public void whenLoad(TopicName topicName) {
                    if (KopTopic.isTransactionMetadataTopicName(topicName.toString(), metadataNamespace)) {
                        transactionCoordinator.handleTxnImmigration(topicName.getPartitionIndex());
                    }
                }

                @Override
                public void whenUnload(TopicName topicName) {
                    if (KopTopic.isTransactionMetadataTopicName(topicName.toString(), metadataNamespace)) {
                        transactionCoordinator.handleTxnEmigration(topicName.getPartitionIndex());
                    }
                }

                @Override
                public String name() {
                    return "TransactionStateRecover-" + transactionCoordinator.getTopicPartitionName();
                }

                @Override
                public boolean test(NamespaceName namespaceName) {
                    return namespaceName.equals(kafkaMetaNs);
                }
            });
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
            final NamespaceName kafkaMetaNs = NamespaceName.get(tenant, kafkaConfig.getKafkaMetadataNamespace());
            final String metadataNamespace = kafkaConfig.getKafkaMetadataNamespace();
            bundleListener.addTopicOwnershipListener(new TopicOwnershipListener() {
                @Override
                public void whenLoad(TopicName topicName) {
                    if (KopTopic.isGroupMetadataTopicName(topicName.toString(), metadataNamespace)) {
                        groupCoordinator.handleGroupImmigration(topicName.getPartitionIndex());
                    }
                }

                @Override
                public void whenUnload(TopicName topicName) {
                    if (KopTopic.isGroupMetadataTopicName(topicName.toString(), metadataNamespace)) {
                        groupCoordinator.handleGroupEmigration(topicName.getPartitionIndex());
                    }
                }

                @Override
                public String name() {
                    return "OffsetAndTopicListener-" + groupCoordinator.getGroupManager().getTopicPartitionName();
                }

                @Override
                public boolean test(NamespaceName namespaceName) {
                    return namespaceName.equals(kafkaMetaNs);
                }
            });
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
                replicaManager,
                kopBrokerLookupManager,
                adminManager,
                producePurgatory,
                fetchPurgatory,
                endPoint.isTlsEnabled(),
                endPoint,
                kafkaConfig.isSkipMessagesWithoutIndex(),
                requestStats,
                sendResponseScheduler,
                kafkaTopicManagerSharedState,
                lookupClient);
    }

    // this is called after initialize, and with kafkaConfig, brokerService all set.
    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkState(kafkaConfig != null);
        checkState(brokerService != null);

        producePurgatory = DelayedOperationPurgatory.builder()
                .purgatoryName("produce")
                .timeoutTimer(SystemTimer.builder().executorName("produce").build())
                .build();
        fetchPurgatory = DelayedOperationPurgatory.builder()
                .purgatoryName("fetch")
                .timeoutTimer(SystemTimer.builder().executorName("fetch").build())
                .build();

        replicaManager = new ReplicaManager(
                kafkaConfig,
                requestStats,
                Time.SYSTEM,
                brokerService.getEntryFilterProvider().getBrokerEntryFilters(),
                producePurgatory,
                fetchPurgatory);

        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                    ImmutableMap.builder();

            EndPoint.parseListeners(kafkaConfig.getListeners(), kafkaConfig.getKafkaProtocolMap()).
                    forEach((listener, endPoint) ->
                            builder.put(endPoint.getInetAddress(), newKafkaChannelInitializer(endPoint))
                    );

            Optional<HttpChannelInitializer> migrationChannelInitializer = migrationManager.build();
            migrationChannelInitializer.ifPresent(
                    initializer -> builder.put(migrationManager.getAddress(),
                            initializer));

            Optional<SchemaRegistryChannelInitializer> schemaRegistryChannelInitializer = schemaRegistryManager.build();
            schemaRegistryChannelInitializer.ifPresent(
                    registryChannelInitializer -> builder.put(schemaRegistryManager.getAddress(),
                            registryChannelInitializer));
            channelInitializerMap = builder.build();
            return channelInitializerMap;
        } catch (Exception e){
            log.error("KafkaProtocolHandler newChannelInitializers failed with ", e);
            return null;
        }
    }

    @Override
    public void close() {
        if (producePurgatory != null) {
            producePurgatory.shutdown();
        }
        if (fetchPurgatory != null) {
            fetchPurgatory.shutdown();
        }
        groupCoordinatorsByTenant.values().forEach(GroupCoordinator::shutdown);
        kopEventManager.close();
        if (schemaRegistryManager != null) {
            schemaRegistryManager.close();
        }
        transactionCoordinatorByTenant.values().forEach(TransactionCoordinator::shutdown);
        KopBrokerLookupManager.clear();
        kafkaTopicManagerSharedState.close();
        kopBrokerLookupManager.close();
        statsProvider.stop();
        sendResponseScheduler.shutdown();

        List<CompletableFuture<?>> closeHandles = new ArrayList<>();
        if (offsetTopicClient != null) {
            closeHandles.add(offsetTopicClient.closeAsync());
        }
        if (txnTopicClient != null) {
            closeHandles.add(txnTopicClient.closeAsync());
        }
        if (lookupClient != null) {
            closeHandles.add(lookupClient.closeAsync());
        }
        if (adminManager != null) {
            adminManager.shutdown();
        }

        // do not block the broker forever
        // see https://github.com/apache/pulsar/issues/19579
        try {
            FutureUtil
                    .waitForAll(closeHandles)
                    .get(Math.max(kafkaConfig.getBrokerShutdownTimeoutMs() / 10, 1000),
                            TimeUnit.MILLISECONDS);
        } catch (ExecutionException err) {
            log.warn("Error while closing some of the internal PulsarClients", err.getCause());
        } catch (TimeoutException err) {
            log.warn("Could not stop all the internal PulsarClients within the configured timeout");
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            log.warn("Could not stop all the internal PulsarClients");
        }
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

        String namespacePrefixForMetadata = MetadataUtils.constructMetadataNamespace(tenant, kafkaConfig);

        OffsetConfig offsetConfig = OffsetConfig.builder()
            .offsetsTopicName(topicName)
            .offsetsTopicNumPartitions(offsetTopicNumPartitions)
            .offsetsTopicCompressionType(CompressionType.valueOf(kafkaConfig.getOffsetsTopicCompressionCodec()))
            .maxMetadataSize(kafkaConfig.getOffsetMetadataMaxSize())
            .offsetsRetentionCheckIntervalMs(kafkaConfig.getOffsetsRetentionCheckIntervalMs())
            .offsetsRetentionMs(TimeUnit.MINUTES.toMillis(kafkaConfig.getOffsetsRetentionMinutes()))
            .build();

        GroupCoordinator groupCoordinator = GroupCoordinator.of(
            tenant,
            client,
            groupConfig,
            offsetConfig,
            namespacePrefixForMetadata,
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
                .transactionLogNumPartitions(kafkaConfig.getKafkaTxnLogTopicNumPartitions())
                .transactionMetadataTopicName(MetadataUtils.constructTxnLogTopicBaseName(tenant, kafkaConfig))
                .transactionProducerIdTopicName(MetadataUtils.constructTxnProducerIdTopicBaseName(tenant, kafkaConfig))
                .abortTimedOutTransactionsIntervalMs(kafkaConfig.getKafkaTxnAbortTimedOutTransactionCleanupIntervalMs())
                .transactionalIdExpirationMs(kafkaConfig.getKafkaTransactionalIdExpirationMs())
                .removeExpiredTransactionalIdsIntervalMs(
                        kafkaConfig.getKafkaTransactionsRemoveExpiredTransactionalIdCleanupIntervalMs())
                .brokerId(kafkaConfig.getKafkaBrokerId())
                .build();

        MetadataUtils.createTxnMetadataIfMissing(tenant, pulsarAdmin, clusterData, kafkaConfig);

        TransactionCoordinator transactionCoordinator = TransactionCoordinator.of(
                tenant,
                kafkaConfig,
                transactionConfig,
                txnTopicClient,
                brokerService.getPulsar().getLocalMetadataStore(),
                kopBrokerLookupManager,
                OrderedScheduler
                        .newSchedulerBuilder()
                        .name("transaction-log-manager-" + tenant)
                        .numThreads(1)
                        .build(),
                Time.SYSTEM);

        transactionCoordinator.startup(kafkaConfig.isKafkaTransactionalIdExpirationEnable()).get();

        return transactionCoordinator;
    }
}
