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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.streamnative.kop.coordinator.group.GroupConfig;
import io.streamnative.kop.coordinator.group.GroupCoordinator;
import io.streamnative.kop.coordinator.group.OffsetConfig;
import io.streamnative.kop.utils.timer.SystemTimer;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ManagedLedgerClientFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.stats.MetricsGenerator;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsServlet;
import org.apache.pulsar.broker.web.WebService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.configuration.VipStatus;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.LocalZooKeeperConnectionService;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Main class for Kafka-on-Pulsar broker service.
 */
@Slf4j
public class KafkaService extends PulsarService {

    @Getter
    private final KafkaServiceConfiguration kafkaConfig;
    @Getter
    private KafkaTopicManager kafkaTopicManager;
    @Getter
    private GroupCoordinator groupCoordinator;
    private Producer<ByteBuffer> groupCoordinatorTopicProducer;
    private Reader<ByteBuffer> groupCoordinatorTopicReader;

    public KafkaService(KafkaServiceConfiguration config) {
        super(config);
        kafkaConfig = config;
    }

    @Override
    public void start() throws PulsarServerException {
        ReentrantLock lock = getMutex();

        lock.lock();

        try {
            // TODO: add Kafka on Pulsar Verison support -- https://github.com/streamnative/kop/issues/3
            log.info("Starting Pulsar Broker service powered by Pulsar version: '{}'",
                (getBrokerVersion() != null ? getBrokerVersion() : "unknown"));

            if (getState() != State.Init) {
                throw new PulsarServerException("Cannot start the service once it was stopped");
            }

            if (!kafkaConfig.getWebServicePort().isPresent() && !kafkaConfig.getWebServicePortTls().isPresent()) {
                throw new IllegalArgumentException("webServicePort/webServicePortTls must be present");
            }

            if (!kafkaConfig.getKafkaServicePort().isPresent() && !kafkaConfig.getKafkaServicePortTls().isPresent()) {
                throw new IllegalArgumentException("brokerServicePort/brokerServicePortTls must be present");
            }

            // Now we are ready to start services
            LocalZooKeeperConnectionService localZooKeeperConnectionService =
                new LocalZooKeeperConnectionService(getZooKeeperClientFactory(),
                    kafkaConfig.getZookeeperServers(), kafkaConfig.getZooKeeperSessionTimeoutMillis());

            setLocalZooKeeperConnectionProvider(localZooKeeperConnectionService);
            localZooKeeperConnectionService.start(getShutdownService());

            // Initialize and start service to access configuration repository.
            startZkCacheService();

            BookKeeperClientFactory bkClientFactory = newBookKeeperClientFactory();
            setBkClientFactory(bkClientFactory);
            setManagedLedgerClientFactory(
                new ManagedLedgerClientFactory(kafkaConfig, getZkClient(), bkClientFactory));
            setBrokerService(new KafkaBrokerService(this));

            // Start load management service (even if load balancing is disabled)
            getLoadManager().set(LoadManager.create(this));

            // Start the leader election service
            startLeaderElectionService();

            // needs load management service
            startNamespaceService();

            setOffloader(createManagedLedgerOffloader(kafkaConfig));

            getBrokerService().start();

            WebService webService = new WebService(this);
            setWebService(webService);
            Map<String, Object> attributeMap = Maps.newHashMap();
            attributeMap.put(WebService.ATTRIBUTE_PULSAR_NAME, this);
            Map<String, Object> vipAttributeMap = Maps.newHashMap();
            vipAttributeMap.put(VipStatus.ATTRIBUTE_STATUS_FILE_PATH, kafkaConfig.getStatusFilePath());
            vipAttributeMap.put(VipStatus.ATTRIBUTE_IS_READY_PROBE, new Supplier<Boolean>() {
                @Override
                public Boolean get() {
                    // Ensure the VIP status is only visible when the broker is fully initialized
                    return getState() == State.Started;
                }
            });
            webService.addRestResources("/",
                VipStatus.class.getPackage().getName(), false, vipAttributeMap);
            webService.addRestResources("/",
                "org.apache.pulsar.broker.web", false, attributeMap);
            webService.addRestResources("/admin",
                "org.apache.pulsar.broker.admin.v1", true, attributeMap);
            webService.addRestResources("/admin/v2",
                "org.apache.pulsar.broker.admin.v2", true, attributeMap);
            webService.addRestResources("/admin/v3",
                "org.apache.pulsar.broker.admin.v3", true, attributeMap);
            webService.addRestResources("/lookup",
                "org.apache.pulsar.broker.lookup", true, attributeMap);

            webService.addServlet("/metrics",
                new ServletHolder(
                    new PrometheusMetricsServlet(
                        this,
                        kafkaConfig.isExposeTopicLevelMetricsInPrometheus(),
                        kafkaConfig.isExposeConsumerLevelMetricsInPrometheus())),
                false, attributeMap);

            if (log.isDebugEnabled()) {
                log.debug("Attempting to add static directory");
            }
            webService.addStaticResources("/static", "/static");

            // Register heartbeat and bootstrap namespaces.
            getNsService().registerBootstrapNamespaces();

            setSchemaRegistryService(SchemaRegistryService.create(this));

            webService.start();

            setMetricsGenerator(new MetricsGenerator(this));

            // By starting the Load manager service, the broker will also become visible
            // to the rest of the broker by creating the registration z-node. This needs
            // to be done only when the broker is fully operative.
            startLoadManagementService();

            setState(State.Started);

            acquireSLANamespace();

            final String bootstrapMessage = "bootstrap service "
                    + (kafkaConfig.getWebServicePort().isPresent()
                ? "port = " + kafkaConfig.getWebServicePort().get() : "")
                    + (kafkaConfig.getWebServicePortTls().isPresent()
                ? "tls-port = " + kafkaConfig.getWebServicePortTls() : "")
                    + (kafkaConfig.getKafkaServicePort().isPresent()
                ? "broker url= " + kafkaConfig.getKafkaServicePort() : "")
                    + (kafkaConfig.getKafkaServicePortTls().isPresent()
                ? "broker url= " + kafkaConfig.getKafkaServicePortTls() : "");

            kafkaTopicManager = new KafkaTopicManager(getBrokerService());

            // start group coordinator
            if (kafkaConfig.isEnableGroupCoordinator()) {
                startGroupCoordinator();
            }

            log.info("Kafka messaging service is ready, {}, cluster={}, configs={}", bootstrapMessage,
                kafkaConfig.getClusterName(), ReflectionToStringBuilder.toString(kafkaConfig));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new PulsarServerException(e);
        } finally {
            lock.unlock();
        }
    }

    // TODO: make group coordinator running in a distributed mode
    //      https://github.com/streamnative/kop/issues/32
    private void startGroupCoordinator() throws Exception {
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

        createKafkaMetadataNamespaceIfNeeded();
        String offsetsTopic = createKafkaOffsetsTopic();

        TopicName offsetsTopicName = TopicName.get(offsetsTopic);
        String offsetsTopicPtn0 = offsetsTopicName.getPartition(0).toString();

        this.groupCoordinatorTopicProducer = getClient().newProducer(Schema.BYTEBUFFER)
            .topic(offsetsTopicPtn0)
            // TODO: make it configurable
            .maxPendingMessages(100000)
            .create();
        this.groupCoordinatorTopicReader = getClient().newReader(Schema.BYTEBUFFER)
            .topic(offsetsTopicPtn0)
            .startMessageId(MessageId.earliest)
            .create();
        this.groupCoordinator = GroupCoordinator.of(
            this.groupCoordinatorTopicProducer,
            this.groupCoordinatorTopicReader,
            groupConfig,
            offsetConfig,
            SystemTimer.builder()
                .executorName("group-coordinator-timer")
                .build(),
            Time.SYSTEM
        );

        this.groupCoordinator.startup(false);
    }

    private void createKafkaMetadataNamespaceIfNeeded() throws PulsarServerException, PulsarAdminException {
        String cluster = kafkaConfig.getClusterName();
        String kafkaMetadataTenant = kafkaConfig.getKafkaMetadataTenant();
        String kafkaMetadataNamespace = kafkaMetadataTenant + "/" + kafkaConfig.getKafkaMetadataNamespace();

        try {
            ClusterData clusterData = new ClusterData(getWebServiceAddress(), null /* serviceUrlTls */,
                getBrokerServiceUrl(), null /* brokerServiceUrlTls */);
            if (!getAdminClient().clusters().getClusters().contains(cluster)) {
                getAdminClient().clusters().createCluster(cluster, clusterData);
            } else {
                getAdminClient().clusters().updateCluster(cluster, clusterData);
            }

            if (!getAdminClient().tenants().getTenants().contains(kafkaMetadataTenant)) {
                getAdminClient().tenants().createTenant(kafkaMetadataTenant,
                    new TenantInfo(Sets.newHashSet(kafkaConfig.getSuperUserRoles()), Sets.newHashSet(cluster)));
            }
            if (!getAdminClient().namespaces().getNamespaces(kafkaMetadataTenant).contains(kafkaMetadataNamespace)) {
                Set<String> clusters = Sets.newHashSet(kafkaConfig.getClusterName());
                getAdminClient().namespaces().createNamespace(kafkaMetadataNamespace, clusters);
                getAdminClient().namespaces().setNamespaceReplicationClusters(kafkaMetadataNamespace, clusters);
                getAdminClient().namespaces().setRetention(kafkaMetadataNamespace,
                    new RetentionPolicies(-1, -1));
            }
        } catch (PulsarAdminException e) {
            log.error("Failed to get retention policy for kafka metadata namespace {}",
                kafkaMetadataNamespace, e);
            throw e;
        }
    }

    private String createKafkaOffsetsTopic() throws PulsarServerException, PulsarAdminException {
        String offsetsTopic = kafkaConfig.getKafkaTenant() + "/" + kafkaConfig.getKafkaMetadataNamespace()
            + "/" + Topic.GROUP_METADATA_TOPIC_NAME;

        PartitionedTopicMetadata offsetsTopicMetadata =
            getAdminClient().topics().getPartitionedTopicMetadata(offsetsTopic);
        if (offsetsTopicMetadata.partitions <= 0) {
            log.info("Kafka group metadata topic {} doesn't exist. Creating it ...",
                offsetsTopic);
            getAdminClient().topics().createPartitionedTopic(
                offsetsTopic,
                KafkaServiceConfiguration.DefaultOffsetsTopicNumPartitions
            );
            log.info("Successfully created group metadata topic {}.", offsetsTopic);
        }

        return offsetsTopic;
    }
}
