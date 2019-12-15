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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.kop.coordinator.group.GroupConfig;
import io.streamnative.kop.coordinator.group.GroupCoordinator;
import io.streamnative.kop.coordinator.group.OffsetConfig;
import io.streamnative.kop.utils.timer.SystemTimer;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ManagedLedgerClientFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.stats.MetricsGenerator;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsServlet;
import org.apache.pulsar.broker.web.WebService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.configuration.VipStatus;
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
    @Setter
    private GroupCoordinator groupCoordinator;

    public KafkaService(KafkaServiceConfiguration config) {
        super(config);
        kafkaConfig = config;
    }

    @Override
    public Map<String, String> getProtocolDataToAdvertise() {
        return ImmutableMap.<String, String>builder()
            .put("kafka", kafkaConfig.getListeners())
            .build();
    }

    @Override
    public void start() throws PulsarServerException {
        ReentrantLock lock = getMutex();

        lock.lock();

        try {
            // TODO: add Kafka on Pulsar Version support -- https://github.com/streamnative/kop/issues/3
            log.info("Starting Pulsar Broker service powered by Pulsar version: '{}'",
                (getBrokerVersion() != null ? getBrokerVersion() : "unknown"));

            if (getState() != State.Init) {
                throw new PulsarServerException("Cannot start the service once it was stopped");
            }

            if (kafkaConfig.getListeners() == null || kafkaConfig.getListeners().isEmpty()) {
                throw new IllegalArgumentException("Kafka Listeners should be provided through brokerConf.listeners");
            }

            if (kafkaConfig.getAdvertisedAddress() != null &&
                !kafkaConfig.getListeners().contains(kafkaConfig.getAdvertisedAddress())) {
                String err = "Error config: advertisedAddress - " + kafkaConfig.getAdvertisedAddress()
                    + " and listeners - " + kafkaConfig.getListeners() + " not match.";
                log.error(err);
                throw new IllegalArgumentException(err);
            }

            setOrderedExecutor(OrderedExecutor.newBuilder().numThreads(8).name("pulsar-ordered")
                .build());

            // init KafkaProtocolHandler
            KafkaProtocolHandler kafkaProtocolHandler = new KafkaProtocolHandler();
            kafkaProtocolHandler.initialize(kafkaConfig);

            // Now we are ready to start services
            setLocalZooKeeperConnectionProvider(new LocalZooKeeperConnectionService(getZooKeeperClientFactory(),
                kafkaConfig.getZookeeperServers(), kafkaConfig.getZooKeeperSessionTimeoutMillis()));
            getLocalZooKeeperConnectionProvider().start(getShutdownService());

            // Initialize and start service to access configuration repository.
            startZkCacheService();

            BookKeeperClientFactory bkClientFactory = newBookKeeperClientFactory();
            setBkClientFactory(bkClientFactory);
            setManagedLedgerClientFactory(
                new ManagedLedgerClientFactory(kafkaConfig, getZkClient(), bkClientFactory));
            setBrokerService(new BrokerService(this));

            // Start load management service (even if load balancing is disabled)
            getLoadManager().set(LoadManager.create(this));

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

            setSchemaRegistryService(SchemaRegistryService.create(this));

            webService.start();

            // Refresh addresses, since the port might have been dynamically assigned
            setWebServiceAddress(webAddress(kafkaConfig));
            setWebServiceAddressTls(webAddressTls(kafkaConfig));
            setBrokerServiceUrl(kafkaConfig.getBrokerServicePort().isPresent()
                ? brokerUrl(advertisedAddress(kafkaConfig), getBrokerListenPort().get())
                : null);
            setBrokerServiceUrlTls(brokerUrlTls(kafkaConfig));

            // needs load management service
            this.startNamespaceService();

            // Start the leader election service
            startLeaderElectionService();

            // Register heartbeat and bootstrap namespaces.
            getNsService().registerBootstrapNamespaces();

            setMetricsGenerator(new MetricsGenerator(this));

            // By starting the Load manager service, the broker will also become visible
            // to the rest of the broker by creating the registration z-node. This needs
            // to be done only when the broker is fully operative.
            startLoadManagementService();

            acquireSLANamespace();

            final String bootstrapMessage = "bootstrap service "
                    + (kafkaConfig.getWebServicePort().isPresent()
                ? "port = " + kafkaConfig.getWebServicePort().get() : "")
                    + (kafkaConfig.getWebServicePortTls().isPresent()
                ? "tls-port = " + kafkaConfig.getWebServicePortTls() : "")
                    + ("kafka listener url= " + kafkaConfig.getListeners());

            // start Kafka protocol handler.
            // put after load manager for the use of existing broker service to create internal topics.
            kafkaProtocolHandler.start(this.getBrokerService());

            Map<InetSocketAddress, ChannelInitializer<SocketChannel>> channelInitializer =
                kafkaProtocolHandler.newChannelInitializers();
            Map<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>> protocolHandlers = ImmutableMap
                .<String, Map<InetSocketAddress, ChannelInitializer<SocketChannel>>>builder()
                .put("kafka", channelInitializer)
                .build();
            getBrokerService().startProtocolHandlers(protocolHandlers);

            this.kafkaTopicManager = kafkaProtocolHandler.getKafkaTopicManager();
            this.groupCoordinator = kafkaProtocolHandler.getGroupCoordinator();

            setState(State.Started);

            log.info("Kafka messaging service is ready, {}, cluster={}, configs={}",
                bootstrapMessage, kafkaConfig.getClusterName(),
                ReflectionToStringBuilder.toString(kafkaConfig));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new PulsarServerException(e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws PulsarServerException {
        if (groupCoordinator != null) {
            this.groupCoordinator.shutdown();
        }
        if (kafkaTopicManager != null) {
            this.kafkaTopicManager.close();
        }
        super.close();
    }

}
