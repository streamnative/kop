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
import io.streamnative.kop.utils.ReflectionUtils;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ManagedLedgerClientFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.schema.SchemaRegistryService;
import org.apache.pulsar.broker.stats.MetricsGenerator;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsServlet;
import org.apache.pulsar.broker.web.WebService;
import org.apache.pulsar.common.configuration.VipStatus;
import org.apache.pulsar.zookeeper.LocalZooKeeperConnectionService;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Main class for Kafka-on-Pulsar broker service.
 */
@Slf4j
public class KafkaService extends PulsarService {

    @Getter
    private final KafkaServiceConfiguration kafkaConfig;

    public KafkaService(KafkaServiceConfiguration config) {
        super(config);
        kafkaConfig = config;
    }

    @Override
    public void start() throws PulsarServerException {
        ReentrantLock lock = ReflectionUtils.getField(this, "mutex");

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

            ReflectionUtils.setField(
                this,
                "localZooKeeperConnectionProvider",
                localZooKeeperConnectionService
            );
            localZooKeeperConnectionService.start(getShutdownService());


            // Initialize and start service to access configuration repository.
            ReflectionUtils.callNoArgVoidMethod(
                this,
                "startZkCacheService"
            );

            BookKeeperClientFactory bkClientFactory = newBookKeeperClientFactory();
            ReflectionUtils.setField(
                this,
                "bkClientFactory",
                bkClientFactory
            );
            ReflectionUtils.setField(
                this,
                "managedLedgerClientFactory",
                new ManagedLedgerClientFactory(kafkaConfig, getZkClient(), bkClientFactory)
            );
            ReflectionUtils.setField(
                this,
                "brokerService",
                new KafkaBrokerService(this)
            );

            // Start load management service (even if load balancing is disabled)
            getLoadManager().set(LoadManager.create(this));

            // Start the leader election service
            ReflectionUtils.callNoArgVoidMethod(
                this,
                "startLeaderElectionService"
            );

            // needs load management service
            ReflectionUtils.callNoArgVoidMethod(
                this,
                "startNamespaceService"
            );

            ReflectionUtils.setField(
                this,
                "offloader",
                createManagedLedgerOffloader(kafkaConfig)
            );

            getBrokerService().start();

            WebService webService = new WebService(this);
            ReflectionUtils.setField(this, "webService", webService);
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
            ReflectionUtils.<NamespaceService>getField(
                this, "nsService"
            ).registerBootstrapNamespaces();

            ReflectionUtils.setField(
                this,
                "schemaRegistryService",
                SchemaRegistryService.create(this)
            );

            webService.start();

            ReflectionUtils.setField(
                this,
                "metricsGenerator",
                new MetricsGenerator(this)
            );

            // By starting the Load manager service, the broker will also become visible
            // to the rest of the broker by creating the registration z-node. This needs
            // to be done only when the broker is fully operative.
            ReflectionUtils.callNoArgVoidMethod(
                this,
                "startLoadManagementService");

            reflectSetState(State.Started);

            ReflectionUtils.callNoArgVoidMethod(
                this,
                "acquireSLANamespace");

            final String bootstrapMessage = "bootstrap service "
                    + (kafkaConfig.getWebServicePort().isPresent()
                ? "port = " + kafkaConfig.getWebServicePort().get() : "")
                    + (kafkaConfig.getWebServicePortTls().isPresent()
                ? "tls-port = " + kafkaConfig.getWebServicePortTls() : "")
                    + (kafkaConfig.getKafkaServicePort().isPresent()
                ? "broker url= " + kafkaConfig.getKafkaServicePort() : "")
                    + (kafkaConfig.getKafkaServicePortTls().isPresent()
                ? "broker url= " + kafkaConfig.getKafkaServicePortTls() : "");

            log.info("Kafka messaging service is ready, {}, cluster={}, configs={}", bootstrapMessage,
                kafkaConfig.getClusterName(), ReflectionToStringBuilder.toString(kafkaConfig));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new PulsarServerException(e);
        } finally {
            lock.unlock();
        }
    }

    protected void reflectSetState(State state) {
        try {
            ReflectionUtils.setField(
                this,
                "state",
                state
            );
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException("Unable to set broker set to " + state, e);
        }
    }

}
