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

import com.beust.jcommander.Parameter;
import com.google.common.collect.Sets;
import java.io.File;
import java.net.URL;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;

/**
 * A standalone instance includes all the components for running Kafka-on-Pulsar.
 */
@Slf4j
public class KafkaStandalone implements AutoCloseable {
    KafkaService kafkaBroker;
    PulsarAdmin admin;
    LocalBookkeeperEnsemble bkEnsemble;
    KafkaServiceConfiguration config;

    public void setKafkaBroker(KafkaService kafkaBroker) {
        this.kafkaBroker = kafkaBroker;
    }

    public void setAdmin(PulsarAdmin admin) {
        this.admin = admin;
    }

    public void setBkEnsemble(LocalBookkeeperEnsemble bkEnsemble) {
        this.bkEnsemble = bkEnsemble;
    }

    public void setBkPort(int bkPort) {
        this.bkPort = bkPort;
    }

    public void setBkDir(String bkDir) {
        this.bkDir = bkDir;
    }

    public void setAdvertisedAddress(String advertisedAddress) {
        this.advertisedAddress = advertisedAddress;
    }

    public void setConfig(KafkaServiceConfiguration config) {
        this.config = config;
    }

    public void setConfigFile(String configFile) {
        this.configFile = configFile;
    }

    public void setWipeData(boolean wipeData) {
        this.wipeData = wipeData;
    }

    public void setNumOfBk(int numOfBk) {
        this.numOfBk = numOfBk;
    }

    public void setZkPort(int zkPort) {
        this.zkPort = zkPort;
    }

    public void setZkDir(String zkDir) {
        this.zkDir = zkDir;
    }

    public void setNoBroker(boolean noBroker) {
        this.noBroker = noBroker;
    }

    public void setOnlyBroker(boolean onlyBroker) {
        this.onlyBroker = onlyBroker;
    }

    public void setNoStreamStorage(boolean noStreamStorage) {
        this.noStreamStorage = noStreamStorage;
    }

    public void setStreamStoragePort(int streamStoragePort) {
        this.streamStoragePort = streamStoragePort;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }

    public ServiceConfiguration getConfig() {
        return config;
    }

    public String getConfigFile() {
        return configFile;
    }

    public boolean isWipeData() {
        return wipeData;
    }

    public int getNumOfBk() {
        return numOfBk;
    }

    public int getZkPort() {
        return zkPort;
    }

    public int getBkPort() {
        return bkPort;
    }

    public String getZkDir() {
        return zkDir;
    }

    public String getBkDir() {
        return bkDir;
    }

    public boolean isNoBroker() {
        return noBroker;
    }

    public boolean isOnlyBroker() {
        return onlyBroker;
    }

    public boolean isNoStreamStorage() {
        return noStreamStorage;
    }

    public int getStreamStoragePort() {
        return streamStoragePort;
    }

    public String getAdvertisedAddress() {
        return advertisedAddress;
    }

    public boolean isHelp() {
        return help;
    }

    @Parameter(names = { "-c", "--config" }, description = "Configuration file path", required = true)
    private String configFile;

    @Parameter(names = { "--wipe-data" }, description = "Clean up previous ZK/BK data")
    private boolean wipeData = false;

    @Parameter(names = { "--num-bookies" }, description = "Number of local Bookies")
    private int numOfBk = 1;

    @Parameter(names = { "--zookeeper-port" }, description = "Local zookeeper's port")
    private int zkPort = 2181;

    @Parameter(names = { "--bookkeeper-port" }, description = "Local bookies base port")
    private int bkPort = 3181;

    @Parameter(names = { "--zookeeper-dir" }, description = "Local zooKeeper's data directory")
    private String zkDir = "data/standalone/zookeeper";

    @Parameter(names = { "--bookkeeper-dir" }, description = "Local bookies base data directory")
    private String bkDir = "data/standalone/bookkeeper";

    @Parameter(names = { "--no-kafkaBroker" }, description = "Only start ZK and BK services, no kafkaBroker")
    private boolean noBroker = false;

    @Parameter(names = { "--only-kafkaBroker" }, description = "Only start Pulsar kafkaBroker service (no ZK, BK)")
    private boolean onlyBroker = false;

    @Parameter(names = {"-nss", "--no-stream-storage"}, description = "Disable stream storage")
    private boolean noStreamStorage = true;

    @Parameter(names = { "--stream-storage-port" }, description = "Local bookies stream storage port")
    private int streamStoragePort = 4181;

    @Parameter(names = { "-a", "--advertised-address" }, description = "Standalone kafkaBroker advertised address")
    private String advertisedAddress = null;

    @Parameter(names = { "-h", "--help" }, description = "Show this help message")
    private boolean help = false;

    public void start() throws Exception {

        if (config == null) {
            throw new IllegalArgumentException("Null configuration is provided");
        }

        if (config.getAdvertisedAddress() != null && !config.getListeners().contains(config.getAdvertisedAddress())) {
            String err = "Error config: advertisedAddress - " + config.getAdvertisedAddress() + " and listeners - "
                + config.getListeners() + " not match.";
            log.error(err);
            throw new IllegalArgumentException(err);
        }

        log.info("--- setup KafkaStandaloneStarter ---");

        if (!this.isOnlyBroker()) {
            ServerConfiguration bkServerConf = new ServerConfiguration();
            bkServerConf.loadConf(new File(configFile).toURI().toURL());

            // Start LocalBookKeeper
            bkEnsemble = new LocalBookkeeperEnsemble(
                    this.getNumOfBk(), this.getZkPort(), this.getBkPort(), this.getStreamStoragePort(), this.getZkDir(),
                    this.getBkDir(), this.isWipeData(), "127.0.0.1");
            bkEnsemble.startStandalone(bkServerConf, !this.isNoStreamStorage());
        }

        if (this.isNoBroker()) {
            return;
        }

        // Start Broker
        kafkaBroker = new KafkaService(config);
        kafkaBroker.start();

        URL webServiceUrl = new URL(
                String.format("http://%s:%d", config.getAdvertisedAddress(), config.getWebServicePort().get()));
        final String brokerServiceUrl = String.format("pulsar://%s:%d", config.getAdvertisedAddress(),
            config.getBrokerServicePort().get());

        admin = PulsarAdmin.builder().serviceHttpUrl(webServiceUrl.toString()).authentication(
                config.getBrokerClientAuthenticationPlugin(), config.getBrokerClientAuthenticationParameters()).build();

        final String cluster = config.getClusterName();

        createDefaultNameSpace(webServiceUrl, brokerServiceUrl, cluster);

        log.info("--- setup completed ---");
    }

    private void createDefaultNameSpace(URL webServiceUrl, String brokerServiceUrl, String cluster) {
        // Create a public tenant and default namespace
        final String publicTenant = TopicName.PUBLIC_TENANT;
        final String defaultNamespace = TopicName.PUBLIC_TENANT + "/" + TopicName.DEFAULT_NAMESPACE;
        try {
            ClusterData clusterData = new ClusterData(webServiceUrl.toString(), null /* serviceUrlTls */,
                brokerServiceUrl, null /* brokerServiceUrlTls */);
            if (!admin.clusters().getClusters().contains(cluster)) {
                admin.clusters().createCluster(cluster, clusterData);
            } else {
                admin.clusters().updateCluster(cluster, clusterData);
            }

            if (!admin.tenants().getTenants().contains(publicTenant)) {
                admin.tenants().createTenant(publicTenant,
                        new TenantInfo(Sets.newHashSet(config.getSuperUserRoles()), Sets.newHashSet(cluster)));
            }
            if (!admin.namespaces().getNamespaces(publicTenant).contains(defaultNamespace)) {
                Set<String> clusters = Sets.newHashSet(config.getClusterName());
                admin.namespaces().createNamespace(defaultNamespace, clusters);
                admin.namespaces().setNamespaceReplicationClusters(defaultNamespace, clusters);
                admin.namespaces().setRetention(defaultNamespace,
                    new RetentionPolicies(20, 100));
            }
        } catch (PulsarAdminException e) {
            log.info("error while create default namespace: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            if (kafkaBroker != null) {
                kafkaBroker.close();
            }

            if (bkEnsemble != null) {
                bkEnsemble.stop();
            }
        } catch (Exception e) {
            log.error("Shutdown failed: {}", e.getMessage());
        }
    }
}
