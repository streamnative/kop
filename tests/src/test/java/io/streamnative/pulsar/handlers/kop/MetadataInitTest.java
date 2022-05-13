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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

import io.streamnative.pulsar.handlers.kop.utils.MetadataUtils;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class MetadataInitTest extends KopProtocolHandlerTestBase {

    private static final String CLUSTER = "my-cluster";
    private static final KafkaServiceConfiguration DEFAULT_CONFIG = new KafkaServiceConfiguration();
    private static final String DEFAULT_METADATA_NS =
            DEFAULT_CONFIG.getKafkaMetadataTenant() + "/" + DEFAULT_CONFIG.getKafkaMetadataNamespace();

    private String protocolHandlerDirectory;

    @Test(timeOut = 40000)
    public void testSingleBrokerRestart() throws Exception {
        log.info("testSingleBrokerRestart");
        final KafkaServiceConfiguration conf = createConfig();
        final PulsarService pulsarService = startBroker(conf);
        new MetadataNamespacePolicies(pulsarService).verify(
                DEFAULT_METADATA_NS,
                DEFAULT_CONFIG.getOffsetsRetentionMinutes(),
                DEFAULT_CONFIG.getSystemTopicRetentionSizeInMB(),
                DEFAULT_CONFIG.getOffsetsMessageTTL()
        );
        stopBroker(pulsarService);

        conf.setOffsetsRetentionMinutes(1001L);
        conf.setSystemTopicRetentionSizeInMB(1025);
        conf.setOffsetsMessageTTL(999);
        final PulsarService pulsarService1 = startBroker(conf);
        // The policies have already been configured, so the restart won't change them
        new MetadataNamespacePolicies(pulsarService).verify(
                DEFAULT_METADATA_NS,
                DEFAULT_CONFIG.getOffsetsRetentionMinutes(),
                DEFAULT_CONFIG.getSystemTopicRetentionSizeInMB(),
                DEFAULT_CONFIG.getOffsetsMessageTTL()
        );
        stopBroker(pulsarService1);
    }

    @Test(timeOut = 30000)
    public void testCustomPolicies() throws Exception {
        log.info("testCustomPolicies");
        final KafkaServiceConfiguration conf = createConfig();
        conf.setOffsetsRetentionMinutes(1001L);
        conf.setSystemTopicRetentionSizeInMB(1025);
        conf.setOffsetsMessageTTL(999);
        conf.setKafkaMetadataTenant("my-tenant");
        conf.setKafkaMetadataNamespace("my-ns");

        final PulsarService pulsarService = startBroker(conf);
        new MetadataNamespacePolicies(pulsarService).verify(
                "my-tenant/my-ns", 1001L, 1025, 999);
        stopBroker(pulsarService);
    }

    @Test(timeOut = 30000)
    public void testMetadataInitDisabled() throws Exception {
        final KafkaServiceConfiguration conf = createConfig();
        conf.setKafkaManageSystemNamespaces(false);

        final PulsarService pulsarService = startBroker(conf);
        assertThrows(PulsarAdminException.NotFoundException.class, () -> new MetadataNamespacePolicies(pulsarService));
        stopBroker(pulsarService);
    }

    @BeforeClass
    public void findProtocolHandler() throws Exception {
        this.protocolHandlerDirectory = getProtocolHandlerDirectory();
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup(false); // only start ZK and BK
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private static String getProtocolHandlerDirectory() throws URISyntaxException {
        final URL url = MetadataInitTest.class.getClassLoader().getResource("test-protocol-handler.nar");
        if (url == null) {
            throw new IllegalStateException("Failed to load test-protocol-handler.nar from resource directory");
        }
        return Paths.get(url.toURI()).toFile().getParent();
    }

    private KafkaServiceConfiguration createConfig() {
        final int brokerPort = PortManager.nextFreePort();
        final int webPort = PortManager.nextFreePort();
        final int kafkaPort = PortManager.nextFreePort();
        log.info("Create config with brokerPort={}, webPort={}, kafkaPort={}", brokerPort, webPort, kafkaPort);

        final KafkaServiceConfiguration conf = new KafkaServiceConfiguration();
        conf.setBrokerServicePort(Optional.of(brokerPort));
        conf.setWebServicePort(Optional.of(webPort));
        conf.setClusterName(CLUSTER);
        conf.setZookeeperServers("localhost:2181");
        conf.setConfigurationStoreServers("localhost:2181");

        conf.setProtocolHandlerDirectory(protocolHandlerDirectory);
        conf.setMessagingProtocols(Collections.singleton("kafka"));
        conf.setKafkaListeners(PLAINTEXT_PREFIX + "localhost:" + kafkaPort);
        return conf;
    }

    private static class MetadataNamespacePolicies {

        private final String namespace;
        private final RetentionPolicies retentionPolicies;
        private final Long compactionThreshold;
        private final Integer messageTTL;

        public MetadataNamespacePolicies(final PulsarService pulsarService) throws Exception {
            final PulsarAdmin admin = pulsarService.getAdminClient();
            final KafkaServiceConfiguration conf = (KafkaServiceConfiguration) pulsarService.getConfiguration();
            assertNotNull(conf);
            this.namespace = conf.getKafkaMetadataTenant() + "/" + conf.getKafkaMetadataNamespace();
            this.retentionPolicies = admin.namespaces().getRetention(namespace);
            this.compactionThreshold = admin.namespaces().getCompactionThreshold(namespace);
            this.messageTTL = admin.namespaces().getNamespaceMessageTTL(namespace);
        }

        public void verify(final String namespace,
                           final long retentionTimeInMinutes,
                           final int retentionSizeInMB,
                           final int messageTTL) {
            assertEquals(this.namespace, namespace);
            assertEquals(retentionPolicies.getRetentionTimeInMinutes(), (int) retentionTimeInMinutes);
            assertEquals(retentionPolicies.getRetentionSizeInMB(), retentionSizeInMB);
            assertNotNull(compactionThreshold);
            assertEquals(compactionThreshold.intValue(), MetadataUtils.MAX_COMPACTION_THRESHOLD);
            assertNotNull(this.messageTTL);
            assertEquals(this.messageTTL.intValue(), messageTTL);
        }
    }
}
