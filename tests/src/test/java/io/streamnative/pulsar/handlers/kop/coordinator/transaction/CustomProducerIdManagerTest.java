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
package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import io.streamnative.pulsar.handlers.kop.utils.MetadataUtils;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class CustomProducerIdManagerTest extends KopProtocolHandlerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        this.conf.setDefaultNumberOfNamespaceBundles(4);
        this.conf.setOffsetsTopicNumPartitions(50);
        this.conf.setKafkaTxnLogTopicNumPartitions(50);
        this.conf.setKafkaTransactionCoordinatorEnabled(true);
        this.conf.setBrokerDeduplicationEnabled(true);
        this.conf.setKafkaManageSystemNamespaces(false); // make transaction coordinator created lazily
        super.internalSetup();

        final TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Collections.singleton(conf.getClusterName()))
                .build();
        admin.tenants().createTenant(conf.getKafkaTenant(), tenantInfo);
        admin.namespaces().createNamespace(conf.getKafkaTenant() + "/" + conf.getKafkaNamespace());
        try {
            admin.tenants().createTenant(conf.getKafkaMetadataTenant(), tenantInfo);
        } catch (PulsarAdminException.ConflictException ignored) {
        }
        admin.namespaces().createNamespace(conf.getKafkaMetadataTenant() + "/" + conf.getKafkaMetadataNamespace());
        admin.topics().createPartitionedTopic(
                MetadataUtils.constructTxnLogTopicBaseName(conf.getKafkaMetadataTenant(), conf),
                conf.getKafkaTxnLogTopicNumPartitions());
        admin.topics().createPartitionedTopic(
                MetadataUtils.constructTxnProducerIdTopicBaseName(conf.getKafkaMetadataTenant(), conf), 1);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testFallbackToDefaultProducerIdManager() throws Exception {
        conf.setKopProducerIdManagerClassName(InvalidProducerIdManager.class.getName());
        initProducerId();
        final MetadataStoreExtended metadataStore = pulsar.getLocalMetadataStore();
        assertNotNull(metadataStore.get(ProducerIdManagerImpl.KOP_PID_BLOCK_ZNODE).get().orElse(null));
    }

    @Test
    public void testGenerateIdFailure() {
        conf.setKopProducerIdManagerClassName(WrongProducerIdManager.class.getName());
        assertThrows(KafkaException.class, this::initProducerId);
    }

    @Test
    public void testPulsarTopicProducerIdManager() throws Exception {
        conf.setKafkaTransactionProducerIdsStoredOnPulsar(true);
        conf.setKopProducerIdManagerClassName(null);
        initProducerId();
        final MetadataStoreExtended metadataStore = pulsar.getLocalMetadataStore();
        assertNull(metadataStore.get(ProducerIdManagerImpl.KOP_PID_BLOCK_ZNODE).get().orElse(null));
        final List<String> metadataTopics =
                admin.topics().getList(conf.getKafkaMetadataTenant() + "/" + conf.getKafkaMetadataNamespace());
        assertTrue(metadataTopics.contains(
                "persistent://public/__kafka/__transaction_producerid_generator-partition-0"));
    }

    private void initProducerId() {
        final Properties props = newKafkaProducerProperties();
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "txn-id");
        @Cleanup final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();
    }

    private abstract static class AbstractProducerIdManager implements ProducerIdManager {

        @Override
        public CompletableFuture<Void> initialize() {
            return CompletableFuture.completedFuture(null);
        }

        public abstract CompletableFuture<Long> generateProducerId();

        @Override
        public void shutdown() {
        }
    }

    public static class WrongProducerIdManager extends AbstractProducerIdManager {

        @Override
        public CompletableFuture<Long> generateProducerId() {
            return FutureUtil.failedFuture(new IllegalStateException("Cannot generate producer ID"));
        }
    }

    public static class InvalidProducerIdManager extends AbstractProducerIdManager {

        public InvalidProducerIdManager(final int ignored) {
        }

        @Override
        public CompletableFuture<Long> generateProducerId() {
            return CompletableFuture.completedFuture(0L);
        }
    }
}
