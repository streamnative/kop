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

import com.google.common.collect.Sets;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TopicType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Inner topic protection test.
 */
@Slf4j
public class InnerTopicProtectionTest extends KopProtocolHandlerTestBase {

    protected int offsetsTopicNumPartitions;

    protected KafkaServiceConfiguration resetConfig(int brokerPort, int webPort, int kafkaPort) {
        KafkaServiceConfiguration kConfig = new KafkaServiceConfiguration();
        kConfig.setBrokerServicePort(Optional.ofNullable(brokerPort));
        kConfig.setWebServicePort(Optional.ofNullable(webPort));
        kConfig.setListeners(PLAINTEXT_PREFIX + "localhost:" + kafkaPort);

        kConfig.setOffsetsTopicNumPartitions(offsetsTopicNumPartitions);

        kConfig.setAdvertisedAddress("localhost");
        kConfig.setClusterName(configClusterName);
        kConfig.setManagedLedgerCacheSizeMB(8);
        kConfig.setActiveConsumerFailoverDelayTimeMillis(0);
        kConfig.setDefaultNumberOfNamespaceBundles(2);
        kConfig.setZookeeperServers("localhost:2181");
        kConfig.setConfigurationStoreServers("localhost:3181");
        kConfig.setAuthenticationEnabled(false);
        kConfig.setAuthorizationEnabled(false);
        kConfig.setAllowAutoTopicCreation(true);
        kConfig.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        kConfig.setBrokerDeleteInactiveTopicsEnabled(false);
        kConfig.setSystemTopicEnabled(true);
        kConfig.setTopicLevelPoliciesEnabled(true);
        kConfig.setGroupInitialRebalanceDelayMs(0);
        kConfig.setBrokerShutdownTimeoutMs(0);
        kConfig.setKafkaTransactionCoordinatorEnabled(true);

        // set protocol related config
        URL testHandlerUrl = this.getClass().getClassLoader().getResource("test-protocol-handler.nar");
        Path handlerPath;
        try {
            handlerPath = Paths.get(testHandlerUrl.toURI());
        } catch (Exception e) {
            log.error("failed to get handler Path, handlerUrl: {}. Exception: ", testHandlerUrl, e);
            return null;
        }

        String protocolHandlerDir = handlerPath.toFile().getParent();

        kConfig.setProtocolHandlerDirectory(
            protocolHandlerDir
        );
        kConfig.setMessagingProtocols(Sets.newHashSet("kafka"));

        return kConfig;
    }

    @Override
    protected void resetConfig() {
        offsetsTopicNumPartitions = 16;
        conf = resetConfig(
            brokerPort,
            brokerWebservicePort,
            kafkaBrokerPort);

        log.info("Ports --  broker: {}, brokerWeb:{}, kafka: {}",
            brokerPort, brokerWebservicePort, kafkaBrokerPort);
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.namespaces().getNamespaces("public").contains("public/default")) {
            admin.namespaces().createNamespace("public/default");
            admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/default",
                new RetentionPolicies(60, 1000));
        }
        if (!admin.namespaces().getNamespaces("public").contains("public/__kafka")) {
            admin.namespaces().createNamespace("public/__kafka");
            admin.namespaces().setNamespaceReplicationClusters("public/__kafka", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/__kafka",
                new RetentionPolicies(-1, -1));
        }
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 30000)
    public void testInnerTopicProduce() throws PulsarAdminException, InterruptedException {
        final String offsetTopic = "public/__kafka/__consumer_offsets";
        final String transactionTopic = "public/__kafka/__transaction_state";
        final String systemTopic = "__change_events";
        final String commonTopic = "normal-topic";
        final String userNamespaceOffsetTopic = "__consumer_offsets";
        final String userNamespaceTransactionTopic = "__transaction_state";

        admin.topics().createPartitionedTopic(commonTopic, 3);
        admin.topics().createPartitionedTopic(userNamespaceOffsetTopic, 3);
        admin.topics().createPartitionedTopic(userNamespaceTransactionTopic, 3);
        // test inner topic produce
        @Cleanup
        final KafkaProducer<String, String> kafkaProducer = newKafkaProducer();
        final String msg = "test-inner-topic-produce-and-consume";
        assertProduceMessage(kafkaProducer, offsetTopic, msg, true);
        assertProduceMessage(kafkaProducer, transactionTopic, msg, true);
        assertProduceMessage(kafkaProducer, systemTopic, msg, true);
        assertProduceMessage(kafkaProducer, commonTopic, msg, false);
        assertProduceMessage(kafkaProducer, userNamespaceOffsetTopic, msg, false);
        assertProduceMessage(kafkaProducer, userNamespaceTransactionTopic, msg, false);
    }

    private void assertProduceMessage(KafkaProducer producer, final String topic, final String value,
                                      boolean assertException) {
        try {
            producer.send(new ProducerRecord<>(topic, value)).get();
            if (assertException) {
                Assert.fail();
            }
        } catch (Exception e) {
            if (assertException) {
                Assert.assertEquals(e.getCause().getMessage(),
                    "The request attempted to perform an operation on an invalid topic.");
            } else {
                Assert.fail();
            }
        }
    }

    protected KafkaProducer<String, String> newKafkaProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        return new KafkaProducer<>(props);
    }
}
