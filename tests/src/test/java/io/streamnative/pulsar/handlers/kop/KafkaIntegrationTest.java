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
/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.kop;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import com.google.common.collect.Sets;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * This class is running Integration tests for Kafka clients.
 * It uses testcontainers to spawn containers that will either:
 * * produce a number of messages
 * * consume a number of messages
 *
 * <p>As testcontainers is not capable of checking exitCode of the app running in the container,
 * Every container should print the exitCode in stdout.
 *
 * <p>This class is waiting for some precise logs to come-up:
 * * "ready to produce"
 * * "ready to consume"
 * * "produced all messages successfully"
 * * "consumed all messages successfully"
 *
 * <p>This class is using environment variables to control the containers, such as:
 * * broker address,
 * * topic name,
 * * produce or consume mode,
 * * how many message to produce/consume,
 */
@Slf4j
public class KafkaIntegrationTest extends KopProtocolHandlerTestBase {

    public KafkaIntegrationTest(final String entryFormat) {
        super(entryFormat);
    }

    @Factory
    public static Object[] instances() {
        return new Object[] {
                new KafkaIntegrationTest("pulsar"),
                new KafkaIntegrationTest("kafka")
        };
    }

    @DataProvider
    public static Object[][] integrations() {
        return new Object[][]{
                {"golang-sarama", Optional.empty(), true, true},
                {"golang-sarama", Optional.of("persistent://public/default/my-sarama-topic-full-name"), true, true},
                {"golang-confluent-kafka", Optional.empty(), true, true},
                // TODO: rustlang-rdkafka is failing on Github Actions and works locally, we need to investigate
                // {"rustlang-rdkafka", Optional.empty(), true, true},
                // consumer is broken, see integrations/README.md
                {"node-kafka-node", Optional.empty(), true, false},
                {"node-rdkafka", Optional.empty(), true, true},
                {"kafka-client-1.0.0", Optional.empty(), true, true},
                {"kafka-client-1.1.0", Optional.empty(), true, true},
                {"kafka-client-2.0.0", Optional.empty(), true, true},
                {"kafka-client-2.1.0", Optional.empty(), true, true},
                {"kafka-client-2.2.0", Optional.empty(), true, true},
                {"kafka-client-2.3.0", Optional.empty(), true, true},
                {"kafka-client-2.4.0", Optional.empty(), true, true},
                {"kafka-client-2.5.0", Optional.empty(), true, true},
                {"kafka-client-2.6.0", Optional.empty(), true, true},
        };
    }

    public static String getSiteLocalAddress() throws UnknownHostException {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address.isSiteLocalAddress()) {
                        return address.getHostAddress();
                    }
                }
            }
        } catch (SocketException e) {
            log.warn("getSiteLocalAddress failed: {}, use local address instead", e.getMessage());
        }
        return InetAddress.getLocalHost().getHostAddress();
    }

    private static WaitingConsumer createLogFollower(final GenericContainer container) {
        final WaitingConsumer waitingConsumer = new WaitingConsumer();
        container.followOutput(waitingConsumer);
        return waitingConsumer;
    }

    private static void checkForErrorsInLogs(final String logs) {
        assertFalse(logs.contains("no available broker to send metadata request to"));
        assertFalse(logs.contains("panic"));
        assertFalse(logs.contains("correlation ID didn't match"));
        assertFalse(logs.contains("Required feature not supported by broker"));


        if (logs.contains("starting to produce")) {
            assertTrue(logs.contains("produced all messages successfully"));
        }

        if (logs.contains("starting to consume")) {
            assertTrue(logs.contains("consumed all messages successfully"));
        }
        assertTrue(logs.contains("ExitCode=0"));
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.resetConfig();
        this.conf.setEnableTransactionCoordinator(true);
        // in order to access PulsarBroker when using Docker for Mac, we need to adjust things:
        // - set pulsar advertized address to host IP
        // - use the `host.testcontainers.internal` address exposed by testcontainers
        final String ip = getSiteLocalAddress();
        System.out.println("Bind Pulsar broker/KoP on " + ip);
        ((KafkaServiceConfiguration) conf).setListeners(
                PLAINTEXT_PREFIX + ip + ":" + kafkaBrokerPort + ","
                        + SSL_PREFIX + ip + ":" + kafkaBrokerPortTls);
        conf.setKafkaAdvertisedListeners(PLAINTEXT_PREFIX + "127.0.0.1:" + kafkaBrokerPort
                + "," + SSL_PREFIX + "127.0.0.1:" + kafkaBrokerPortTls);
        super.internalSetup();


        if (!this.admin.clusters().getClusters().contains(this.configClusterName)) {
            // so that clients can test short names
            this.admin.clusters().createCluster(this.configClusterName,
                    new ClusterData("http://127.0.0.1:" + this.brokerWebservicePort));
        } else {
            this.admin.clusters().updateCluster(this.configClusterName,
                    new ClusterData("http://127.0.0.1:" + this.brokerWebservicePort));
        }

        if (!this.admin.tenants().getTenants().contains("public")) {
            this.admin.tenants().createTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        } else {
            this.admin.tenants().updateTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        }
        if (!this.admin.namespaces().getNamespaces("public").contains("public/default")) {
            this.admin.namespaces().createNamespace("public/default");
            this.admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
            this.admin.namespaces().setRetention("public/default",
                    new RetentionPolicies(60, 1000));
        }
        if (!this.admin.namespaces().getNamespaces("public").contains("public/__kafka")) {
            this.admin.namespaces().createNamespace("public/__kafka");
            this.admin.namespaces().setNamespaceReplicationClusters("public/__kafka", Sets.newHashSet("test"));
            this.admin.namespaces().setRetention("public/__kafka",
                    new RetentionPolicies(-1, -1));
        }
        Testcontainers.exposeHostPorts(ImmutableMap.of(super.kafkaBrokerPort, super.kafkaBrokerPort));
    }

    @Test(timeOut = 3 * 60_000, dataProvider = "integrations")
    void simpleProduceAndConsume(final String integration, final Optional<String> topic,
                                 final boolean shouldProduce, final boolean shouldConsume) throws Exception {
        String topicName = topic.orElse(integration);
        System.out.println("starting integration " + integration + " with topicName " + topicName);

        admin.topics().createPartitionedTopic(topicName, 1);

        System.out.println("topic created");

        final GenericContainer producer = new GenericContainer<>("streamnative/kop-test-" + integration)
                .withEnv("KOP_BROKER", "host.testcontainers.internal:" + super.kafkaBrokerPort)
                .withEnv("KOP_PRODUCE", "true")
                .withEnv("KOP_TOPIC", topic.orElse(integration))
                .withEnv("KOP_LIMIT", "10")
                .withLogConsumer(
                        new org.testcontainers.containers.output.Slf4jLogConsumer(KafkaIntegrationTest.log))
                .waitingFor(Wait.forLogMessage("starting to produce\\n", 1))
                .withNetworkMode("host");

        final GenericContainer consumer = new GenericContainer<>("streamnative/kop-test-" + integration)
                .withEnv("KOP_BROKER", "host.testcontainers.internal:" + super.kafkaBrokerPort)
                .withEnv("KOP_TOPIC", topic.orElse(integration))
                .withEnv("KOP_CONSUME", "true")
                .withEnv("KOP_LIMIT", "10")
                .withLogConsumer(
                        new org.testcontainers.containers.output.Slf4jLogConsumer(KafkaIntegrationTest.log))
                .waitingFor(Wait.forLogMessage("starting to consume\\n", 1))
                .withNetworkMode("host");

        WaitingConsumer producerWaitingConsumer = null;
        WaitingConsumer consumerWaitingConsumer = null;
        if (shouldProduce) {
            producer.start();
            producerWaitingConsumer = KafkaIntegrationTest.createLogFollower(producer);
            System.out.println("producer started");
        }

        if (shouldConsume) {
            consumer.start();
            consumerWaitingConsumer = KafkaIntegrationTest.createLogFollower(consumer);
            System.out.println("consumer started");
        }

        if (shouldProduce) {
            producerWaitingConsumer.waitUntil(frame ->
                    frame.getUtf8String().contains("ExitCode"), 30, TimeUnit.SECONDS);
            KafkaIntegrationTest.checkForErrorsInLogs(producer.getLogs());
        }

        if (shouldConsume) {
            consumerWaitingConsumer.waitUntil(frame ->
                    frame.getUtf8String().contains("ExitCode"), 30, TimeUnit.SECONDS);
            KafkaIntegrationTest.checkForErrorsInLogs(consumer.getLogs());
        }
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }
}
