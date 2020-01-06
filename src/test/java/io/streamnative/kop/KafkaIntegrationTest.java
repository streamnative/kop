package io.streamnative.kop;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.AfterClass;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

@Slf4j
public class KafkaIntegrationTest extends MockKafkaServiceBaseTest {

    @DataProvider
    public static Object[][] integrations() {
        return new Object[][] {
                {"golang-sarama", Optional.empty(), true, true},
                {"golang-sarama", Optional.of("persistent://public/default/my-sarama-topic-full-name"), true, true},
                {"golang-confluent-kafka", Optional.empty(), true, true},
                {"rustlang-rdkafka", Optional.empty(), true, true},
                // consumer is broken, see integrations/README.md
                {"node-kafka-node", Optional.empty(), true, false},
        };
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {

        super.resetConfig();
        super.internalSetup();

        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        } else {
            admin.clusters().updateCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        }

        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        } else {
            admin.tenants().updateTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        }
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
        Testcontainers.exposeHostPorts(ImmutableMap.of(super.kafkaBrokerPort, super.kafkaBrokerPort));
    }

    @Test(timeOut = 120_000, dataProvider = "integrations")
    void simpleProduceAndConsume(String integration, Optional<String> topic, boolean shouldProduce, boolean shouldConsume) throws Exception {

        getAdmin().topics().createPartitionedTopic(topic.orElse(integration), 1);

        GenericContainer producer = new GenericContainer<>(
                new ImageFromDockerfile().withFileFromPath(".", Paths.get("integrations/" + integration)))
                .withEnv("KOP_BROKER", "localhost:" + super.kafkaBrokerPort)
                .withEnv("KOP_PRODUCE", "true")
                .withEnv("KOP_TOPIC", topic.orElse(integration))
                .withEnv("KOP_NBR_MESSAGES", "10")
                .withEnv("KOP_EXPECT_MESSAGES", "10")
                .withLogConsumer(new org.testcontainers.containers.output.Slf4jLogConsumer(log))
                .waitingFor(Wait.forLogMessage("starting to produce\\n", 1))
                .withNetworkMode("host");

        GenericContainer consumer = new GenericContainer<>(
                new ImageFromDockerfile().withFileFromPath(".", Paths.get("integrations/" + integration)))
                .withEnv("KOP_BROKER", "localhost:" + super.kafkaBrokerPort)
                .withEnv("KOP_TOPIC", topic.orElse(integration))
                .withEnv("KOP_CONSUME", "true")
                .withEnv("KOP_NBR_MESSAGES", "10")
                .withEnv("KOP_EXPECT_MESSAGES", "10")
                .withLogConsumer(new org.testcontainers.containers.output.Slf4jLogConsumer(log))
                .waitingFor(Wait.forLogMessage("starting to consume\\n", 1))
                .withNetworkMode("host");

        WaitingConsumer producerWaitingConsumer = null;
        WaitingConsumer consumerWaitingConsumer = null;
        if (shouldProduce) {
            producer.start();
            producerWaitingConsumer = createLogFollower(producer);
            System.out.println("producer started");
        }

        if (shouldConsume) {
            consumer.start();
            consumerWaitingConsumer = createLogFollower(consumer);
            System.out.println("consumer started");
        }

        if (shouldProduce) {
            producerWaitingConsumer.waitUntil(frame ->
                    frame.getUtf8String().contains("ExitCode"), 30, TimeUnit.SECONDS);
            checkForErrorsInLogs(producer.getLogs());
        }

        if (shouldConsume) {
            consumerWaitingConsumer.waitUntil(frame ->
                    frame.getUtf8String().contains("ExitCode"), 30, TimeUnit.SECONDS);
            checkForErrorsInLogs(consumer.getLogs());
        }
    }

    @Override
    @AfterClass
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    private WaitingConsumer createLogFollower(GenericContainer container) {
        WaitingConsumer waitingConsumer = new WaitingConsumer();
        container.followOutput(waitingConsumer);
        return waitingConsumer;
    }

    private void checkForErrorsInLogs(String logs) {
        assertFalse(logs.contains("no available broker to send metadata request to"));
        assertFalse(logs.contains("panic"));
        assertFalse(logs.contains("correlation ID didn't match"));
        assertFalse(logs.contains("Required feature not supported by broker"));


        if (logs.contains("starting to produce")) {
            assertTrue(logs.contains("produced all messages successfully"));
        }

        if (logs.contains("starting to consume")) {
            assertTrue(logs.contains("received msg"));
            assertTrue(logs.contains("limit reached, exiting"));
        }
        assertTrue(logs.contains("ExitCode=0"));
    }
}
