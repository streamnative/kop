package io.streamnative.kop.integrations;

import com.google.common.collect.Sets;
import io.streamnative.kop.MockKafkaServiceBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.AfterClass;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

@Slf4j
public class GolangSaramaTest extends MockKafkaServiceBaseTest {

    private static final String SHORT_TOPIC_NAME = "my-sarama-topic";
    private static final String LONG_TOPIC_NAME = "persistent://public/default/my-sarama-topic-full-name";

    @DataProvider
    public static Object[][] topics() {
        return new Object[][]{
                {SHORT_TOPIC_NAME}, {LONG_TOPIC_NAME}
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
        getAdmin().topics().createPartitionedTopic("persistent://public/default/" + SHORT_TOPIC_NAME, 1);
        getAdmin().topics().createPartitionedTopic(LONG_TOPIC_NAME, 1);


        Testcontainers.exposeHostPorts(ImmutableMap.of(super.kafkaBrokerPort, super.kafkaBrokerPort));
    }

    @Test(timeOut = 60_000, dataProvider = "topics")
    void simpleProduceAndConsume(String topic) throws Exception {

        GenericContainer producer = new GenericContainer<>(
                new ImageFromDockerfile().withFileFromPath(".", Paths.get("integrations/golang/sarama")));

        GenericContainer consumer = new GenericContainer<>(
                new ImageFromDockerfile().withFileFromPath(".", Paths.get("integrations/golang/sarama")));

        producer
                .withEnv("KOP_BROKER", "localhost:" + super.kafkaBrokerPort)
                .withEnv("KOP_PRODUCE", "true")
                .withEnv("KOP_TOPIC", topic)
                .waitingFor(
                        Wait.forLogMessage("starting to produce\\n", 1)
                )
                .withNetworkMode("host");

        consumer
                .withEnv("KOP_BROKER", "localhost:" + super.kafkaBrokerPort)
                .withEnv("KOP_TOPIC", topic)
                .withEnv("KOP_CONSUME", "true")
                .waitingFor(
                        Wait.forLogMessage("ready to consume\\n", 1)
                )
                .withNetworkMode("host");

        producer.start();
        WaitingConsumer consumerWaitingConsumer = createLogFollower(producer);
        System.out.println("producer started");
        consumer.start();
        WaitingConsumer producerWaitingConsumer = createLogFollower(consumer);
        System.out.println("consumer started");


        producerWaitingConsumer.waitUntil(frame ->
                frame.getUtf8String().contains("ExitCode"), 30, TimeUnit.SECONDS);
        consumerWaitingConsumer.waitUntil(frame ->
                frame.getUtf8String().contains("ExitCode"), 30, TimeUnit.SECONDS);

        checkForSaramaErrors(producer.getLogs());
        checkForSaramaErrors(consumer.getLogs());
    }

    @Override
    @AfterClass
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }


    private WaitingConsumer createLogFollower(GenericContainer container) {
        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(log);
        container.followOutput(logConsumer);
        WaitingConsumer waitingConsumer = new WaitingConsumer();
        container.followOutput(waitingConsumer, STDOUT);

        return waitingConsumer;
    }

    private void checkForSaramaErrors(String logs) {
        assertFalse(logs.contains("no available broker to send metadata request to"));
        assertFalse(logs.contains("panic"));
        assertFalse(logs.contains("correlation ID didn't match"));

        if (logs.contains("starting to produce")) {
            assertTrue(logs.contains("produced all messages successfully"));
        }

        if (logs.contains("ready to consume")) {
            assertTrue(logs.contains("received msg"));
            assertTrue(logs.contains("limit reached, exiting"));
        }
        assertTrue(logs.contains("ExitCode=0"));
    }
}
