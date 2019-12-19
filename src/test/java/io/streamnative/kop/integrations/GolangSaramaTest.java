package io.streamnative.kop.integrations;

import com.google.common.collect.Sets;
import io.streamnative.kop.MockKafkaServiceBaseTest;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT;
import static org.testng.AssertJUnit.assertFalse;

@Slf4j
public class GolangSaramaTest extends MockKafkaServiceBaseTest {

    @ClassRule
    public GenericContainer container = new GenericContainer<>(
            new ImageFromDockerfile().withFileFromPath(".", Paths.get("integrations/golang/sarama")));

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
        getAdmin().topics().createPartitionedTopic("persistent://public/default/my-sarama-topic", 1);

        container
                .withEnv("KOP_BROKER", "localhost:" + super.kafkaBrokerPort)
                .withNetworkMode("host");
        Testcontainers.exposeHostPorts(ImmutableMap.of(super.kafkaBrokerPort, super.kafkaBrokerPort));
    }

    @Test(timeOut = 120_000)
    void simpleProduceAndConsume() throws Exception {
        System.out.println("building container");
        container.start();
        System.out.println("container started");

        Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(log);
        container.followOutput(logConsumer);
        WaitingConsumer consumer = new WaitingConsumer();
        container.followOutput(consumer, STDOUT);

        consumer.waitUntil(frame ->
                frame.getUtf8String().contains("reached"), 30, TimeUnit.SECONDS);
        System.out.println("after reached");

        checkForSaramaErrors(container.getLogs());
    }

    @Override
    @AfterClass
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private void checkForSaramaErrors(String logs) {
        assertFalse(logs.contains("no available broker to send metadata request to"));
        assertFalse(logs.contains("panic"));
        assertFalse(logs.contains("correlation ID didn't match"));
    }
}
