package io.streamnative.pulsar.handlers.kop;

import com.google.common.collect.Sets;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class EntryPublishTimeTest extends KopProtocolHandlerTestBase {
    private static final Logger log = LoggerFactory.getLogger(EntryPublishTimeTest.class);

    KafkaRequestHandler kafkaRequestHandler;
    SocketAddress serviceAddress;

    public EntryPublishTimeTest(String format) {
        super(format);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
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

        log.info("created namespaces, init handler");

        ProtocolHandler handler = pulsar.getProtocolHandlers().protocol("kafka");
        GroupCoordinator groupCoordinator = ((KafkaProtocolHandler) handler).getGroupCoordinator();

        kafkaRequestHandler = new KafkaRequestHandler(
                pulsar,
                (KafkaServiceConfiguration) conf,
                groupCoordinator,
                false,
                getPlainEndPoint());
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockCtx).channel();
        kafkaRequestHandler.ctx = mockCtx;

        serviceAddress = new InetSocketAddress(pulsar.getBindAddress(), kafkaBrokerPort);
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }
}