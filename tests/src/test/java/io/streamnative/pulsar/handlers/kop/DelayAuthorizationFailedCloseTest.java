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

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import io.streamnative.kafka.client.api.KafkaVersion;
import io.streamnative.kafka.client.api.ProducerConfiguration;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.Properties;
import javax.crypto.SecretKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test delay close handler when authorization failed.
 */
@Slf4j
public class DelayAuthorizationFailedCloseTest extends KopProtocolHandlerTestBase {

    private static final String TENANT = "DelayAuthorizationFailedCloseTest";
    private static final String NAMESPACE = "ns1";

    private static final int FAILED_AUTHENTICATION_DELAY_MS = 300;
    private static final String ADMIN_USER = "admin_user";
    protected static final int BUFFER_SIZE = 4 * 1024;
    private static final long DEFAULT_CONNECTION_MAX_IDLE_MS = 9 * 60 * 1000;

    private Selector selector;
    private Time time;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

        AuthenticationProviderToken provider = new AuthenticationProviderToken();

        Properties properties = new Properties();
        properties.setProperty("tokenSecretKey", AuthTokenUtils.encodeKeyBase64(secretKey));
        ServiceConfiguration authConf = new ServiceConfiguration();
        authConf.setProperties(properties);
        provider.initialize(authConf);

        String adminToken = AuthTokenUtils.createToken(secretKey, ADMIN_USER, Optional.empty());

        super.resetConfig();
        conf.setSaslAllowedMechanisms(Sets.newHashSet("PLAIN"));
        conf.setKafkaMetadataTenant("internal");
        conf.setKafkaMetadataNamespace("__kafka");
        conf.setKafkaTenant(TENANT);
        conf.setKafkaNamespace(NAMESPACE);

        conf.setClusterName(super.configClusterName);
        conf.setAuthorizationEnabled(true);
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationAllowWildcardsMatching(true);
        conf.setSuperUserRoles(Sets.newHashSet(ADMIN_USER));
        conf.setAuthenticationProviders(
                Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters("token:" + adminToken);
        conf.setProperties(properties);
        conf.setFailedAuthenticationDelayMs(FAILED_AUTHENTICATION_DELAY_MS);

        super.internalSetup();
        log.info("success internal setup");

        if (!admin.namespaces().getNamespaces(TENANT).contains(TENANT + "/__kafka")) {
            admin.namespaces().createNamespace(TENANT + "/__kafka");
            admin.namespaces().setNamespaceReplicationClusters(TENANT + "/__kafka", Sets.newHashSet("test"));
            admin.namespaces().setRetention(TENANT + "/__kafka",
                    new RetentionPolicies(-1, -1));
        }
        log.info("created namespaces, init handler");

        time = Time.SYSTEM;
        Metrics metrics = new Metrics(time);
        ProducerConfiguration producerConfiguration = producerConfiguration();
        ChannelBuilder channelBuilder =
                ClientUtils.createChannelBuilder(
                        new ProducerConfig(producerConfiguration.toProperties()), time, new LogContext());
        String clientId = "clientId";
        selector = new Selector(
                DEFAULT_CONNECTION_MAX_IDLE_MS,
                metrics,
                time,
                "test-selector",
                channelBuilder,
                new LogContext(String.format("[Test Selector clientId=%s] ", clientId)));
    }

    @Override
    protected void createAdmin() throws Exception {
        super.admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString())
                .authentication(this.conf.getBrokerClientAuthenticationPlugin(),
                        this.conf.getBrokerClientAuthenticationParameters()).build());
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000)
    void testClientConnectionClose() throws IOException {
        String id = "0";
        blockingConnect(id);
        KafkaChannel channel = selector.channel(id);
        assertTrue(channel.isConnected());

        log.info("Channel is connected: [{}]", channel.isConnected());

        long startTimeMs = time.milliseconds();

        // Send Metadata request
        MetadataRequest.Builder builder = MetadataRequest.Builder.allTopics();
        AbstractRequest request = builder.build();
        selector.send(new NetworkSend(id, request.toSend(
                new RequestHeader(builder.apiKey(), request.version(), "fake_client_id", 0))));

        Awaitility.await().until(() -> {
            poll(selector);
            return !selector.channels().isEmpty();
        });

        // Wait until handler close.
        Awaitility.await().until(() -> {
            poll(selector);
            return selector.channels().isEmpty();
        });

        assertTrue(time.milliseconds() >= startTimeMs + FAILED_AUTHENTICATION_DELAY_MS);
    }


    protected ProducerConfiguration producerConfiguration() {
        return ProducerConfiguration.builder()
                .bootstrapServers("localhost:" + getKafkaBrokerPort())
                .keySerializer(KafkaVersion.DEFAULT.getStringSerializer())
                .valueSerializer(KafkaVersion.DEFAULT.getStringSerializer())
                .build();
    }

    // connect and wait for the connection to complete
    private void blockingConnect(String node) throws IOException {
        blockingConnect(node, new InetSocketAddress("localhost", getKafkaBrokerPort()));
    }

    protected void blockingConnect(String node, InetSocketAddress serverAddr) throws IOException {
        selector.connect(node, serverAddr, BUFFER_SIZE, BUFFER_SIZE);
        while (!selector.connected().contains(node)) {
            selector.poll(10000L);
        }
        while (!selector.isChannelReady(node)) {
            selector.poll(10000L);
        }
    }

    private void poll(Selector selector) {
        try {
            selector.poll(50);
        } catch (IOException e) {
            fail("Caught unexpected exception " + e);
        }
    }
}
