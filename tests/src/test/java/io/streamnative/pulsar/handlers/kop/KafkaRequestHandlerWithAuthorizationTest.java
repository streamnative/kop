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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import io.jsonwebtoken.SignatureAlgorithm;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupCoordinator;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.security.auth.Resource;
import io.streamnative.pulsar.handlers.kop.security.auth.ResourceType;
import io.streamnative.pulsar.handlers.kop.stats.NullStatsLogger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import javax.crypto.SecretKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for {@link KafkaRequestHandler} with authorization enabled.
 */
@Slf4j
public class KafkaRequestHandlerWithAuthorizationTest extends KopProtocolHandlerTestBase {

    private static final String TENANT = "KafkaAuthorizationTest";
    private static final String NAMESPACE = "ns1";
    private static final String SHORT_TOPIC = "topic1";
    private static final String TOPIC = "persistent://" + TENANT + "/" + NAMESPACE + "/" + SHORT_TOPIC;
    private static final int DEFAULT_PARTITION_NUM = 2;

    private static final String ADMIN_USER = "admin_user";

    private KafkaRequestHandler handler;
    private AdminManager adminManager;

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
        conf.setDefaultNumPartitions(DEFAULT_PARTITION_NUM);
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

        super.internalSetup();
        log.info("success internal setup");

        if (!admin.namespaces().getNamespaces(TENANT).contains(TENANT + "/__kafka")) {
            admin.namespaces().createNamespace(TENANT + "/__kafka");
            admin.namespaces().setNamespaceReplicationClusters(TENANT + "/__kafka", Sets.newHashSet("test"));
            admin.namespaces().setRetention(TENANT + "/__kafka",
                    new RetentionPolicies(-1, -1));
        }

        admin.topics().createPartitionedTopic(TOPIC, DEFAULT_PARTITION_NUM);

        log.info("created namespaces, init handler");

        ProtocolHandler handler1 = pulsar.getProtocolHandlers().protocol("kafka");
        GroupCoordinator groupCoordinator = ((KafkaProtocolHandler) handler1).getGroupCoordinator();
        TransactionCoordinator transactionCoordinator = ((KafkaProtocolHandler) handler1).getTransactionCoordinator();

        adminManager = new AdminManager(pulsar.getAdminClient(), conf);
        handler = new KafkaRequestHandler(
                pulsar,
                (KafkaServiceConfiguration) conf,
                groupCoordinator,
                transactionCoordinator,
                adminManager,
                pulsar.getLocalMetadataStore().getMetadataCache(LocalBrokerData.class),
                false,
                getPlainEndPoint(),
                NullStatsLogger.INSTANCE);
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        Channel mockChannel = mock(Channel.class);
        doReturn(mockChannel).when(mockCtx).channel();
        handler.ctx = mockCtx;
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
        adminManager.shutdown();
        super.internalCleanup();
    }

    @DataProvider(name = "metadataVersions")
    public static Object[][] metadataVersions() {
        return new Object[][]{ { (short) 0 }, { (short) 1 } };
    }

    @Test(timeOut = 10000, dataProvider = "metadataVersions")
    public void testMetadataForPartitionedTopicFailed(short version) throws Exception {
        final String topic = "testMetadataForPartitionedTopic-" + version;
        admin.topics().createNonPartitionedTopic("persistent://" + TENANT + "/" + NAMESPACE + "/" + topic);

        final RequestHeader header = new RequestHeader(ApiKeys.METADATA, version, "client", 0);
        final MetadataRequest request =
                new MetadataRequest(Collections.singletonList(topic), false, version);
        final CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        handler.handleTopicMetadataRequest(
                new KafkaCommandDecoder.KafkaHeaderAndRequest(
                        header, request, PulsarByteBufAllocator.DEFAULT.heapBuffer(), null),
                responseFuture);
        final MetadataResponse response = (MetadataResponse) responseFuture.get();
        assertEquals(response.topicMetadata().size(), 1);
        assertEquals(response.errors().size(), 1);
        assertEquals(response.errors().get(topic), Errors.TOPIC_AUTHORIZATION_FAILED);
    }

    @Test(timeOut = 10000, dataProvider = "metadataVersions")
    public void testMetadataForPartitionedTopicSuccess(short version) throws Exception {
        final String topic = TOPIC + "-" + version;
        KafkaRequestHandler spyHandler = spy(handler);
        doReturn(CompletableFuture.completedFuture(true))
                .when(spyHandler)
                .authorize(eq(AclOperation.DESCRIBE), eq(Resource.of(ResourceType.TOPIC, topic)));
        final RequestHeader header = new RequestHeader(ApiKeys.METADATA, version, "client", 0);
        final MetadataRequest request =
                new MetadataRequest(Collections.singletonList(topic), true, version);
        final CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleTopicMetadataRequest(
                new KafkaCommandDecoder.KafkaHeaderAndRequest(
                        header, request, PulsarByteBufAllocator.DEFAULT.heapBuffer(), null),
                responseFuture);
         final MetadataResponse response = (MetadataResponse) responseFuture.get();
        assertEquals(response.topicMetadata().size(), 1);
        assertEquals(response.errors().size(), 0);
    }

    @Test(timeOut = 10000)
    public void testMetadataListTopic() throws Exception {
        final String topic = TOPIC;
        KafkaRequestHandler spyHandler = spy(handler);
        for (int i = 0; i < DEFAULT_PARTITION_NUM; i++) {
            doReturn(CompletableFuture.completedFuture(true))
                    .when(spyHandler)
                    .authorize(eq(AclOperation.DESCRIBE),
                            eq(Resource.of(ResourceType.TOPIC, TopicName.get(topic).getPartition(i).toString()))
                    );
        }

        final RequestHeader header = new RequestHeader(ApiKeys.METADATA, (short) 1, "client", 0);
        final MetadataRequest request =
                new MetadataRequest(Collections.emptyList(), true, (short) 1);
        final CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        spyHandler.handleTopicMetadataRequest(
                new KafkaCommandDecoder.KafkaHeaderAndRequest(
                        header, request, PulsarByteBufAllocator.DEFAULT.heapBuffer(), null),
                responseFuture);
        final MetadataResponse response = (MetadataResponse) responseFuture.get();
        String localName = TopicName.get(topic).getLocalName();

        HashMap<String, MetadataResponse.TopicMetadata> topicMap = new HashMap<>();
        response.topicMetadata().forEach(metadata -> {
            topicMap.put(metadata.topic(), metadata);
        });
        assertTrue(topicMap.containsKey(localName));
        assertEquals(topicMap.get(localName).partitionMetadata().size(), DEFAULT_PARTITION_NUM);
        assertNull(response.errors().get(localName));

        response.errors().forEach((t, errors) -> {
            if (!localName.equals(t)) {
                assertEquals(errors, Errors.TOPIC_AUTHORIZATION_FAILED);
            }
        });
    }


}
