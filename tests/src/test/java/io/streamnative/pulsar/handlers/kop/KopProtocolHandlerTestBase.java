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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.streamnative.pulsar.handlers.kop.utils.MetadataUtils;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.SameThreadOrderedSafeExecutor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.eclipse.jetty.server.Server;

/**
 * Unit test to test KoP handler.
 */
@Slf4j
public abstract class KopProtocolHandlerTestBase {

    protected KafkaServiceConfiguration conf;
    protected PulsarService pulsar;
    protected PulsarAdmin admin;
    protected URL brokerUrl;
    protected URL brokerUrlTls;
    protected URI lookupUrl;
    protected PulsarClient pulsarClient;

    protected int brokerWebservicePort = PortManager.nextFreePort();
    protected int brokerWebservicePortTls = PortManager.nextFreePort();
    @Getter
    protected int brokerPort = PortManager.nextFreePort();
    @Getter
    protected int kafkaBrokerPort = PortManager.nextFreePort();
    @Getter
    protected int kafkaBrokerPortTls = PortManager.nextFreePort();
    @Getter
    protected int kafkaSchemaRegistryPort = PortManager.nextFreePort();

    protected MockZooKeeper mockZooKeeper;
    protected NonClosableMockBookKeeper mockBookKeeper;
    protected boolean isTcpLookup = false;
    protected final String configClusterName = "test";

    protected final String tenant = "public";
    protected final String namespace = "default";

    private SameThreadOrderedSafeExecutor sameThreadOrderedSafeExecutor;
    private ExecutorService bkExecutor;

    // Fields about Confluent Schema Registry
    protected boolean enableSchemaRegistry = false;
    private static final String KAFKASTORE_TOPIC = SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC;
    protected SchemaRegistryRestApplication restApp;
    protected Server restServer;
    protected String restConnect;

    private final String entryFormat;

    protected static final String PLAINTEXT_PREFIX = SecurityProtocol.PLAINTEXT.name() + "://";
    protected static final String SSL_PREFIX = SecurityProtocol.SSL.name() + "://";

    public KopProtocolHandlerTestBase() {
        this.entryFormat = "pulsar";
        resetConfig();
    }

    public KopProtocolHandlerTestBase(final String entryFormat) {
        this.entryFormat = entryFormat;
        resetConfig();
    }

    protected EndPoint getPlainEndPoint() {
        return new EndPoint(PLAINTEXT_PREFIX + "127.0.0.1:" + kafkaBrokerPort);
    }

    protected void resetConfig() {
        KafkaServiceConfiguration kafkaConfig = new KafkaServiceConfiguration();
        addBrokerEntryMetadataInterceptors(kafkaConfig);
        kafkaConfig.setBrokerServicePort(Optional.ofNullable(brokerPort));
        kafkaConfig.setAdvertisedAddress("localhost");
        kafkaConfig.setWebServicePort(Optional.ofNullable(brokerWebservicePort));
        kafkaConfig.setClusterName(configClusterName);

        kafkaConfig.setManagedLedgerCacheSizeMB(8);
        kafkaConfig.setActiveConsumerFailoverDelayTimeMillis(0);
        kafkaConfig.setDefaultRetentionTimeInMinutes(7);
        kafkaConfig.setDefaultNumberOfNamespaceBundles(1);
        kafkaConfig.setZookeeperServers("localhost:2181");
        kafkaConfig.setConfigurationStoreServers("localhost:3181");

        kafkaConfig.setAuthenticationEnabled(false);
        kafkaConfig.setAuthorizationEnabled(false);
        kafkaConfig.setAllowAutoTopicCreation(true);
        kafkaConfig.setAllowAutoTopicCreationType("partitioned");
        kafkaConfig.setBrokerDeleteInactiveTopicsEnabled(false);

        kafkaConfig.setKafkaMetadataTenant(tenant);
        kafkaConfig.setKafkaMetadataNamespace(namespace);

        // kafka related settings.
        kafkaConfig.setEnableGroupCoordinator(true);
        kafkaConfig.setOffsetsTopicNumPartitions(1);
        kafkaConfig.setKafkaListeners(
                PLAINTEXT_PREFIX + "localhost:" + kafkaBrokerPort + ","
                        + SSL_PREFIX + "localhost:" + kafkaBrokerPortTls);
        kafkaConfig.setEntryFormat(entryFormat);

        // set protocol related config
        URL testHandlerUrl = this.getClass().getClassLoader().getResource("test-protocol-handler.nar");
        Path handlerPath;
        try {
            handlerPath = Paths.get(testHandlerUrl.toURI());
        } catch (Exception e) {
            log.error("failed to get handler Path, handlerUrl: {}. Exception: ", testHandlerUrl, e);
            return;
        }

        String protocolHandlerDir = handlerPath.toFile().getParent();

        kafkaConfig.setProtocolHandlerDirectory(
            protocolHandlerDir
        );
        kafkaConfig.setMessagingProtocols(Sets.newHashSet("kafka"));

        this.conf = kafkaConfig;
    }

    protected final void internalSetup() throws Exception {
        init();
        lookupUrl = new URI(brokerUrl.toString());
        if (isTcpLookup) {
            lookupUrl = new URI("broker://localhost:" + brokerPort);
        }
        pulsarClient = newPulsarClient(lookupUrl.toString(), 0);
    }

    protected PulsarClient newPulsarClient(String url, int intervalInSecs) throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(url).statsInterval(intervalInSecs, TimeUnit.SECONDS).build();
    }

    protected void createAdmin() throws Exception {
        this.admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString()).build());
    }

    protected final void init() throws Exception {
        sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();
        bkExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("mock-pulsar-bk")
                .setUncaughtExceptionHandler((thread, ex) -> log.info("Uncaught exception", ex))
                .build());

        brokerUrl = new URL("http://" + this.conf.getAdvertisedAddress() + ":" + brokerWebservicePort);
        brokerUrlTls = new URL("https://" + this.conf.getAdvertisedAddress() + ":" + brokerWebservicePortTls);

        String serviceUrl = "http://" + this.conf.getAdvertisedAddress() + ":" + brokerWebservicePort;
        String serviceUrlTls = "https://" + this.conf.getAdvertisedAddress() + ":" + brokerWebservicePortTls;
        String brokerServiceUrl = "pulsar://" + this.conf.getAdvertisedAddress() + ":" + brokerPort;
        String brokerServiceUrlTls = null; // TLS not supported at this time

        mockZooKeeper = createMockZooKeeper(configClusterName, serviceUrl, serviceUrlTls, brokerServiceUrl,
            brokerServiceUrlTls);
        mockBookKeeper = createMockBookKeeper(mockZooKeeper, bkExecutor);

        startBroker();

        createAdmin();

        MetadataUtils.createKafkaMetadataIfMissing(admin, this.conf);

        if (enableSchemaRegistry) {
            admin.topics().createPartitionedTopic(KAFKASTORE_TOPIC, 1);
            final Properties props = new Properties();
            props.put(SchemaRegistryConfig.PORT_CONFIG, Integer.toString(getKafkaSchemaRegistryPort()));
            // Increase the kafkastore.timeout.ms (default: 500) to avoid test failure in CI
            props.put(SchemaRegistryConfig.KAFKASTORE_TIMEOUT_CONFIG, 3000);
            // NOTE: KoP doesn't support kafkastore.connection.url
            props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG,
                    "PLAINTEXT://localhost:" + getKafkaBrokerPort());
            props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, KAFKASTORE_TOPIC);
            props.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, AvroCompatibilityLevel.NONE.name);
            props.put(SchemaRegistryConfig.MASTER_ELIGIBILITY, true);

            restApp = new SchemaRegistryRestApplication(props);
            restServer = restApp.createServer();
            restServer.start();
            restConnect = restServer.getURI().toString();
            if (restConnect.endsWith("/")) {
                restConnect = restConnect.substring(0, restConnect.length() - 1);
            }
        }
    }

    protected final void internalCleanup() throws Exception {
        try {
            // if init fails, some of these could be null, and if so would throw
            // an NPE in shutdown, obscuring the real error
            if (restServer != null) {
                restServer.stop();
                restServer.join();
            }
            if (admin != null) {
                admin.close();
            }
            if (pulsarClient != null) {
                pulsarClient.close();
            }
            if (pulsar != null) {
                stopBroker();
            }
            if (mockBookKeeper != null) {
                mockBookKeeper.reallyShutdown();
            }
            if (mockZooKeeper != null) {
                mockZooKeeper.shutdown();
            }
            if (sameThreadOrderedSafeExecutor != null) {
                sameThreadOrderedSafeExecutor.shutdown();
            }
            if (bkExecutor != null) {
                bkExecutor.shutdown();
            }
        } catch (Exception e) {
            log.warn("Failed to clean up mocked pulsar service:", e);
            throw e;
        }
    }

    protected abstract void setup() throws Exception;

    protected abstract void cleanup() throws Exception;

    protected void restartBroker() throws Exception {
        stopBroker();
        startBroker();
    }

    protected void stopBroker() throws Exception {
        pulsar.close();
    }

    protected void startBroker() throws Exception {
        this.pulsar = startBroker(conf);
    }

    protected PulsarService startBroker(ServiceConfiguration conf) throws Exception {
        addBrokerEntryMetadataInterceptors(conf);
        PulsarService pulsar = spy(new PulsarService(conf));
        setupBrokerMocks(pulsar);
        pulsar.start();

        Compactor spiedCompactor = spy(pulsar.getCompactor());
        doReturn(spiedCompactor).when(pulsar).getCompactor();

        return pulsar;
    }

    protected void setupBrokerMocks(PulsarService pulsar) throws Exception {
        // Override default providers with mocked ones
        doReturn(mockZooKeeperClientFactory).when(pulsar).getZooKeeperClientFactory();
        doReturn(mockBookKeeperClientFactory).when(pulsar).newBookKeeperClientFactory();
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(pulsar).createLocalMetadataStore();

        Supplier<NamespaceService> namespaceServiceSupplier = () -> spy(new NamespaceService(pulsar));
        doReturn(namespaceServiceSupplier).when(pulsar).getNamespaceServiceProvider();

        doReturn(sameThreadOrderedSafeExecutor).when(pulsar).getOrderedExecutor();
    }

    public static MockZooKeeper createMockZooKeeper(String clusterName, String brokerUrl, String brokerUrlTls,
            String brokerServiceUrl, String brokerServiceUrlTls) throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
            "".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList, CreateMode.PERSISTENT);

        zk.create(
            "/ledgers/LAYOUT",
            "1\nflat:1".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList,
            CreateMode.PERSISTENT);

        ZkUtils.createFullPathOptimistic(zk, "/admin/clusters/" + clusterName,
            String.format("{\"serviceUrl\" : \"%s\", \"serviceUrlTls\" : \"%s\", \"brokerServiceUrl\" : \"%s\","
            + "\"brokerServiceUrlTls\" : \"%s\"}", brokerUrl, brokerUrlTls, brokerServiceUrl, brokerServiceUrlTls)
                .getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList, CreateMode.PERSISTENT);

        return zk;
    }

    public static NonClosableMockBookKeeper createMockBookKeeper(ZooKeeper zookeeper,
                                                                 ExecutorService executor) throws Exception {
        return spy(new NonClosableMockBookKeeper(zookeeper, executor));
    }

    /**
     * Prevent the MockBookKeeper instance from being closed when the broker is restarted within a test.
     */
    public static class NonClosableMockBookKeeper extends PulsarMockBookKeeper {

        public NonClosableMockBookKeeper(ZooKeeper zk, ExecutorService executor) throws Exception {
            super(zk, executor);
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public void shutdown() {
            // no-op
        }

        public void reallyShutdown() {
            super.shutdown();
        }
    }

    protected ZooKeeperClientFactory mockZooKeeperClientFactory = new ZooKeeperClientFactory() {

        @Override
        public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType,
                                                   int zkSessionTimeoutMillis) {
            // Always return the same instance
            // (so that we don't loose the mock ZK content on broker restart
            return CompletableFuture.completedFuture(mockZooKeeper);
        }
    };

    private BookKeeperClientFactory mockBookKeeperClientFactory = new BookKeeperClientFactory() {

        @Override
        public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient,
                                 Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                                 Map<String, Object> properties) {
            // Always return the same instance (so that we don't loose the mock BK content on broker restart
            return mockBookKeeper;
        }

        @Override
        public BookKeeper create(ServiceConfiguration serviceConfiguration, ZooKeeper zooKeeper,
                                 Optional<Class<? extends EnsemblePlacementPolicy>> optional,
                                 Map<String, Object> map, StatsLogger statsLogger) throws IOException {
            return mockBookKeeper;
        }

        @Override
        public void close() {
            // no-op
        }
    };

    public static void retryStrategically(Predicate<Void> predicate, int retryCount, long intSleepTimeInMillis)
        throws Exception {
        for (int i = 0; i < retryCount; i++) {
            if (predicate.test(null) || i == (retryCount - 1)) {
                break;
            }
            Thread.sleep(intSleepTimeInMillis + (intSleepTimeInMillis * i));
        }
    }

    public static void setFieldValue(Class clazz, Object classObj, String fieldName, Object fieldValue)
        throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(classObj, fieldValue);
    }

    /**
     * A producer wrapper.
     */
    @Getter
    public static class KProducer implements Closeable {
        private final KafkaProducer producer;
        private final String topic;
        private final Boolean isAsync;

        public KProducer(String topic, Boolean isAsync, String host,
                         int port, String username, String password,
                         Boolean retry, String keySer, String valueSer) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoKafkaOnPulsarProducer");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSer);
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);

            if (retry) {
                props.put(ProducerConfig.RETRIES_CONFIG, 3);
                props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
            }

            if (null != username && null != password) {
                String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule "
                    + "required username=\"%s\" password=\"%s\";";
                String jaasCfg = String.format(jaasTemplate, username, password);
                props.put("sasl.jaas.config", jaasCfg);
                props.put("security.protocol", "SASL_PLAINTEXT");
                props.put("sasl.mechanism", "PLAIN");
            }

            producer = new KafkaProducer<>(props);
            this.topic = topic;
            this.isAsync = isAsync;
        }

        public KProducer(String topic, Boolean isAsync, String host,
                         int port, String username, String password) {
            this(topic, isAsync, host, port, username, password, false,
                    IntegerSerializer.class.getName(), StringSerializer.class.getName());
        }

        public KProducer(String topic, Boolean isAsync, String host, int port) {
            this(topic, isAsync, "localhost", port, null, null, false,
                    IntegerSerializer.class.getName(), StringSerializer.class.getName());
        }

        public KProducer(String topic, Boolean isAsync, int port, Boolean retry) {
            this(topic, isAsync, "localhost", port, null, null, retry,
                    IntegerSerializer.class.getName(), StringSerializer.class.getName());
        }


        public KProducer(String topic, Boolean isAsync, int port) {
            this(topic, isAsync, "localhost", port);
        }

        public KProducer(String topic, Boolean isAsync, int port, String keySer, String valueSer) {
            this(topic, isAsync, "localhost", port, null, null, false,
                    keySer, valueSer);
        }

        @Override
        public void close() {
            this.producer.close();
        }
    }

    /**
     * A callback wrapper for produce async.
     */
    class DemoCallBack implements Callback {

        private final long startTime;
        private final int key;
        private final String message;

        public DemoCallBack(long startTime, int key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        /**
         * A callback method the user can implement to provide asynchronous handling of request completion.
         * This method will be called when the record sent to the server has been acknowledged.
         * Exactly one of the arguments will be non-null.
         *
         * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
         *                  occurred.
         * @param exception The exception thrown during processing of this record. Null if no error occurred.
         */
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (metadata != null) {
                System.out.println(
                    "message(" + key + ", " + message + ") sent to partition(" + metadata.partition()
                        + "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
            } else {
                exception.printStackTrace();
            }
        }
    }


    /**
     * A consumer wrapper.
     */
    @Getter
    public static class KConsumer implements Closeable {
        private final KafkaConsumer consumer;
        private final String topic;
        private final String consumerGroup;

        public KConsumer(
            String topic, String host, int port,
            boolean autoCommit, String username, String password,
            String consumerGroup, String keyDeser, String valueDeser) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            if (autoCommit) {
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
                props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            } else {
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            }

            if (null != username && null != password) {
                String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule "
                    + "required username=\"%s\" password=\"%s\";";
                String jaasCfg = String.format(jaasTemplate, username, password);
                props.put("sasl.jaas.config", jaasCfg);
                props.put("security.protocol", "SASL_PLAINTEXT");
                props.put("sasl.mechanism", "PLAIN");
            }

            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                keyDeser);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                valueDeser);

            this.consumer = new KafkaConsumer<>(props);
            this.topic = topic;
            this.consumerGroup = consumerGroup;
        }

        public KConsumer(String topic, String host, int port, boolean autoCommit,
                         String username, String password, String consumerGroup) {
            this(topic, host, port, autoCommit, username, password, consumerGroup,
                    "org.apache.kafka.common.serialization.IntegerDeserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
        }

        public KConsumer(String topic, int port, boolean autoCommit, String consumerGroup) {
            this(topic, "localhost", port, autoCommit, null, null, consumerGroup,
                    "org.apache.kafka.common.serialization.IntegerDeserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
        }

        public KConsumer(String topic, int port, boolean autoCommit) {
            this(topic, "localhost", port, autoCommit, null, null, "DemoKafkaOnPulsarConsumer",
                    "org.apache.kafka.common.serialization.IntegerDeserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
        }

        public KConsumer(String topic, String host, int port) {
            this(topic, "localhost", port, false, null, null, "DemoKafkaOnPulsarConsumer",
                    "org.apache.kafka.common.serialization.IntegerDeserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
        }

        public KConsumer(String topic, int port) {
            this(topic, "localhost", port);
        }

        public KConsumer(String topic, int port, String group) {
            this(topic, "localhost", port, false, null, null, group,
                    "org.apache.kafka.common.serialization.IntegerDeserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
        }

        public KConsumer(String topic, int port, String keyDeser, String valueDeser) {
            this(topic, "localhost", port, false, null, null,
                    "DemoKafkaOnPulsarConsumer", keyDeser, valueDeser);
        }

        @Override
        public void close() {
            this.consumer.close();
        }
    }

    public static void addBrokerEntryMetadataInterceptors(ServiceConfiguration configuration) {
        Set<String> interceptorNames = new HashSet<>();
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor");
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor");
        configuration.setBrokerEntryMetadataInterceptors(interceptorNames);
    }

    public static Integer kafkaIntDeserialize(byte[] data) {
        if (data == null) {
            return null;
        }

        if (data.length != 4) {
            throw new SerializationException("Size of data received by IntegerDeserializer is not 4");
        }

        int value = 0;
        for (byte b : data) {
            value <<= 8;
            value |= b & 0xFF;
        }
        return value;
    }

    public static byte[] kafkaIntSerialize(Integer data) {
        if (data == null) {
            return null;
        }

        return new byte[] {
            (byte) (data >>> 24),
            (byte) (data >>> 16),
            (byte) (data >>> 8),
            data.byteValue()
        };
    }

}
