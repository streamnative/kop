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
package io.streamnative.kop;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
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
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.SameThreadOrderedSafeExecutor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * A test base to start a KafkaService.
 */
@Slf4j
public abstract class MockKafkaServiceBaseTest {

    protected KafkaServiceConfiguration conf;
    protected KafkaService kafkaService;
    protected PulsarAdmin admin;
    protected URL brokerUrl;
    protected URL brokerUrlTls;
    protected URI lookupUrl;
    protected PulsarClient pulsarClient;

    protected final int brokerWebservicePort = PortManager.nextFreePort();
    protected final int brokerWebservicePortTls = PortManager.nextFreePort();
    protected final int brokerPort = PortManager.nextFreePort();
    protected final int kafkaBrokerPort = PortManager.nextFreePort();


    protected MockZooKeeper mockZookKeeper;
    protected NonClosableMockBookKeeper mockBookKeeper;
    protected boolean isTcpLookup = false;
    protected final String configClusterName = "test";

    private SameThreadOrderedSafeExecutor sameThreadOrderedSafeExecutor;
    private ExecutorService bkExecutor;

    public MockKafkaServiceBaseTest() {
        resetConfig();
    }

    protected void resetConfig() {
        this.conf = new KafkaServiceConfiguration();
        this.conf.setKafkaServicePort(Optional.ofNullable(kafkaBrokerPort));
        this.conf.setBrokerServicePort(Optional.ofNullable(brokerPort));
        this.conf.setAdvertisedAddress("localhost");
        this.conf.setWebServicePort(Optional.ofNullable(brokerWebservicePort));
        this.conf.setClusterName(configClusterName);
        this.conf.setAdvertisedAddress("localhost");
        this.conf.setManagedLedgerCacheSizeMB(8);
        this.conf.setActiveConsumerFailoverDelayTimeMillis(0);
        this.conf.setDefaultNumberOfNamespaceBundles(1);
        this.conf.setZookeeperServers("localhost:2181");
        this.conf.setConfigurationStoreServers("localhost:3181");
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

    protected final void init() throws Exception {
        sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();
        bkExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("mock-kafkaService-bk")
                .setUncaughtExceptionHandler((thread, ex) -> log.info("Uncaught exception", ex))
                .build());

        mockZookKeeper = createMockZooKeeper();
        mockBookKeeper = createMockBookKeeper(mockZookKeeper, bkExecutor);

        startBroker();

        brokerUrl = new URL("http://" + kafkaService.getAdvertisedAddress() + ":" + brokerWebservicePort);
        brokerUrlTls = new URL("https://" + kafkaService.getAdvertisedAddress() + ":" + brokerWebservicePortTls);

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString()).build());
    }

    protected final void internalCleanup() throws Exception {
        try {
            // if init fails, some of these could be null, and if so would throw
            // an NPE in shutdown, obscuring the real error
            if (admin != null) {
                admin.close();
            }
            if (pulsarClient != null) {
                pulsarClient.close();
            }
            if (kafkaService != null) {
                kafkaService.close();
            }
            if (mockBookKeeper != null) {
                mockBookKeeper.reallyShutdown();
            }
            if (mockZookKeeper != null) {
                mockZookKeeper.shutdown();
            }
            if (sameThreadOrderedSafeExecutor != null) {
                sameThreadOrderedSafeExecutor.shutdown();
            }
            if (bkExecutor != null) {
                bkExecutor.shutdown();
            }
        } catch (Exception e) {
            log.warn("Failed to clean up mocked kafkaService service:", e);
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
        kafkaService.close();
    }

    protected void startBroker() throws Exception {
        this.kafkaService = startBroker(conf);
    }

    protected KafkaService startBroker(KafkaServiceConfiguration conf) throws Exception {
        KafkaService kafkaService = spy(new KafkaService(conf));

        setupBrokerMocks(kafkaService);
        boolean isAuthorizationEnabled = conf.isAuthorizationEnabled();
        // enable authorization to initialize authorization service which is used by grant-permission
        conf.setAuthorizationEnabled(true);
        kafkaService.start();
        conf.setAuthorizationEnabled(isAuthorizationEnabled);

        Compactor spiedCompactor = spy(kafkaService.getCompactor());
        doReturn(spiedCompactor).when(kafkaService).getCompactor();

        return kafkaService;
    }

    protected void setupBrokerMocks(KafkaService kafkaService) throws Exception {
        // Override default providers with mocked ones
        doReturn(mockZooKeeperClientFactory).when(kafkaService).getZooKeeperClientFactory();
        doReturn(mockBookKeeperClientFactory).when(kafkaService).newBookKeeperClientFactory();

        Supplier<NamespaceService> namespaceServiceSupplier = () -> spy(new NamespaceService(kafkaService));
        doReturn(namespaceServiceSupplier).when(kafkaService).getNamespaceServiceProvider();

        doReturn(sameThreadOrderedSafeExecutor).when(kafkaService).getOrderedExecutor();
    }

    public static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
            "".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList, CreateMode.PERSISTENT);

        zk.create(
            "/ledgers/LAYOUT",
            "1\nflat:1".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList,
            CreateMode.PERSISTENT);
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
            return CompletableFuture.completedFuture(mockZookKeeper);
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
    public class KProducer {
        private final KafkaProducer<Integer, String> producer;
        private final String topic;
        private final Boolean isAsync;

        public KProducer(String topic, Boolean isAsync) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost" + ":" + kafkaBrokerPort);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoKafkaOnPulsarProducer");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producer = new KafkaProducer<>(props);
            this.topic = topic;
            this.isAsync = isAsync;
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
    public class KConsumer {
        private final KafkaConsumer<Integer, String> consumer;
        private final String topic;

        public KConsumer(String topic) {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost" + ":" + kafkaBrokerPort);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoKafkaOnPulsarConsumer");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

            this.consumer = new KafkaConsumer<>(props);
            this.topic = topic;
        }
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
