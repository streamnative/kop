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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.utils.ConfigurationUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.awaitility.Awaitility;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.testng.annotations.Test;

/**
 * Pulsar service configuration object.
 */
@Slf4j
public class KafkaServiceConfigurationTest {
    @Test
    public void testKopNamespace() {
        String ts = "koptenant";
        String ns = "kopns";
        KafkaServiceConfiguration configuration = new KafkaServiceConfiguration();
        configuration.setKafkaTenant(ts);
        configuration.setKafkaNamespace(ns);
        assertEquals(ts, configuration.getKafkaTenant());
        assertEquals(ns, configuration.getKafkaNamespace());
    }

    @Test
    public void testMaxReadEntriesNum() {
        int readEntriesNum = 60;
        KafkaServiceConfiguration configuration = new KafkaServiceConfiguration();
        configuration.setMaxReadEntriesNum(readEntriesNum);
        assertEquals(60, configuration.getMaxReadEntriesNum());
    }

    @Test
    public void testKafkaListeners() {
        KafkaServiceConfiguration configuration = new KafkaServiceConfiguration();
        configuration.setListeners("PLAINTEXT://localhost:9092");
        assertEquals(configuration.getListeners(), "PLAINTEXT://localhost:9092");
        // kafkaListeners has higher priority than listeners
        configuration.setKafkaListeners("PLAINTEXT://localhost:9093");
        assertEquals(configuration.getListeners(), "PLAINTEXT://localhost:9093");
    }

    @Test
    public void testKafkaListenersWithoutHostname() throws UnknownHostException {
        KafkaServiceConfiguration configuration = new KafkaServiceConfiguration();
        configuration.setListeners("PLAINTEXT://:9092");
        assertEquals(configuration.getListeners(), "PLAINTEXT://:9092");
        String hostName = InetAddress.getLocalHost().getCanonicalHostName();
        String expectListeners = "PLAINTEXT://" + hostName + ":9092";
        assertEquals(configuration.getKafkaAdvertisedListeners(), expectListeners);
    }

    @Test
    public void testKafkaListenersWithAdvertisedListener() {
        KafkaServiceConfiguration configuration = new KafkaServiceConfiguration();
        configuration.setAdvertisedAddress("advertise-me");
        configuration.setKafkaListeners("PLAINTEXT://0.0.0.0:9092");
        configuration.setKafkaAdvertisedListeners("PLAINTEXT://advertisedAddress:9092");
        assertEquals(configuration.getListeners(), "PLAINTEXT://0.0.0.0:9092");
        String expectAdvertisedListeners = "PLAINTEXT://advertise-me:9092";
        assertEquals(configuration.getKafkaAdvertisedListeners(), expectAdvertisedListeners);
    }

    @Test
    public void testKafkaListenersWithAdvertisedListenerComputedByPulsar() {
        KafkaServiceConfiguration configuration = new KafkaServiceConfiguration();
        // do not set the advertisedAddress on broker.conf
        // Pulsar computes it in its own way
        configuration.setAdvertisedAddress(null);
        configuration.setKafkaListeners("PLAINTEXT://0.0.0.0:9092");
        configuration.setKafkaAdvertisedListeners("PLAINTEXT://advertisedAddress:9092");
        assertEquals(configuration.getListeners(), "PLAINTEXT://0.0.0.0:9092");
        final String localAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(null);
        String expectAdvertisedListeners = "PLAINTEXT://" + localAddress + ":9092";
        assertEquals(configuration.getKafkaAdvertisedListeners(), expectAdvertisedListeners);
    }

    @Test
    public void testKafkaListenersWithAdvertisedListenerSASL() throws UnknownHostException {
        KafkaServiceConfiguration configuration = new KafkaServiceConfiguration();
        configuration.setAdvertisedAddress("advertise-me");
        configuration.setKafkaListeners("SASL_PLAINTEXT://0.0.0.0:9092");
        configuration.setKafkaAdvertisedListeners("SASL_PLAINTEXT://advertisedAddress:9092");
        assertEquals(configuration.getListeners(), "SASL_PLAINTEXT://0.0.0.0:9092");
        String expectAdvertisedListeners = "SASL_PLAINTEXT://advertise-me:9092";
        assertEquals(configuration.getKafkaAdvertisedListeners(), expectAdvertisedListeners);
    }

    @Test
    public void testGroupIdZooKeeperPath() {
        String zkPathForKop = "/consumer_group_test";
        KafkaServiceConfiguration configuration = new KafkaServiceConfiguration();
        configuration.setGroupIdZooKeeperPath(zkPathForKop);
        assertEquals("/consumer_group_test", configuration.getGroupIdZooKeeperPath());
    }

    @Test
    public void testConfigurationUtilsStream() throws Exception {
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        final String zkServer = "z1.example.com,z2.example.com,z3.example.com";
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("zookeeperServers=" + zkServer);
        printWriter.println("configurationStoreServers=gz1.example.com,gz2.example.com,gz3.example.com/foo");
        printWriter.println("brokerDeleteInactiveTopicsEnabled=true");
        printWriter.println("statusFilePath=/tmp/status.html");
        printWriter.println("managedLedgerDefaultEnsembleSize=1");
        printWriter.println("backlogQuotaDefaultLimitGB=18");
        printWriter.println("clusterName=usc");
        printWriter.println("brokerClientAuthenticationPlugin=test.xyz.client.auth.plugin");
        printWriter.println("brokerClientAuthenticationParameters=role:my-role");
        printWriter.println("superUserRoles=appid1,appid2");
        printWriter.println("brokerServicePort=7777");
        printWriter.println("brokerServicePortTls=8777");
        printWriter.println("webServicePort=");
        printWriter.println("webServicePortTls=");
        printWriter.println("managedLedgerDefaultMarkDeleteRateLimit=5.0");
        printWriter.println("managedLedgerDigestType=CRC32C");
        printWriter.println("kopAllowedNamespaces=public/default,public/__kafka");
        printWriter.println("kafkaListenerName=external");

        printWriter.close();
        testConfigFile.deleteOnExit();

        InputStream stream = new FileInputStream(testConfigFile);
        final KafkaServiceConfiguration kafkaServiceConfig =
            ConfigurationUtils.create(stream, KafkaServiceConfiguration.class);

        assertNotNull(kafkaServiceConfig);
        assertEquals(kafkaServiceConfig.getMetadataStoreUrl(), "zk:" + zkServer);
        assertTrue(kafkaServiceConfig.isBrokerDeleteInactiveTopicsEnabled());
        assertEquals(kafkaServiceConfig.getBacklogQuotaDefaultLimitGB(), 18.0);
        assertEquals(kafkaServiceConfig.getClusterName(), "usc");
        assertEquals(kafkaServiceConfig.getBrokerClientAuthenticationParameters(), "role:my-role");
        assertEquals(kafkaServiceConfig.getBrokerServicePort().get(), new Integer(7777));
        assertEquals(kafkaServiceConfig.getBrokerServicePortTls().get(), new Integer(8777));
        assertFalse(kafkaServiceConfig.getWebServicePort().isPresent());
        assertFalse(kafkaServiceConfig.getWebServicePortTls().isPresent());
        assertEquals(kafkaServiceConfig.getManagedLedgerDigestType(), DigestType.CRC32C);
        assertEquals(
                kafkaServiceConfig.getKopAllowedNamespaces(), Sets.newHashSet("public/default", "public/__kafka"));
    }

    @Test
    public void testConfigurationChangedByServiceConfiguration() throws Exception {
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        final String advertisedAddress1 = "advertisedAddress1";
        final String advertisedAddress2 = "advertisedAddress2";

        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("advertisedAddress=" + advertisedAddress1);
        printWriter.close();
        testConfigFile.deleteOnExit();

        InputStream stream = new FileInputStream(testConfigFile);
        final ServiceConfiguration serviceConfiguration =
                ConfigurationUtils.create(stream, ServiceConfiguration.class);

        serviceConfiguration.setAdvertisedAddress(advertisedAddress2);

        final KafkaServiceConfiguration kafkaServiceConfig = ConfigurationUtils
                .create(serviceConfiguration.getProperties(), KafkaServiceConfiguration.class);

        assertEquals(kafkaServiceConfig.getAdvertisedAddress(), advertisedAddress1);
        assertEquals(serviceConfiguration.getAdvertisedAddress(), advertisedAddress2);
    }

    @Test
    public void testGetKopOauth2Configs() {
        // Read kop-oauth2.properties
        final KafkaServiceConfiguration conf = new KafkaServiceConfiguration();
        conf.setKopOauth2ConfigFile("src/test/resources/kop-oauth2.properties");
        Properties oauth2Props = conf.getKopOauth2Properties();
        Properties expectedProps = new Properties();
        expectedProps.setProperty("unsecuredLoginStringClaim_sub", "admin");
        assertEquals(oauth2Props, expectedProps);

        // Read kop-oauth2-another.properties
        conf.setKopOauth2ConfigFile("src/test/resources/kop-oauth2-another.properties");
        oauth2Props = conf.getKopOauth2Properties();
        expectedProps = new Properties();
        expectedProps.setProperty("unsecuredLoginStringClaim_sub", "another-user");
        assertEquals(oauth2Props, expectedProps);

        conf.setKopOauth2ConfigFile("src/test/resources/not-existed.properties");
        final Properties emptyProps = conf.getKopOauth2Properties();
        assertTrue(emptyProps.isEmpty());
    }

    @Test
    public void testAllowedNamespaces() throws Exception {
        final KafkaServiceConfiguration conf = new KafkaServiceConfiguration();
        assertEquals(conf.getKopAllowedNamespaces(), Collections.singletonList("${tenant}/default"));

        assertEquals(KafkaRequestHandler.expandAllowedNamespaces(conf.getKopAllowedNamespaces(),
                        "my-tenant", null).get(),
                Collections.singletonList("my-tenant/default"));
        conf.setKafkaNamespace("my-ns");

        assertEquals(KafkaRequestHandler.expandAllowedNamespaces(conf.getKopAllowedNamespaces(),
                        "my-tenant", null).get(),
                Collections.singletonList("my-tenant/my-ns"));

        conf.setKopAllowedNamespaces(Collections.singleton("my-tenant-2/my-ns-2"));
        assertEquals(KafkaRequestHandler.expandAllowedNamespaces(conf.getKopAllowedNamespaces(),
                        "my-tenant", null).get(),
                Collections.singletonList("my-tenant-2/my-ns-2"));

        conf.setKopAllowedNamespaces(null);
        assertEquals(KafkaRequestHandler.expandAllowedNamespaces(conf.getKopAllowedNamespaces(),
                        "my-tenant", null).get(),
                Collections.singletonList("my-tenant/my-ns"));

        conf.setKopAllowedNamespaces(Sets.newHashSet("my-tenant/my-ns-0", "my-tenant/my-ns-1"));
        assertEquals(KafkaRequestHandler.expandAllowedNamespaces(conf.getKopAllowedNamespaces(),
                        "my-tenant", null).get(),
                Arrays.asList("my-tenant/my-ns-0", "my-tenant/my-ns-1"));

        conf.setKopAllowedNamespaces(Collections.singleton("${tenant}/*"));

        PulsarService pulsarService = mock(PulsarService.class);
        PulsarResources pulsarResources = mock(PulsarResources.class);
        NamespaceResources namespaceResources = mock(NamespaceResources.class);
        when(pulsarService.getPulsarResources()).thenReturn(pulsarResources);
        when(pulsarResources.getNamespaceResources()).thenReturn(namespaceResources);
        when(namespaceResources.listNamespacesAsync(any(String.class)))
                .thenReturn(CompletableFuture.completedFuture(Arrays.asList("one", "two")));
        assertEquals(KafkaRequestHandler.expandAllowedNamespaces(conf.getKopAllowedNamespaces(),
                        "logged", pulsarService).get(),
                Arrays.asList("logged/one", "logged/two"));

    }

    @Test
    public void testKopMigrationServiceConfiguration() {
        int port = 8005;
        KafkaServiceConfiguration configuration = new KafkaServiceConfiguration();
        configuration.setKopMigrationEnable(true);
        configuration.setKopMigrationServicePort(port);
        assertTrue(configuration.isKopMigrationEnable());
        assertEquals(port, configuration.getKopMigrationServicePort());
    }

    @Test(timeOut = 10000)
    public void testKopAuthorizationCache() throws InterruptedException {
        KafkaServiceConfiguration configuration = new KafkaServiceConfiguration();
        configuration.setKopAuthorizationCacheRefreshMs(100);
        configuration.setKopAuthorizationCacheMaxCountPerConnection(5);
        LoadingCache<Integer, Integer> cache = configuration.getAuthorizationCacheBuilder().build(new CacheLoader<>() {
            @Override
            public @Nullable Integer load(Integer integer) {
                return null;
            }
        });
        for (int i = 0; i < 5; i++) {
            assertNull(cache.get(1));
        }
        for (int i = 0; i < 10; i++) {
            cache.put(i, i + 100);
        }
        Awaitility.await().atMost(Duration.ofMillis(10)).pollInterval(Duration.ofMillis(1))
                .until(() -> IntStream.range(0, 10).mapToObj(cache::get).filter(Objects::nonNull).count() <= 5);
        IntStream.range(0, 10).mapToObj(cache::get).filter(Objects::nonNull).map(i -> i - 100).forEach(key ->
                assertEquals(cache.get(key).intValue(), key + 100));

        Thread.sleep(200); // wait until the cache expired
        for (int i = 0; i < 10; i++) {
            assertNull(cache.get(i));
        }

        configuration.setKopAuthorizationCacheRefreshMs(0);
        LoadingCache<Integer, Integer> cache2 = configuration.getAuthorizationCacheBuilder().build(new CacheLoader<>() {
            @Override
            public @Nullable Integer load(Integer integer) {
                return null;
            }
        });
        for (int i = 0; i < 5; i++) {
            cache2.put(i, i);
        }
        Awaitility.await().atMost(Duration.ofMillis(10)).pollInterval(Duration.ofMillis(1))
                .until(() -> IntStream.range(0, 5).mapToObj(cache2::get).noneMatch(Objects::nonNull));
    }
}
