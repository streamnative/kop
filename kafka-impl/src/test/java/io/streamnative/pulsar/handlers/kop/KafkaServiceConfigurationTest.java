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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.pulsar.broker.ServiceConfiguration;
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
        assertEquals(kafkaServiceConfig.getZookeeperServers(), zkServer);
        assertEquals(kafkaServiceConfig.isBrokerDeleteInactiveTopicsEnabled(), true);
        assertEquals(kafkaServiceConfig.getBacklogQuotaDefaultLimitGB(), 18);
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
    public void testAllowedNamespaces() {
        final KafkaServiceConfiguration conf = new KafkaServiceConfiguration();
        assertEquals(conf.getKopAllowedNamespaces(), Collections.singletonList("public/default"));
        conf.setKafkaTenant("my-tenant");
        assertEquals(conf.getKopAllowedNamespaces(), Collections.singletonList("my-tenant/default"));
        conf.setKafkaNamespace("my-ns");
        assertEquals(conf.getKopAllowedNamespaces(), Collections.singletonList("my-tenant/my-ns"));
        conf.setKopAllowedNamespaces(Collections.singleton("my-tenant-2/my-ns-2"));
        assertEquals(conf.getKopAllowedNamespaces(), Collections.singletonList("my-tenant-2/my-ns-2"));
        conf.setKopAllowedNamespaces(null);
        assertEquals(conf.getKopAllowedNamespaces(), Collections.singletonList("my-tenant/my-ns"));
        conf.setKopAllowedNamespaces(Sets.newHashSet("my-tenant/my-ns-0", "my-tenant/my-ns-1"));
        assertEquals(conf.getKopAllowedNamespaces(), Arrays.asList("my-tenant/my-ns-0", "my-tenant/my-ns-1"));
    }
}
