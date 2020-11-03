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
import static org.testng.Assert.fail;

import io.streamnative.pulsar.handlers.kop.utils.ConfigurationUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.DigestType;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.Assert;
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
    public void testKopListeners() throws Exception {
        KafkaServiceConfiguration config = new KafkaServiceConfiguration();
        String advertisedAddress = "127.0.0.1";
        config.setAdvertisedAddress(advertisedAddress);

        // fail for advertisedAddress not match
        try {
            String inputListener = "PLAINTEXT://hello:9092";
            verifyGetListenersFromConfig(config, inputListener, inputListener);
            fail("Should throw exception for advertisedAddress not match");
        } catch (Exception e) {
            // expected
            log.info("exception: " + e);
        }

        // success match
        try {
            String inputListener = "PLAINTEXT://" + advertisedAddress + ":9092";
            verifyGetListenersFromConfig(config, inputListener, inputListener);
        } catch (Exception e) {
            fail("Should success and equals " + e);
        }

        // success match both no hostname
        try {
            String inputListener = "PLAINTEXT://:9092" + ",SSL://:9093";
            String outputListener = "PLAINTEXT://" + advertisedAddress + ":9092"
                                    + ",SSL://" + advertisedAddress + ":9093";
            verifyGetListenersFromConfig(config, inputListener, outputListener);
        } catch (Exception e) {
            fail("Should success and equals " + e);
        }

        // success match ssl no hostname
        try {
            String inputListener = "PLAINTEXT://" + advertisedAddress + ":9092" + ",SSL://:9093";
            String outputListener = "PLAINTEXT://" + advertisedAddress + ":9092"
                                    + ",SSL://" + advertisedAddress + ":9093";
            verifyGetListenersFromConfig(config, inputListener, outputListener);
        } catch (Exception e) {
            fail("Should success and equals " + e);
        }

        // fail for ssl hostname not match
        try {
            String inputListener = "PLAINTEXT://" + advertisedAddress + ":9092" + ",SSL://" + "hello" + ":9093";
            String outputListener = "PLAINTEXT://" + advertisedAddress + ":9092"
                                    + ",SSL://" + advertisedAddress + ":9093";
            verifyGetListenersFromConfig(config, inputListener, outputListener);
            fail("Should throw exception for ssl advertisedAddress not match");
        } catch (Exception e) {
            // expected
            log.info("exception: " + e);
        }

        // fail for no port
        try {
            String inputListener = "PLAINTEXT://" + advertisedAddress;
            verifyGetListenersFromConfig(config, inputListener, inputListener);
            fail("Should throw exception for no port");
        } catch (Exception e) {
            // expected
            log.info("exception: " + e);
        }

        // fail for wrong port
        try {
            String inputListener = "PLAINTEXT://" + advertisedAddress + ":65537";
            verifyGetListenersFromConfig(config, inputListener, inputListener);
            fail("Should throw exception for wrong port");
        } catch (Exception e) {
            // expected
            log.info("exception: " + e);
        }

        // fail for wrong port
        try {
            String inputListener = "PLAINTEXT://" + advertisedAddress + ":wrongPort";
            verifyGetListenersFromConfig(config, inputListener, inputListener);
            fail("Should throw exception for wrong port");
        } catch (Exception e) {
            // expected
            log.info("exception: " + e);
        }
    }

    private static void verifyGetListenersFromConfig(KafkaServiceConfiguration config,
                                                     String inputListener,
                                                     String outputListener) {

        config.setListeners(inputListener);
        log.info("inputListener: {}, outputListener: {}", inputListener, outputListener);
        Assert.assertEquals(KafkaProtocolHandler.getListenersFromConfig(config), outputListener);
    }

}
