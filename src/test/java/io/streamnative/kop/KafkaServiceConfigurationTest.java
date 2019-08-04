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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import io.streamnative.kop.utils.ConfigurationUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import org.apache.bookkeeper.client.api.DigestType;

import org.testng.annotations.Test;

/**
 * Pulsar service configuration object.
 */
public class KafkaServiceConfigurationTest {
    @Test
    public void testKopNamespace() {
        String name = "koptenant/kopns";
        KafkaServiceConfiguration configuration = new KafkaServiceConfiguration();
        configuration.setKafkaNamespace(name);
        assertEquals(name, configuration.getKafkaNamespace());
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
}
