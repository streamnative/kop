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
package io.streamnative.pulsar.handlers.kop.utils;


import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.utils.ssl.SSLUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test SSLUtils.
 */
@Slf4j
public class TestSSLUtils {

    String cipherA = "cipherA";
    String cipherB = "cipherB";

    private File createConfigFileWithCiphers() throws FileNotFoundException {
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("");
        printWriter.println("kopSslCipherSuites=" + cipherA + "," + cipherB);
        printWriter.close();
        testConfigFile.deleteOnExit();
        return testConfigFile;
    }

    @Test
    public void testKConfigTLSCiphers() throws Exception{
        // test KafkaServiceConfiguration get cipher config right from config file.
        KafkaServiceConfiguration kConfig = ConfigurationUtils.create(
            new FileInputStream(createConfigFileWithCiphers()),
            KafkaServiceConfiguration.class);

        Set<String> ciphers = kConfig.getKopSslCipherSuites();
        Assert.assertEquals(ciphers.size(), 2);
        ciphers.forEach(s -> {
            Assert.assertTrue(s.equals(cipherA) || s.equals(cipherB));
            log.debug("cipher: {}", s);
        });

        // test factory set ciphers right.
        SslContextFactory sslContextFactory = SSLUtils.createSslContextFactory(kConfig);
        String[] sslCiphers = sslContextFactory.getIncludeCipherSuites();
        Assert.assertEquals(sslCiphers.length, 2);
        List<String> ciphersList = Lists.newArrayList(sslCiphers);
        ciphers.forEach(c -> {
            Assert.assertTrue(c.equals(cipherA) || c.equals(cipherB));
            log.debug("ssl factory cipher: {}", c);
        });
    }
}
