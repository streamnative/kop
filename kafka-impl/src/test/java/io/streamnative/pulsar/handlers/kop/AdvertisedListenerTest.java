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

import java.net.InetAddress;
import java.net.UnknownHostException;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class AdvertisedListenerTest {

    @Test
    public void testNormalCase() {
        AdvertisedListener al = AdvertisedListener.create("PLAINTEXT://192.168.0.1:9092");
        Assert.assertEquals(al.getListenerName(), "PLAINTEXT");
        Assert.assertEquals(al.getHostname(), "192.168.0.1");
        Assert.assertEquals(al.getPort(), 9092);
    }

    @Test
    public void testEmptyHostName() throws UnknownHostException {
        AdvertisedListener al = AdvertisedListener.create("PLAINTEXT://:9092");
        Assert.assertEquals(al.getListenerName(), "PLAINTEXT");
        Assert.assertEquals(al.getHostname(), InetAddress.getLocalHost().getCanonicalHostName());
        Assert.assertEquals(al.getPort(), 9092);
    }

    @Test
    public void testWrongFormat() {
        try {
            AdvertisedListener.create("PLAINTEXT:/:9092");
            Assert.fail();
        } catch (IllegalStateException ex) {
            log.info("Exception message: {}", ex.getMessage());
            Assert.assertTrue(ex.getMessage().contains("Listener 'PLAINTEXT:/:9092' is invalid"));
        }
        try {
            AdvertisedListener.create("PLAINTEXT9092");
            Assert.fail();
        } catch (IllegalStateException ex) {
            log.info("Exception message: {}", ex.getMessage());
            Assert.assertTrue(ex.getMessage().contains("Listener 'PLAINTEXT9092' is invalid"));
        }
        try {
            AdvertisedListener.create("PLAINTEXT:///192.168.0.1:9092");
            Assert.fail();
        } catch (IllegalStateException ex) {
            log.info("Exception message: {}", ex.getMessage());
            Assert.assertTrue(ex.getMessage().contains("Listener 'PLAINTEXT:///192.168.0.1:9092' is invalid"));
        }
    }

    @Test
    public void testWrongPort() {
        AdvertisedListener.create("PLAINTEXT://192.168.0.1:0");
        AdvertisedListener.create("PLAINTEXT://192.168.0.1:65535");
        try {
            AdvertisedListener.create("PLAINTEXT://192.168.0.1:65536");
            Assert.fail();
        } catch (IllegalStateException ex) {
            log.info("Exception message: {}", ex.getMessage());
            Assert.assertTrue(ex.getMessage().contains("port '65536' is invalid"));
        }
        try {
            AdvertisedListener.create("PLAINTEXT://192.168.0.1:-1");
            Assert.fail();
        } catch (IllegalStateException ex) {
            log.info("Exception message: {}", ex.getMessage());
            Assert.assertTrue(ex.getMessage()
                    .contains("Listener 'PLAINTEXT://192.168.0.1:-1' is invalid: port '-1' is invalid."));
        }
        try {
            AdvertisedListener.create("PLAINTEXT://192.168.0.1");
            Assert.fail();
        } catch (IllegalStateException ex) {
            log.info("Exception message: {}", ex.getMessage());
            Assert.assertTrue(ex.getMessage().contains("Listener 'PLAINTEXT://192.168.0.1' is invalid"));
        }
    }

    @Test
    public void testIpv6() {
        AdvertisedListener advertisedListener = AdvertisedListener.create("PLAINTEXT://[2409:8c85:aa34:1d3::1e]:9092");
        Assert.assertEquals(advertisedListener.getListenerName(), "PLAINTEXT");
        Assert.assertEquals(advertisedListener.getHostname(), "2409:8c85:aa34:1d3:0:0:0:1e");
        Assert.assertEquals(advertisedListener.getPort(), 9092);

        advertisedListener = AdvertisedListener.create("PLAINTEXT://[::1]:9092");
        Assert.assertEquals(advertisedListener.getListenerName(), "PLAINTEXT");
        Assert.assertEquals(advertisedListener.getHostname(), "0:0:0:0:0:0:0:1");
        Assert.assertEquals(advertisedListener.getPort(), 9092);

        advertisedListener = AdvertisedListener.create("PLAINTEXT://::1:9092");
        Assert.assertEquals(advertisedListener.getListenerName(), "PLAINTEXT");
        Assert.assertEquals(advertisedListener.getHostname(), "0:0:0:0:0:0:0:1");
        Assert.assertEquals(advertisedListener.getPort(), 9092);
    }
}