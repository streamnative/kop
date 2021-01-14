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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.net.InetAddress;
import java.util.Map;

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.testng.annotations.Test;

/**
 * Tests for EndPoint.
 */
public class EndPointTest {

    @Test
    public void testValidListener() throws Exception {
        EndPoint endPoint = new EndPoint("PLAINTEXT://192.168.0.1:9092");
        assertEquals(endPoint.getSecurityProtocol(), SecurityProtocol.PLAINTEXT);
        assertEquals(endPoint.getHostname(), "192.168.0.1");
        assertEquals(endPoint.getPort(), 9092);

        final String localhost = InetAddress.getLocalHost().getCanonicalHostName();
        endPoint = new EndPoint("SSL://:9094");
        assertEquals(endPoint.getSecurityProtocol(), SecurityProtocol.SSL);
        assertEquals(endPoint.getHostname(), localhost);
        assertEquals(endPoint.getPort(), 9094);
    }

    @Test
    public void testInvalidListener() throws Exception {
        try {
            new EndPoint("hello world");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("listener 'hello world' is invalid"));
        }
        try {
            new EndPoint("pulsar://localhost:6650");
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("No enum constant"));
        }
        try {
            new EndPoint("PLAINTEXT://localhost:65536");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("port 65536 is invalid"));
        }
        try {
            new EndPoint("PLAINTEXT://localhost:-1");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("port -1 is invalid"));
        }
    }

    @Test
    public void testValidListeners() throws Exception {
        final Map<SecurityProtocol, EndPoint> endPointMap =
                EndPoint.parseListeners("PLAINTEXT://localhost:9092,SSL://:9093");
        assertEquals(endPointMap.size(), 2);

        final EndPoint plainEndPoint = endPointMap.get(SecurityProtocol.PLAINTEXT);
        assertNotNull(plainEndPoint);
        assertEquals(plainEndPoint.getSecurityProtocol(), SecurityProtocol.PLAINTEXT);
        assertEquals(plainEndPoint.getHostname(), "localhost");
        assertEquals(plainEndPoint.getPort(), 9092);

        final EndPoint sslEndPoint = endPointMap.get(SecurityProtocol.SSL);
        final String localhost = InetAddress.getLocalHost().getCanonicalHostName();
        assertNotNull(sslEndPoint);
        assertEquals(sslEndPoint.getSecurityProtocol(), SecurityProtocol.SSL);
        assertEquals(sslEndPoint.getHostname(), localhost);
        assertEquals(sslEndPoint.getPort(), 9093);
    }

    @Test
    public void testRepeatedListeners() throws Exception {
        try {
            EndPoint.parseListeners("PLAINTEXT://localhost:9092,SSL://:9093,SSL://localhost:9094");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains(" has multiple listeners whose protocol is SSL"));
        }
    }

    @Test
    public void testGetEndPoint() {
        String listeners = "PLAINTEXT://:9092,SSL://:9093,SASL_SSL://:9094,SASL_PLAINTEXT:9095";
        assertEquals(EndPoint.getPlainTextEndPoint(listeners).getPort(), 9092);
        assertEquals(EndPoint.getSslEndPoint(listeners).getPort(), 9093);
        listeners = "SASL_PLAINTEXT://:9095,SASL_SSL://:9094,SSL://:9093,PLAINTEXT:9092";
        assertEquals(EndPoint.getPlainTextEndPoint(listeners).getPort(), 9095);
        assertEquals(EndPoint.getSslEndPoint(listeners).getPort(), 9094);

        try {
            EndPoint.getPlainTextEndPoint("SSL://:9093");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("has no plain text endpoint"));
        }

        try {
            EndPoint.getSslEndPoint("PLAINTEXT://:9092");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("has no ssl endpoint"));
        }
    }

    @Test
    public void testOriginalUrl() throws Exception {
        final EndPoint endPoint = new EndPoint("PLAINTEXT://:9092");
        assertEquals(endPoint.getHostname(), InetAddress.getLocalHost().getCanonicalHostName());
        assertEquals(endPoint.getOriginalListener(), "PLAINTEXT://:9092");
    }
}
