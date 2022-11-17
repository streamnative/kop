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
        EndPoint endPoint = new EndPoint("PLAINTEXT://192.168.0.1:9092", null);
        assertEquals(endPoint.getSecurityProtocol(), SecurityProtocol.PLAINTEXT);
        assertEquals(endPoint.getHostname(), "192.168.0.1");
        assertEquals(endPoint.getPort(), 9092);

        final String localhost = InetAddress.getLocalHost().getCanonicalHostName();
        endPoint = new EndPoint("SSL://:9094", null);
        assertEquals(endPoint.getSecurityProtocol(), SecurityProtocol.SSL);
        assertEquals(endPoint.getHostname(), localhost);
        assertEquals(endPoint.getPort(), 9094);
    }

    @Test
    public void testInvalidListener() throws Exception {
        try {
            new EndPoint("hello world", null);
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("Listener 'hello world' is invalid"));
        }
        try {
            new EndPoint("pulsar://localhost:6650", null);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("No enum constant"));
        }
        try {
            new EndPoint("PLAINTEXT://localhost:65536", null);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("port '65536' is invalid"));
        }
        try {
            new EndPoint("PLAINTEXT://localhost:-1", null);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("port '-1' is invalid"));
        }
    }

    @Test
    public void testValidListeners() throws Exception {
        final Map<String, EndPoint> endPointMap =
                EndPoint.parseListeners("PLAINTEXT://localhost:9092,SSL://:9093");
        assertEquals(endPointMap.size(), 2);

        final EndPoint plainEndPoint = endPointMap.get("PLAINTEXT");
        assertNotNull(plainEndPoint);
        assertEquals(plainEndPoint.getSecurityProtocol(), SecurityProtocol.PLAINTEXT);
        assertEquals(plainEndPoint.getHostname(), "localhost");
        assertEquals(plainEndPoint.getPort(), 9092);

        final EndPoint sslEndPoint = endPointMap.get("SSL");
        final String localhost = InetAddress.getLocalHost().getCanonicalHostName();
        assertNotNull(sslEndPoint);
        assertEquals(sslEndPoint.getSecurityProtocol(), SecurityProtocol.SSL);
        assertEquals(sslEndPoint.getHostname(), localhost);
        assertEquals(sslEndPoint.getPort(), 9093);
    }

    @Test
    public void testValidMultiListeners() throws Exception {
        final String kafkaProtocolMap = "internal:PLAINTEXT,internal_ssl:SSL,external:PLAINTEXT,external_ssl:SSL";
        final String kafkaListeners = "internal://localhost:9092,internal_ssl://localhost:9093,"
                + "external://externalhost:9094,external_ssl://externalhost:9095";
        final Map<String, EndPoint> endPointMap = EndPoint.parseListeners(kafkaListeners, kafkaProtocolMap);
        assertEquals(endPointMap.size(), 4);

        final EndPoint internal = endPointMap.get("internal");
        assertNotNull(internal);
        assertEquals(internal.getSecurityProtocol(), SecurityProtocol.PLAINTEXT);
        assertEquals(internal.getHostname(), "localhost");
        assertEquals(internal.getPort(), 9092);

        final EndPoint internalSSL = endPointMap.get("internal_ssl");
        assertNotNull(internalSSL);
        assertEquals(internalSSL.getSecurityProtocol(), SecurityProtocol.SSL);
        assertEquals(internalSSL.getHostname(), "localhost");
        assertEquals(internalSSL.getPort(), 9093);

        final EndPoint external = endPointMap.get("external");
        assertNotNull(external);
        assertEquals(external.getSecurityProtocol(), SecurityProtocol.PLAINTEXT);
        assertEquals(external.getHostname(), "externalhost");
        assertEquals(external.getPort(), 9094);

        final EndPoint externalSSL = endPointMap.get("external_ssl");
        assertNotNull(externalSSL);
        assertEquals(externalSSL.getSecurityProtocol(), SecurityProtocol.SSL);
        assertEquals(externalSSL.getHostname(), "externalhost");
        assertEquals(externalSSL.getPort(), 9095);
    }

    @Test
    public void testInvalidMultiListeners() throws Exception {
        try {
            EndPoint.parseListeners("internal://localhost:9092,internal_ssl://localhost:9093",
                    "internal:PLAINTEXT,internal:SSL,internal_ssl:SSL");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains(" has multiple listeners whose listenerName is internal"));
        }

        try {
            EndPoint.parseListeners("internal://localhost:9092,internal_ssl://localhost:9093",
                    "internal:PLAINTEXT,external:SSL");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("internal_ssl is not set in kafkaProtocolMap"));
        }
    }

    @Test
    public void testRepeatedListeners() throws Exception {
        try {
            EndPoint.parseListeners("PLAINTEXT://localhost:9092,SSL://:9093,SSL://localhost:9094");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains(" has multiple listeners whose listenerName is SSL"));
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
        final EndPoint endPoint = new EndPoint("PLAINTEXT://:9092", null);
        assertEquals(endPoint.getHostname(), InetAddress.getLocalHost().getCanonicalHostName());
        assertEquals(endPoint.getOriginalListener(), "PLAINTEXT://:9092");
    }
}
