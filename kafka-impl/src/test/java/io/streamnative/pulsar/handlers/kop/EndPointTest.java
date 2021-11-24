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
import java.net.UnknownHostException;
import java.util.Collections;
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
            assertTrue(e.getMessage().contains("listener 'hello world' is invalid"));
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
            assertTrue(e.getMessage().contains("port 65536 is invalid"));
        }
        try {
            new EndPoint("PLAINTEXT://localhost:-1", null);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("port -1 is invalid"));
        }
    }

    @Test
    public void testValidListeners() throws Exception {
        final KafkaServiceConfiguration kafkaConfig = new KafkaServiceConfiguration();
        kafkaConfig.setKafkaListeners("PLAINTEXT://localhost:9092,SSL://:9093");
        final Map<String, EndPoint> endPointMap =
                EndPoint.parseListeners(kafkaConfig);
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
        final KafkaServiceConfiguration kafkaConfig = new KafkaServiceConfiguration();
        final String kafkaProtocolMap = "internal:PLAINTEXT,internal_ssl:SSL,external:PLAINTEXT,external_ssl:SSL";
        final String kafkaListeners = "internal://localhost:9092,internal_ssl://localhost:9093,"
                + "external://externalhost:9094,external_ssl://externalhost:9095";
        kafkaConfig.setKafkaListeners(kafkaListeners);
        kafkaConfig.setKafkaProtocolMap(kafkaProtocolMap);
        final Map<String, EndPoint> endPointMap = EndPoint.parseListeners(kafkaConfig);
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
        final KafkaServiceConfiguration kafkaConfig = new KafkaServiceConfiguration();
        try {
            kafkaConfig.setKafkaListeners("internal://localhost:9092,internal_ssl://localhost:9093");
            kafkaConfig.setKafkaProtocolMap("internal:PLAINTEXT,internal:SSL,internal_ssl:SSL");
            EndPoint.parseListeners(kafkaConfig);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains(" has multiple listeners whose listenerName is internal"));
        }

        try {
            kafkaConfig.setKafkaListeners("internal://localhost:9092,internal_ssl://localhost:9093");
            kafkaConfig.setKafkaProtocolMap("internal:PLAINTEXT,external:SSL");
            EndPoint.parseListeners(kafkaConfig);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("internal_ssl is not set in kafkaProtocolMap"));
        }
    }

    @Test
    public void testRepeatedListeners() throws Exception {
        final KafkaServiceConfiguration kafkaConfig = new KafkaServiceConfiguration();
        try {
            kafkaConfig.setKafkaListeners("PLAINTEXT://localhost:9092,SSL://:9093,SSL://localhost:9094");
            EndPoint.parseListeners(kafkaConfig);
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

    @Test
    public void testValidInterListeners() throws Exception {
        final KafkaServiceConfiguration kafkaConfig1 = new KafkaServiceConfiguration();
        kafkaConfig1.setKafkaListeners("kafka_internal://localhost:9092,kafka_external://:9093");
        kafkaConfig1.setKafkaProtocolMap("kafka_internal:SASL_PLAINTEXT,kafka_external:SSL");
        // check InterBrokerListenerName
        kafkaConfig1.setInterBrokerListenerName("kafka_internal");
        kafkaConfig1.setSaslAllowedMechanisms(Collections.singleton("PLAIN"));
        Map<String, EndPoint> endPointMap =
                EndPoint.parseListeners(kafkaConfig1);
        assertEquals(endPointMap.size(), 2);

        EndPoint plainEndPoint = endPointMap.get("kafka_internal");
        assertNotNull(plainEndPoint);
        assertEquals(plainEndPoint.getSecurityProtocol(), SecurityProtocol.SASL_PLAINTEXT);
        assertEquals(plainEndPoint.getHostname(), "localhost");
        assertEquals(plainEndPoint.getPort(), 9092);

        EndPoint sslEndPoint = endPointMap.get("kafka_external");
        String localhost = InetAddress.getLocalHost().getCanonicalHostName();
        assertNotNull(sslEndPoint);
        assertEquals(sslEndPoint.getSecurityProtocol(), SecurityProtocol.SSL);
        assertEquals(sslEndPoint.getHostname(), localhost);
        assertEquals(sslEndPoint.getPort(), 9093);

        assertEquals(kafkaConfig1.getInterBrokerListenerName(), "kafka_internal");
        assertEquals(kafkaConfig1.getInterBrokerSecurityProtocol(), SecurityProtocol.SASL_PLAINTEXT.name);

        final KafkaServiceConfiguration kafkaConfig2 = new KafkaServiceConfiguration();
        kafkaConfig2.setKafkaListeners("PLAINTEXT://localhost:9092,SSL://:9093");
        // check InterBrokerSecurityProtocol
        kafkaConfig2.setInterBrokerSecurityProtocol("PLAINTEXT");
        endPointMap = EndPoint.parseListeners(kafkaConfig2);
        assertEquals(endPointMap.size(), 2);

        plainEndPoint = endPointMap.get("PLAINTEXT");
        assertNotNull(plainEndPoint);
        assertEquals(plainEndPoint.getSecurityProtocol(), SecurityProtocol.PLAINTEXT);
        assertEquals(plainEndPoint.getHostname(), "localhost");
        assertEquals(plainEndPoint.getPort(), 9092);

        sslEndPoint = endPointMap.get("SSL");
        localhost = InetAddress.getLocalHost().getCanonicalHostName();
        assertNotNull(sslEndPoint);
        assertEquals(sslEndPoint.getSecurityProtocol(), SecurityProtocol.SSL);
        assertEquals(sslEndPoint.getHostname(), localhost);
        assertEquals(sslEndPoint.getPort(), 9093);

        assertEquals(kafkaConfig2.getInterBrokerListenerName(), "PLAINTEXT");
        assertEquals(kafkaConfig2.getInterBrokerSecurityProtocol(), SecurityProtocol.PLAINTEXT.name);
    }

    @Test
    public void testInvalidInterListeners() throws Exception {
        final KafkaServiceConfiguration kafkaConfig1 = new KafkaServiceConfiguration();
        try {
            kafkaConfig1.setKafkaListeners("PLAINTEXT://localhost:9092,SSL://:9093");
            kafkaConfig1.setInterBrokerListenerName("kafka_internal");
            EndPoint.parseListeners(kafkaConfig1);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("kafkaListeners "));
            assertTrue(e.getMessage().contains(" has not contained interBrokerListenerName"));
        }

        final KafkaServiceConfiguration kafkaConfig2 = new KafkaServiceConfiguration();
        try {
            kafkaConfig2.setKafkaListeners("kafka_external://localhost:9092,kafka_ssl://:9093");
            kafkaConfig2.setKafkaProtocolMap("kafka_external:PLAINTEXT,kafka_ssl:SSL");
            kafkaConfig2.setInterBrokerListenerName("kafka_internal");
            EndPoint.parseListeners(kafkaConfig2);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("kafkaProtocolMap "));
            assertTrue(e.getMessage().contains(" has not contained interBrokerListenerName"));
        }

        final KafkaServiceConfiguration kafkaConfig3 = new KafkaServiceConfiguration();
        try {
            kafkaConfig3.setKafkaListeners("SASL_PLAINTEXT://localhost:9092,SSL://:9093");
            EndPoint.parseListeners(kafkaConfig3);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("kafkaListeners "));
            assertTrue(e.getMessage().contains(" has not contained interBrokerSecurityProtocol"));
        }

        final KafkaServiceConfiguration kafkaConfig4 = new KafkaServiceConfiguration();
        try {
            kafkaConfig4.setKafkaListeners("kafka_external://localhost:9092,kafka_ssl://:9093");
            kafkaConfig4.setKafkaProtocolMap("kafka_external:PLAINTEXT,kafka_ssl:SSL");
            kafkaConfig4.setInterBrokerSecurityProtocol("SASL_PLAINTEXT");
            EndPoint.parseListeners(kafkaConfig4);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("kafkaProtocolMap "));
            assertTrue(e.getMessage().contains(" has not contained interBrokerSecurityProtocol"));
        }
    }

    @Test
    public void testValidMechanism() throws UnknownHostException {
        final KafkaServiceConfiguration kafkaConfig = new KafkaServiceConfiguration();
        kafkaConfig.setKafkaListeners("SASL_PLAINTEXT://localhost:9092,SSL://:9093");
        kafkaConfig.setInterBrokerListenerName("SASL_PLAINTEXT");
        // check inter broker sasl mechanism
        kafkaConfig.setSaslAllowedMechanisms(Collections.singleton("PLAIN"));
        kafkaConfig.setSaslMechanismInterBrokerProtocol("PLAIN");
        final Map<String, EndPoint> endPointMap =
                EndPoint.parseListeners(kafkaConfig);
        assertEquals(endPointMap.size(), 2);

        final EndPoint plainEndPoint = endPointMap.get("SASL_PLAINTEXT");
        assertNotNull(plainEndPoint);
        assertEquals(plainEndPoint.getSecurityProtocol(), SecurityProtocol.SASL_PLAINTEXT);
        assertEquals(plainEndPoint.getHostname(), "localhost");
        assertEquals(plainEndPoint.getPort(), 9092);

        final EndPoint sslEndPoint = endPointMap.get("SSL");
        final String localhost = InetAddress.getLocalHost().getCanonicalHostName();
        assertNotNull(sslEndPoint);
        assertEquals(sslEndPoint.getSecurityProtocol(), SecurityProtocol.SSL);
        assertEquals(sslEndPoint.getHostname(), localhost);
        assertEquals(sslEndPoint.getPort(), 9093);

        assertEquals(kafkaConfig.getInterBrokerListenerName(), "SASL_PLAINTEXT");
        assertEquals(kafkaConfig.getInterBrokerSecurityProtocol(), SecurityProtocol.SASL_PLAINTEXT.name);
    }

    @Test
    public void testInvalidMechanism() {
        final KafkaServiceConfiguration kafkaConfig = new KafkaServiceConfiguration();
        try {
            kafkaConfig.setKafkaListeners("SASL_PLAINTEXT://localhost:9092,SSL://:9093");
            kafkaConfig.setInterBrokerListenerName("SASL_PLAINTEXT");
            // check inter broker sasl mechanism
            kafkaConfig.setSaslAllowedMechanisms(Collections.singleton("PLAIN"));
            kafkaConfig.setSaslMechanismInterBrokerProtocol("GSSAPI");
            EndPoint.parseListeners(kafkaConfig);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("saslAllowedMechanisms "));
            assertTrue(e.getMessage().contains(" has not contained saslMechanismInterBrokerProtocol"));
        }
    }
}
