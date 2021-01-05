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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Sets;
import java.io.Closeable;
import java.util.Properties;
import javax.net.ssl.HostnameVerifier;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Validate Kafka SSL channel config.
 * Similar to KafkaSSLChannelTest, except the setKopSslClientAuth is set as required.
 */
@Slf4j
public class KafkaSSLChannelWithClientAuthTest extends KopProtocolHandlerTestBase {
    protected final String kopSslKeystoreLocation = "./src/test/resources/ssl/certificate/broker.keystore.jks";
    protected final String kopSslKeystorePassword = "broker";
    protected final String kopSslTruststoreLocation = "./src/test/resources/ssl/certificate/client.truststore.jks";
    protected final String kopSslTruststorePassword = "client";

    static {
        final HostnameVerifier defaultHostnameVerifier = javax.net.ssl.HttpsURLConnection.getDefaultHostnameVerifier();

        final HostnameVerifier localhostAcceptedHostnameVerifier = new HostnameVerifier() {

            public boolean verify(String hostname, javax.net.ssl.SSLSession sslSession) {
                if (hostname.equals("localhost")) {
                    return true;
                }
                return defaultHostnameVerifier.verify(hostname, sslSession);
            }
        };
        javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(localhostAcceptedHostnameVerifier);
    }

    public KafkaSSLChannelWithClientAuthTest(final String entryFormat) {
        super(entryFormat);
    }

    @Factory
    public static Object[] instances() {
        return new Object[] {
                new KafkaSSLChannelWithClientAuthTest("pulsar"),
                new KafkaSSLChannelWithClientAuthTest("kafka")
        };
    }

    protected void sslSetUpForBroker() throws Exception {
        ((KafkaServiceConfiguration) conf).setKopSslClientAuth("required");
        ((KafkaServiceConfiguration) conf).setKopSslKeystoreType("JKS");
        ((KafkaServiceConfiguration) conf).setKopSslKeystoreLocation(kopSslKeystoreLocation);
        ((KafkaServiceConfiguration) conf).setKopSslKeystorePassword(kopSslKeystorePassword);
        ((KafkaServiceConfiguration) conf).setKopSslTruststoreLocation(kopSslTruststoreLocation);
        ((KafkaServiceConfiguration) conf).setKopSslTruststorePassword(kopSslTruststorePassword);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        sslSetUpForBroker();
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName,
                new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        } else {
            admin.clusters().updateCluster(configClusterName,
                new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        }

        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        } else {
            admin.tenants().updateTenant("public",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        }
        if (!admin.namespaces().getNamespaces("public").contains("public/default")) {
            admin.namespaces().createNamespace("public/default");
            admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/default",
                new RetentionPolicies(60, 1000));
        }
        if (!admin.namespaces().getNamespaces("public").contains("public/__kafka")) {
            admin.namespaces().createNamespace("public/__kafka");
            admin.namespaces().setNamespaceReplicationClusters("public/__kafka", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/__kafka",
                new RetentionPolicies(-1, -1));
        }
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    // verify producer with SSL configured could produce successfully.
    @Test
    public void testKafkaProduceSSL() throws Exception {
        int partitionNumber = 1;
        boolean isBatch = false;
        String topicName = "kopKafkaProduceKafkaConsumeSSL" + partitionNumber;
        String key1 = "header_key1_";
        String key2 = "header_key2_";
        String value1 = "header_value1_";
        String value2 = "header_value2_";

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, partitionNumber);

        // 1. produce message with Kafka producer.
        int totalMsgs = 10;
        String messageStrPrefix = "Message_Kop_KafkaProduceKafkaConsume_" + partitionNumber + "_";

        @Cleanup
        SslProducer kProducer = new SslProducer(topicName, getKafkaBrokerPortTls());

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            ProducerRecord record = new ProducerRecord<>(
                topicName,
                i,
                messageStr);
            record.headers()
                .add(key1 + i, (value1 + i).getBytes(UTF_8))
                .add(key2 + i, (value2 + i).getBytes(UTF_8));

            if (isBatch) {
                kProducer.getProducer()
                    .send(record);
            } else {
                kProducer.getProducer()
                    .send(record)
                    .get();
            }
            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message with header: ({}, {})", i, messageStr);
            }
        }
    }

    /**
     * A producer with ssl connect wrapper.
     */
    @Getter
    public static class SslProducer implements Closeable {
        private final KafkaProducer<Integer, String> producer;
        private final String topic;

        public SslProducer(String topic, int port) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost" + ":" + port);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoKafkaOnPulsarProducerSSL");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // SSL client config
            props.put("security.protocol", "SSL");
            props.put("ssl.truststore.location", "./src/test/resources/ssl/certificate/broker.truststore.jks");
            props.put("ssl.truststore.password", "broker");
            props.put("ssl.keystore.location", "./src/test/resources/ssl/certificate/client.keystore.jks");
            props.put("ssl.keystore.password", "client");

            // default is https, here need to set empty.
            props.put("ssl.endpoint.identification.algorithm", "");

            producer = new KafkaProducer<>(props);
            this.topic = topic;
        }

        @Override
        public void close() {
            this.producer.close();
        }
    }
}
