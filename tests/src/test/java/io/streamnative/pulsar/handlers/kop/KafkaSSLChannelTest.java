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
import static org.testng.AssertJUnit.assertFalse;

import java.io.Closeable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.HostnameVerifier;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Validate Kafka SSL channel config.
 */
@Slf4j
public class KafkaSSLChannelTest extends KopProtocolHandlerTestBase {

    private String kopSslKeystoreLocation;
    private String kopSslKeystorePassword;
    private String kopSslTruststoreLocation;
    private String kopSslTruststorePassword;
    private String kopClientTruststoreLocation;
    private String kopClientTruststorePassword;
    private final boolean withCertHost;

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

    public KafkaSSLChannelTest(final String entryFormat, boolean withCertHost) {
        super(entryFormat);
        this.withCertHost = withCertHost;
        setSslConfigurations(withCertHost);
    }

    /**
     * Set ssl configurations.
     * @param withCertHost the keystore with certHost or not.
     */
    private void setSslConfigurations(boolean withCertHost) {
        String path = "./src/test/resources/ssl/certificate" + (withCertHost ? "2" : "") + "/";
        if (!withCertHost) {
            this.kopSslKeystoreLocation = path + "broker.keystore.jks";
            this.kopSslKeystorePassword = "broker";
            this.kopSslTruststoreLocation = path + "broker.truststore.jks";
            this.kopSslTruststorePassword = "broker";
        } else {
            this.kopSslKeystoreLocation = path + "server.keystore.jks";
            this.kopSslKeystorePassword = "server";
            this.kopSslTruststoreLocation = path + "server.truststore.jks";
            this.kopSslTruststorePassword = "server";
        }
        kopClientTruststoreLocation = path + "client.truststore.jks";
        kopClientTruststorePassword = "client";
    }

    @Factory
    public static Object[] instances() {
        return new Object[] {
                new KafkaSSLChannelTest("pulsar", false),
                new KafkaSSLChannelTest("pulsar", true),
                new KafkaSSLChannelTest("kafka", false),
                new KafkaSSLChannelTest("kafka", true)
        };
    }

    protected void sslSetUpForBroker() throws Exception {
        conf.setKafkaTransactionCoordinatorEnabled(true);
        conf.setKopTlsEnabledWithBroker(true);
        conf.setKopSslKeystoreType("JKS");
        conf.setKopSslKeystoreLocation(kopSslKeystoreLocation);
        conf.setKopSslKeystorePassword(kopSslKeystorePassword);
        conf.setKopSslTruststoreLocation(kopSslTruststoreLocation);
        conf.setKopSslTruststorePassword(kopSslTruststorePassword);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        sslSetUpForBroker();
        super.internalSetup();
        log.info("success internal setup");
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
        SslProducer kProducer = new SslProducer(topicName, getKafkaBrokerPortTls(),
                kopClientTruststoreLocation, kopClientTruststorePassword);

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

        public SslProducer(String topic, int port, String truststoreLocation, String truststorePassword) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost" + ":" + port);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoKafkaOnPulsarProducerSSL");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // SSL client config
            props.put("security.protocol", "SSL");
            props.put("ssl.truststore.location", truststoreLocation);
            props.put("ssl.truststore.password", truststorePassword);

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


    @Test
    public void basicProduceAndConsumeWithTxTest() throws Exception {
        final String isolation = "read_committed";
        String topicName = "test-tls-tx-" + this.withCertHost + "_" + entryFormat;
        String transactionalId = "test-id-" + this.withCertHost + "_" + entryFormat;
        String kafkaServer = "localhost:" + getKafkaBrokerPortTls();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        // SSL client config
        producerProps.put("security.protocol", "SSL");
        producerProps.put("ssl.truststore.location", kopClientTruststoreLocation);
        producerProps.put("ssl.truststore.password", kopClientTruststorePassword);

        // default is https, here need to set empty.
        producerProps.put("ssl.endpoint.identification.algorithm", "");

        @Cleanup
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);

        producer.initTransactions();

        int totalTxnCount = 10;
        int messageCountPerTxn = 10;

        String lastMessage = "";
        for (int txnIndex = 0; txnIndex < totalTxnCount; txnIndex++) {
            producer.beginTransaction();

            String contentBase;
            if (txnIndex % 2 != 0) {
                contentBase = "commit msg txnIndex %s messageIndex %s";
            } else {
                contentBase = "abort msg txnIndex %s messageIndex %s";
            }

            for (int messageIndex = 0; messageIndex < messageCountPerTxn; messageIndex++) {
                String msgContent = String.format(contentBase, txnIndex, messageIndex);
                log.info("send txn message {}", msgContent);
                lastMessage = msgContent;
                producer.send(new ProducerRecord<>(topicName, messageIndex, msgContent)).get();
            }
            producer.flush();

            if (txnIndex % 2 != 0) {
                producer.commitTransaction();
            } else {
                producer.abortTransaction();
            }

            Thread.sleep(100);
        }

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-test");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);

        // SSL client config
        consumerProps.put("security.protocol", "SSL");
        consumerProps.put("ssl.truststore.location", kopClientTruststoreLocation);
        consumerProps.put("ssl.truststore.password", kopClientTruststorePassword);

        // default is https, here need to set empty.
        consumerProps.put("ssl.endpoint.identification.algorithm", "");

        final int totalMessageCount = totalTxnCount * messageCountPerTxn;

        @Cleanup
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(topicName));

        log.info("the last message is: {}", lastMessage);
        AtomicInteger receiveCount = new AtomicInteger(0);
        while (true) {
            ConsumerRecords<Integer, String> consumerRecords =
                    consumer.poll(Duration.of(100, ChronoUnit.MILLIS));

            boolean readFinish = false;
            for (ConsumerRecord<Integer, String> record : consumerRecords) {
                if (isolation.equals("read_committed")) {
                    assertFalse(record.value().contains("abort msg txnIndex"));
                }
                log.info("Fetch for receive record offset: {}, key: {}, value: {}",
                        record.offset(), record.key(), record.value());
                receiveCount.incrementAndGet();
                if (lastMessage.equalsIgnoreCase(record.value())) {
                    log.info("receive the last message");
                    readFinish = true;
                }
            }

            if (readFinish) {
                log.info("Fetch for read finish.");
                break;
            }
        }
        log.info("Fetch for receive message finish. isolation: {}, receive count: {}", isolation, receiveCount.get());

        if (isolation.equals("read_committed")) {
            Assert.assertEquals(receiveCount.get(), totalMessageCount / 2);
        } else {
            Assert.assertEquals(receiveCount.get(), totalMessageCount);
        }
        log.info("Fetch for finish consume messages. isolation: {}", isolation);
    }
}
