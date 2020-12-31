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

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

@Slf4j
public class TransactionTest extends KopProtocolHandlerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
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

    @Test
    public void produceTxnMessageTest() throws Exception {
        String kafkaServer = "localhost:" + getKafkaBrokerPort();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "12");
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);

        String topicName = "kop-produce-txn-message";

        producer.initTransactions();

        String lastMessage = "";
        for (int txnCount = 0; txnCount < 10; txnCount++) {
            producer.beginTransaction();

            String contentBase;
            if (txnCount % 2 != 0) {
                contentBase = "commit msg txnCount %s txnIndex %s";
            } else {
                contentBase = "abort msg txnCount %s txnIndex %s";
            }

            for (int i = 0; i < 10; i++) {
                String msgContent = String.format(contentBase, txnCount, i);
                log.info("send txn message {}", msgContent);
                lastMessage = msgContent;
                producer.send(new ProducerRecord<>(topicName, i, msgContent)).get();
            }

            if (txnCount % 2 != 0) {
                producer.commitTransaction();
            } else {
                producer.abortTransaction();
            }
        }

        final String testMessage = lastMessage;
        CountDownLatch countDownLatch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                consumeTxnMessage(countDownLatch, testMessage);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        countDownLatch.await();
        producer.close();
    }

    private void consumeTxnMessage(CountDownLatch countDownLatch, String lastMessage) throws Exception {
        String kafkaServer = "localhost:" + getKafkaBrokerPort();
        String topicName = "kop-produce-txn-message";

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-test");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(topicName));

        log.info("the last message is: {}", lastMessage);
        while (true) {
            ConsumerRecords<Integer, String> consumerRecords =
                    consumer.poll(Duration.of(100, ChronoUnit.MILLIS));

            CompletableFuture<Void> completableFuture = new CompletableFuture<>();
            consumerRecords.forEach(record -> {
                log.info("receive record key: {}, value: {}", record.key(), record.value());
                if (lastMessage.equalsIgnoreCase(record.value())) {
                    log.info("receive the last message");
                    completableFuture.complete(null);
                }
            });
            if (completableFuture.isDone()) {
                countDownLatch.countDown();
                break;
            }
//            Thread.sleep(1000 * 3);
        }
    }

    @Test
    public void test() throws Exception {
        String sourceTopicName = "kop-txn-source";
        String sinkTopicName = "kop-txn-sink";
//        String pulsarTopicName = "persistent://public/default/" + sinkTopicName;
//        pulsar.getAdminClient().topics().createPartitionedTopic(pulsarTopicName, 1);

        String kafkaServer = "localhost:" + getKafkaBrokerPort();

        prepareData(sourceTopicName, kafkaServer);

        // producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "12");
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);

        // consumer
        Properties consumeProps = new Properties();
        consumeProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        consumeProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumeProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumeProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumeProps);
        consumer.subscribe(Collections.singleton(sourceTopicName));

        ConsumerRecords<Integer, String> records = consumer.poll(Duration.of(1000, ChronoUnit.SECONDS));
        Iterator<ConsumerRecord<Integer, String>> iterator = records.iterator();

        producer.initTransactions();
        producer.beginTransaction();

        long lastOffset = 0;
        while (iterator.hasNext()) {
            ConsumerRecord<Integer, String> record = iterator.next();
            lastOffset = record.offset();
            producer.send(new ProducerRecord(sinkTopicName, record.key() + 100, record.value() + " [processed]")).get();
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(sinkTopicName, 0), new OffsetAndMetadata(lastOffset));
        producer.sendOffsetsToTransaction(offsets, "my-group-id");

        producer.commitTransaction();

        producer.close();
        consumer.close();
    }

    private void prepareData(String sourceTopicName, String kafkaServer) {
        // producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);

        for (int i = 0; i < 15; i++) {
            producer.send(new ProducerRecord<>(sourceTopicName, i, "Hello Index " + i));
        }
    }

}
