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
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
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
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Transaction test.
 */
@Slf4j
public class TransactionTest extends KopProtocolHandlerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        this.conf.setEnableTransactionCoordinator(true);
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

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 1000 * 20)
    public void readCommittedTest() throws Exception {
        basicProduceAndConsumeTest("read-committed-test", "txn-11", "read_committed");
    }

    @Test(timeOut = 1000 * 20)
    public void readUncommittedTest() throws Exception {
        basicProduceAndConsumeTest("read-uncommitted-test", "txn-12", "read_uncommitted");
    }

    public void basicProduceAndConsumeTest(String topicName,
                                           String transactionalId,
                                           String isolation) throws Exception {
        String kafkaServer = "localhost:" + getKafkaBrokerPort();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

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

            if (txnIndex % 2 != 0) {
                producer.commitTransaction();
            } else {
                producer.abortTransaction();
            }
        }

        consumeTxnMessage(topicName, totalTxnCount * messageCountPerTxn, lastMessage, isolation);
    }

    private void consumeTxnMessage(String topicName,
                                   int totalMessageCount,
                                   String lastMessage,
                                   String isolation) throws InterruptedException {
        String kafkaServer = "localhost:" + getKafkaBrokerPort();

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-test");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);

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

    @Test(timeOut = 1000 * 15)
    public void offsetCommitTest() throws Exception {
        txnOffsetTest("txn-offset-commit-test", 10, true);
    }

    @Test(timeOut = 1000 * 5)
    public void offsetAbortTest() throws Exception {
        txnOffsetTest("txn-offset-abort-test", 10, false);
    }

    public void txnOffsetTest(String topic, int messageCnt, boolean isCommit) throws Exception {
        String kafkaServer = "localhost:" + getKafkaBrokerPort();
        String groupId = "my-group-id";

        List<String> sendMsgs = prepareData(topic, kafkaServer, "first send message - ", messageCnt);

        // producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "12");
        @Cleanup
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);

        // consumer
        Properties consumeProps = new Properties();
        consumeProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        consumeProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumeProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumeProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumeProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        @Cleanup
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumeProps);
        consumer.subscribe(Collections.singleton(topic));

        producer.initTransactions();
        producer.beginTransaction();

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        AtomicInteger msgCnt = new AtomicInteger(messageCnt);

        while (msgCnt.get() > 0) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
            for (ConsumerRecord<Integer, String> record : records) {
                log.info("receive message (first) - {}", record.value());
                Assert.assertEquals(sendMsgs.get(messageCnt - msgCnt.get()), record.value());
                msgCnt.decrementAndGet();
                offsets.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
            }
        }
        producer.sendOffsetsToTransaction(offsets, groupId);

        if (isCommit) {
            producer.commitTransaction();
        } else {
            producer.abortTransaction();
        }

        resetToLastCommittedPositions(consumer);

        msgCnt = new AtomicInteger(messageCnt);
        while (msgCnt.get() > 0) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.of(1000, ChronoUnit.MILLIS));
            if (isCommit) {
                if (records.isEmpty()) {
                    msgCnt.decrementAndGet();
                } else {
                    Assert.fail("The transaction was committed, the consumer shouldn't receive any more messages.");
                }
            } else {
                for (ConsumerRecord<Integer, String> record : records) {
                    log.info("receive message (second) - {}", record.value());
                    Assert.assertEquals(sendMsgs.get(messageCnt - msgCnt.get()), record.value());
                    msgCnt.decrementAndGet();
                }
            }
        }
    }

    private List<String> prepareData(String sourceTopicName,
                                     String kafkaServer,
                                     String messageContent,
                                     int messageCount) throws ExecutionException, InterruptedException {
        // producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);

        List<String> sendMsgs = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            String msg = messageContent + i;
            sendMsgs.add(msg);
            producer.send(new ProducerRecord<>(sourceTopicName, i, msg)).get();
        }
        return sendMsgs;
    }

    private static void resetToLastCommittedPositions(KafkaConsumer<Integer, String> consumer) {
        consumer.assignment().forEach(tp -> {
            OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
            if (offsetAndMetadata != null) {
                consumer.seek(tp, offsetAndMetadata.offset());
            } else {
                consumer.seekToBeginning(Collections.singleton(tp));
            }
        });
    }

}
