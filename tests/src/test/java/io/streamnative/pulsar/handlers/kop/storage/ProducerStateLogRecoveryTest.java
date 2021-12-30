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
package io.streamnative.pulsar.handlers.kop.storage;

import static org.testng.AssertJUnit.assertFalse;

import io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class ProducerStateLogRecoveryTest extends KopProtocolHandlerTestBase {

    private ReplicaManager replicaManager;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        this.conf.setKafkaTransactionCoordinatorEnabled(true);
        this.conf.setEntryFormat("kafka");
        super.internalSetup();
        log.info("success internal setup");
        final KafkaProtocolHandler handler = (KafkaProtocolHandler) pulsar.getProtocolHandlers().protocol("kafka");
        replicaManager = handler.getReplicaManager(conf.getKafkaTenant());
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "produceConfigProvider")
    protected static Object[][] produceConfigProvider() {
        // isBatch
        return new Object[][]{
                {true},
                {false}
        };
    }

    @Test(timeOut = 30 * 1000, dataProvider = "produceConfigProvider")
    public void readCommittedFromRecoveryTest(boolean isBatch) throws Exception {
        basicProduceAndConsumeTest("read-committed-test", "txn-11", "read_committed", isBatch);
    }

    @Test(timeOut = 30 * 1000, dataProvider = "produceConfigProvider")
    public void readUncommittedFromRecoveryTest(boolean isBatch) throws Exception {
        basicProduceAndConsumeTest("read-uncommitted-test", "txn-12", "read_uncommitted", isBatch);
    }

    public void basicProduceAndConsumeTest(String topicName,
                                           String transactionalId,
                                           String isolation,
                                           boolean isBatch) throws Exception {
        String kafkaServer = "localhost:" + getKafkaBrokerPort();
        String fullTopicName = "persistent://public/default/" + topicName + "-partition-0";
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        int totalTxnCount = 10;
        int messageCountPerTxn = 10;

        String lastMessage = "";
        for (int txnIndex = 0; txnIndex < totalTxnCount; txnIndex++) {
            producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId + "-" + txnIndex);
            @Cleanup
            KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);

            producer.initTransactions();
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
                if (isBatch) {
                    producer.send(new ProducerRecord<>(topicName, messageIndex, msgContent));
                } else {
                    producer.send(new ProducerRecord<>(topicName, messageIndex, msgContent)).get();
                }
            }
            producer.flush();

            if (txnIndex % 2 != 0) {
                producer.commitTransaction();
            } else {
                producer.abortTransaction();
            }
        }
        TopicName topic = TopicName.get(topicName);
        PartitionLog oldPartitionLog = replicaManager.getPartitionLog(new TopicPartition(topicName, 0),
                topic.getNamespace());
        // Remove it, will it will rebuild
        replicaManager.getLogManager().removeLog(fullTopicName);
        Awaitility.await().until(() -> {
            PartitionLog newPartitionLog = replicaManager.getPartitionLog(new TopicPartition(topicName, 0),
                    topic.getNamespace());
            return oldPartitionLog.hashCode() != newPartitionLog.hashCode();
        });
        consumeTxnMessage(topicName, totalTxnCount * messageCountPerTxn, lastMessage, isolation);
    }

    private void consumeTxnMessage(String topicName,
                                   int totalMessageCount,
                                   String lastMessage,
                                   String isolation) {
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
                if (isolation.equals("read_committed")) {
                    assertFalse(record.value().contains("abort msg txnIndex"));
                }
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
