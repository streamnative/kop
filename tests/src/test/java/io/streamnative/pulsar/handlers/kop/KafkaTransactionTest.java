package io.streamnative.pulsar.handlers.kop;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Future;

@Slf4j
public class KafkaTransactionTest {

    @Test
    public void test() throws Exception {
        String sourceTopicName = "kop-txn-source-8";
        String sinkTopicName = "kop-txn-sink";

        String kafkaServer = "localhost:9092";

        // producer
        new Thread(() -> {
            try {
                sendTransactionMessages("txn1", kafkaServer, sourceTopicName, false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(1000);

        new Thread(() -> {
            try {
                sendTransactionMessages("txn2", kafkaServer, sourceTopicName, true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(1000);

        new Thread(() -> {
            try {
                sendTransactionMessages("txn3", kafkaServer, sourceTopicName, false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(1000);

        new Thread(() -> {
            try {
                sendNormalMessages(kafkaServer, sourceTopicName);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(1000 * 30);

//        consumeMessages(kafkaServer, sourceTopicName);
    }

    @Test
    public void consumeTest() {
        String sourceTopicName = "kop-txn-source-8";
        String kafkaServer = "localhost:9092";
        consumeMessages(kafkaServer, sourceTopicName);
    }

    private void sendTransactionMessages(String transactionalId, String kafkaServer, String topic, boolean delayCommit) throws Exception {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);

        producer.initTransactions();
        producer.beginTransaction();

        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, 1, "hello"));
        RecordMetadata recordMetadata = future.get();
        System.out.println("sent message delayCommit: " + delayCommit + ", offset: " + recordMetadata.offset());
        if (delayCommit) {
            Thread.sleep(5000);
        }
        producer.commitTransaction();
        System.out.println("send message delayCommit: " + delayCommit + " commit finish.");
        producer.close();
    }

    private void sendNormalMessages(String kafkaServer, String topic) throws Exception {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);

        for (int i = 0; i < 3; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, 1, "hello"));
            System.out.println("sent normal message offset: " + future.get().offset());
        }
        producer.close();
    }

    private void consumeMessages(String kafkaServer, String sourceTopicName) {
        // consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "10");
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(consumerProps);

        consumer.subscribe(Collections.singleton(sourceTopicName));

        while (true) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            Iterator<ConsumerRecord<Integer, String>> iterator = records.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<Integer, String> record = iterator.next();
                System.out.println("receive message offset: " + record.offset());
            }
        }
    }

}
