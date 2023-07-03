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
package io.streamnative.pulsar.handlers.kop.e2e;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import io.streamnative.kafka.client.api.Header;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessagePayloadProcessor;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Basic end-to-end test.
 */
@Slf4j
public class BasicEndToEndTestBase extends KopProtocolHandlerTestBase {

    protected static final String GROUP_ID = "my-group";
    protected static final String SUBSCRIPTION = "pulsar-sub";

    public BasicEndToEndTestBase(final String entryFormat) {
        super(entryFormat);
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private String bootstrapServers() {
        return "localhost:" + getKafkaBrokerPort();
    }

    protected KafkaProducer<String, String> newKafkaProducer() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(props);
    }

    protected KafkaConsumer<String, String> newKafkaConsumer(final String topic) {
        return newKafkaConsumer(topic, null);
    }

    protected KafkaConsumer<String, String> newKafkaConsumer(final String topic, final String group) {
        return newKafkaConsumer(topic, group, false);
    }
    protected KafkaConsumer<String, String> newKafkaConsumer(final String topic,
                                                             final String group,
                                                             final boolean readCommitted) {
        final Properties props =  new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, (group == null) ? GROUP_ID : group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, readCommitted ? "read_committed" : "read_uncommitted");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    protected Producer<byte[]> newPulsarProducer(final String topic) throws PulsarClientException {
        return newPulsarProducer(topic, true);
    }

    protected Producer<byte[]> newPulsarProducer(final String topic,
                                                 final boolean enableBatching) throws PulsarClientException {
        return pulsarClient.newProducer().topic(topic)
                .enableBatching(enableBatching)
                .batchingMaxMessages(5)
                .create();
    }

    protected Consumer<byte[]> newPulsarConsumer(final String topic) throws PulsarClientException {
        return newPulsarConsumer(topic, SUBSCRIPTION);
    }

    protected Consumer<byte[]> newPulsarConsumer(final String topic,
                                                 final String subscription) throws PulsarClientException {
        return newPulsarConsumer(topic, subscription, null);
    }

    protected Consumer<byte[]> newPulsarConsumer(final String topic,
                                                 final String subscription,
                                                 final MessagePayloadProcessor payloadProcessor)
            throws PulsarClientException {
        return pulsarClient.newConsumer().topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .messagePayloadProcessor(payloadProcessor)
                .subscribe();
    }

    private static String format(final String s) {
        return (s == null) ? "null" : ("'" + s + "'");
    }

    protected void sendSingleMessages(final KafkaProducer<String, String> producer,
                                      final String topic,
                                      final List<String> values) throws ExecutionException, InterruptedException {
        for (String value : values) {
            producer.send(new ProducerRecord<>(topic, value), (metadata, exception) -> {
                assertNull(exception);
                if (log.isDebugEnabled()) {
                    log.debug("KafkaProducer send {} to {}-partition-{}@{}",
                            format(value), metadata.topic(), metadata.partition(), metadata.offset());
                }
            }).get();
        }
    }

    protected void sendSingleMessages(final Producer<byte[]> producer, final List<String> values)
            throws PulsarClientException {
        for (String value : values) {
            final MessageId messageId = producer.newMessage().value((value == null) ? null : value.getBytes()).send();
            if (log.isDebugEnabled()) {
                final MessageIdImpl impl = (MessageIdImpl) messageId;
                log.debug("Pulsar Producer send {} to ({}, {})", format(value), impl.getLedgerId(), impl.getEntryId());
            }
        }
    }

    protected void sendBatchedMessages(final KafkaProducer<String, String> producer,
                                       final String topic,
                                       final List<String> values) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = null;
        for (String value : values) {
            future = producer.send(new ProducerRecord<>(topic, value), (metadata, exception) -> {
                assertNull(exception);
                if (log.isDebugEnabled()) {
                    log.debug("KafkaProducer send {} to {}-partition-{}@{}",
                            format(value), metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        }
        assertNotNull(future);
        future.get();
    }

    protected void sendBatchedMessages(final Producer<byte[]> producer, final List<String> values)
            throws ExecutionException, InterruptedException {
        CompletableFuture<MessageId> future = null;
        for (String value : values) {
            future = producer.newMessage().value((value == null) ? null : value.getBytes()).sendAsync()
                    .thenApply(messageId -> {
                        if (log.isDebugEnabled()) {
                            final MessageIdImpl impl = (MessageIdImpl) messageId;
                            log.debug("Pulsar Producer send {} to ({}, {})",
                                    format(value), impl.getLedgerId(), impl.getEntryId());
                        }
                        return null;
                    });
        }
        assertNotNull(future);
        future.get();
    }

    protected List<String> receiveMessages(final KafkaConsumer<String, String> consumer, int numMessages) {
        if (log.isDebugEnabled()) {
            log.debug("KafkaConsumer receiveMessages {} messages..");
        }
        List<String> values = new ArrayList<>();
        while (numMessages > 0) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(100))) {
                if (log.isDebugEnabled()) {
                    log.debug("KafkaConsumer receive: {} from partition {}", record.value(), record.partition());
                }
                values.add(record.value());
                numMessages--;
            }
        }
        return values;
    }

    protected List<ConsumerRecord<String, String>> receiveRecords(final KafkaConsumer<String, String> consumer,
                                                                  int numMessages) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        while (numMessages > 0) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(100))) {
                if (log.isDebugEnabled()) {
                    log.debug("KafkaConsumer receive: {}", record.value());
                }
                records.add(record);
                numMessages--;
            }
        }
        return records;
    }

    protected static List<String> getValuesFromRecords(final List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::value).collect(Collectors.toList());
    }

    protected static List<String> getKeysFromRecords(final List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::key).collect(Collectors.toList());
    }

    protected static List<Header> getFirstHeadersFromRecords(final List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::headers)
                .map(headers -> Header.fromHeaders(headers.toArray()))
                .map(headers -> (headers != null) ? headers.get(0) : null)
                .collect(Collectors.toList());
    }

    protected static String convertPulsarMessageToString(final Message<byte[]> message) {
        return (message.getValue() != null) ? new String(message.getValue(), StandardCharsets.UTF_8) : null;
    }

    protected List<Message<byte[]>> receivePulsarMessages(final Consumer<byte[]> consumer, int numMessages)
            throws PulsarClientException {
        List<Message<byte[]>> messages = new ArrayList<>();
        while (numMessages > 0) {
            Message<byte[]> message = consumer.receive(100, TimeUnit.MILLISECONDS);
            if (message != null) {
                messages.add(message);
                if (log.isDebugEnabled()) {
                    log.debug("Pulsar Consumer receive {} from {}",
                            convertPulsarMessageToString(message), message.getMessageId());
                }
                numMessages--;
            }
        }
        return messages;
    }

    protected List<String> receiveMessages(final Consumer<byte[]> consumer, int numMessages)
            throws PulsarClientException {
        return receivePulsarMessages(consumer, numMessages).stream()
                .map(BasicEndToEndTestBase::convertPulsarMessageToString)
                .collect(Collectors.toList());
    }

    protected int kafkaPublishMessage(KProducer kProducer, int numMessages, String messageStrPrefix) throws Exception {
        int i = 0;
        for (; i < numMessages; i++) {
            String messageStr = messageStrPrefix + i;
            ProducerRecord record = new ProducerRecord<>(
                kProducer.getTopic(),
                i,
                messageStr);

            kProducer.getProducer()
                .send(record)
                .get();
            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer {} Sent message with header: ({}, {})",
                    kProducer.getTopic(), i, messageStr);
            }
        }
        return i;
    }

    protected void kafkaConsumeCommitMessage(KConsumer kConsumer,
                                             int numMessages,
                                             String messageStrPrefix,
                                             List<TopicPartition> topicPartitions) {
        kConsumer.getConsumer().assign(topicPartitions);
        int i = 0;
        while (i < numMessages) {
            if (log.isDebugEnabled()) {
                log.debug("kConsumer {} start poll message: {}",
                    kConsumer.getTopic() + kConsumer.getConsumerGroup(), i);
            }
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(record.value(), messageStrPrefix + key.toString());

                if (log.isDebugEnabled()) {
                    log.debug("Kafka consumer get message: {}, key: {} at offset {}",
                        record.key(), record.value(), record.offset());
                }
                i++;
            }
        }

        assertEquals(i, numMessages);

        try {
            kConsumer.getConsumer().commitSync(Duration.ofSeconds(1));
        } catch (Exception e) {
            log.error("Commit offset failed: ", e);
        }

        if (log.isDebugEnabled()) {
            log.debug("kConsumer {} finished poll and commit message: {}",
                kConsumer.getTopic() + kConsumer.getConsumerGroup(), i);
        }
    }

}
