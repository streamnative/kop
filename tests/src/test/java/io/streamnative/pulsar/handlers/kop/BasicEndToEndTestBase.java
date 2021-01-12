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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Basic end-to-end test.
 */
@Slf4j
public class BasicEndToEndTestBase extends KopProtocolHandlerTestBase {

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
        final Properties props =  new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    protected Producer<byte[]> newPulsarProducer(final String topic) throws PulsarClientException {
        return pulsarClient.newProducer().topic(topic).create();
    }

    protected Consumer<byte[]> newPulsarConsumer(final String topic) throws PulsarClientException {
        return pulsarClient.newConsumer().topic(topic)
                .subscriptionName("pulsar-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
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
                Assert.assertNull(exception);
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
                Assert.assertNull(exception);
                if (log.isDebugEnabled()) {
                    log.debug("KafkaProducer send {} to {}-partition-{}@{}",
                            format(value), metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        }
        Assert.assertNotNull(future);
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
        Assert.assertNotNull(future);
        future.get();
    }

    protected List<String> receiveMessages(final KafkaConsumer<String, String> consumer, int numMessages) {
        List<String> values = new ArrayList<>();
        while (numMessages > 0) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(100))) {
                if (log.isDebugEnabled()) {
                    log.debug("KafkaConsumer receive: {}", record.value());
                }
                values.add(record.value());
                numMessages--;
            }
        }
        return values;
    }

    protected List<String> receiveMessages(final Consumer<byte[]> consumer, int numMessages)
            throws PulsarClientException {
        List<String> values = new ArrayList<>();
        while (numMessages > 0) {
            Message<byte[]> message = consumer.receive(100, TimeUnit.MILLISECONDS);
            if (message != null) {
                final byte[] value = message.getValue();
                values.add((value == null) ? null : new String(value));
                if (log.isDebugEnabled()) {
                    log.debug("Pulsar Consumer receive: {}", values.get(values.size() - 1));
                }
            }
            numMessages--;
        }
        return values;
    }
}
