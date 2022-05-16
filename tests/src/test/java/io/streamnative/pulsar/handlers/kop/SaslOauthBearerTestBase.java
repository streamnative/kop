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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TimeoutException;

/**
 * Base class for SASL-OAUTHBEARER tests.
 */
@Slf4j
public abstract class SaslOauthBearerTestBase extends KopProtocolHandlerTestBase {

    public SaslOauthBearerTestBase() {
        super("kafka");
    }

    protected abstract void configureOauth2(Properties props);

    protected void testSimpleProduceConsume() throws Exception {
        final String topic = "testSimpleProduceConsume";
        final String message = "hello";

        final Properties producerProps = newKafkaProducerProperties();
        configureOauth2(producerProps);
        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, message)).get();
        log.info("Send to {}-partition-{}@{}", metadata.topic(), metadata.partition(), metadata.offset());

        final Properties consumerProps = newKafkaConsumerProperties();
        configureOauth2(consumerProps);
        @Cleanup
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(topic));

        final List<String> receivedMessages = new ArrayList<>();
        while (receivedMessages.isEmpty()) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                receivedMessages.add(record.value());
                log.info("Receive {} from {}-partition-{}@{}",
                        record.value(), record.topic(), record.partition(), record.offset());
            }
        }
        assertEquals(receivedMessages.size(), 1);
        assertEquals(receivedMessages.get(0), message);
    }


    protected void testProduceWithoutAuth() throws Exception {
        final String topic = "testProduceWithoutAuth";

        final Properties producerProps = newKafkaProducerProperties();
        producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);

        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        try {
            producer.send(new ProducerRecord<>(topic, "", "hello")).get();
            fail("should have failed");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
            assertTrue(e.getMessage().contains("Topic " + topic
                    + " not present in metadata after 3000 ms."));
        }
    }
}
