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

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.api.proto.MarkersMessageIdData;
import org.apache.pulsar.common.protocol.Markers;
import org.testng.annotations.Test;

/**
 * Basic end-to-end test with `entryFormat=pulsar`.
 */
public class BasicEndToEndPulsarTest extends BasicEndToEndTestBase {

    public BasicEndToEndPulsarTest() {
        super("pulsar");
    }

    @Test(timeOut = 20000)
    public void testNullValueMessages() throws Exception {
        final String topic = "test-produce-null-value";

        @Cleanup
        final KafkaProducer<String, String> kafkaProducer = newKafkaProducer();
        sendSingleMessages(kafkaProducer, topic, Arrays.asList(null, ""));
        sendBatchedMessages(kafkaProducer, topic, Arrays.asList("test", null, ""));

        @Cleanup
        final Producer<byte[]> pulsarProducer = newPulsarProducer(topic);
        sendSingleMessages(pulsarProducer, Arrays.asList(null, ""));
        sendBatchedMessages(kafkaProducer, topic, Arrays.asList("test", null, ""));

        final List<String> expectValues = Arrays.asList(null, "", "test", null, "", null, "", "test", null, "");

        @Cleanup
        final Consumer<byte[]> pulsarConsumer = newPulsarConsumer(topic);
        List<String> pulsarReceives = receiveMessages(pulsarConsumer, expectValues.size());
        assertEquals(pulsarReceives, expectValues);

        @Cleanup
        final KafkaConsumer<String, String> kafkaConsumer = newKafkaConsumer(topic);
        List<String> kafkaReceives = receiveMessages(kafkaConsumer, expectValues.size());
        assertEquals(kafkaReceives, expectValues);
    }

    @Test(timeOut = 20000)
    public void testMixedConsumersWithSameSubscription() throws Exception {
        final String topic = "testMixedConsumersWithSameSubscription";
        final List<String> messages = IntStream.range(0, 10).mapToObj(Integer::toString).collect(Collectors.toList());
        final String subscription = "same-sub";

        @Cleanup
        final KafkaProducer<String, String> kafkaProducer = newKafkaProducer();
        @Cleanup
        final Producer<byte[]> pulsarProducer = newPulsarProducer(topic);

        sendSingleMessages(kafkaProducer, topic, messages.subList(0, messages.size() / 2));
        sendSingleMessages(pulsarProducer, messages.subList(messages.size() / 2, messages.size()));

        KafkaConsumer<String, String> kafkaConsumer = newKafkaConsumer(topic, subscription);
        final List<String> kafkaReceives = receiveMessages(kafkaConsumer, messages.size());
        assertEquals(kafkaReceives, messages);
        kafkaConsumer.commitSync(Duration.ofSeconds(1));
        kafkaConsumer.close();

        // 1. Even if Pulsar consumer subscribes the same topic with the same subscription, the offset that Kafka
        // consumer committed doesn't affect.
        @Cleanup
        final Consumer<byte[]> pulsarConsumer = newPulsarConsumer(topic, subscription);
        final List<String> pulsarReceives = receiveMessages(pulsarConsumer, messages.size());
        assertEquals(pulsarReceives, messages);

        // 2. However, when a Kafka consumer subscribes the same topic with the same group, it will begin to subscribe
        // from the offset that has been committed before
        kafkaConsumer = newKafkaConsumer(topic, subscription);
        assertEquals(kafkaConsumer.poll(Duration.ofSeconds(1)).count(), 0);
        kafkaConsumer.close();
    }

    @Test(timeOut = 20000)
    public void testSkipReplicatedSubscriptionsMarker() throws Exception {
        final String topic = "testSkipReplicatedSubscriptionsMarker";
        final List<String> messages = IntStream.range(0, 10).mapToObj(Integer::toString).collect(Collectors.toList());
        final String subscription = "same-sub-test";

        @Cleanup
        final Producer<byte[]> pulsarProducer = newPulsarProducer(topic);
        Map<String, MarkersMessageIdData> clusters = new TreeMap<>();
        clusters.put("us-east", new MarkersMessageIdData().setLedgerId(10).setEntryId(11));
        clusters.put("us-cent", new MarkersMessageIdData().setLedgerId(20).setEntryId(21));
        ByteBuf subscriptionUpdate = Markers.newReplicatedSubscriptionsUpdate("subscriptionName", clusters);

        Optional<Topic> optionalTopic = pulsar.getBrokerService()
                .getTopicIfExists(KopTopic.toString(topic, 0, "public/default")).get();
        assertTrue(optionalTopic.isPresent());
        Topic t = optionalTopic.get();
        CompletableFuture<Void> future = new CompletableFuture<>();
        t.publishMessage(subscriptionUpdate, (e, ledgerId, entryId) -> {
            future.complete(null);
        });
        future.get();
        sendSingleMessages(pulsarProducer, messages);
        KafkaConsumer<String, String> kafkaConsumer = newKafkaConsumer(topic, subscription);
        final List<String> kafkaReceives = receiveMessages(kafkaConsumer, messages.size());
        assertEquals(kafkaReceives, messages);
        kafkaConsumer.commitSync(Duration.ofSeconds(1));
        kafkaConsumer.close();
    }

    @Test(timeOut = 30000)
    public void testPublishZeroTimestampRecord() {
        String topic = "test-publish-zero-timestamp-record";
        String subscription = "test-group";
        Properties properties = newKafkaProducerProperties();
        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        producer.send(new ProducerRecord<>(
                topic,
                null,
                0L,
                "k1",
                "v1",
                null)
        );

        producer.flush();

        @Cleanup
        final KafkaConsumer<String, String> consumer = newKafkaConsumer(topic, subscription);
        consumer.subscribe(Collections.singleton(topic));
        List<ConsumerRecord<String, String>> consumerRecords = receiveRecords(consumer, 1);
        assertEquals(consumerRecords.size(), 1);
        assertEquals(consumerRecords.get(0).key(), "k1");
        assertEquals(consumerRecords.get(0).value(), "v1");
        assertEquals(consumerRecords.get(0).timestamp(), 0L);
    }

    @Test(timeOut = 30000)
    public void testPublishTimestampInBatch() {
        String topic = "test-publish-timestamp-in-batch";
        String subscription = "test-group";
        Properties properties = newKafkaProducerProperties();
        int numRecords = 100;
        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < numRecords; i++) {
            producer.send(new ProducerRecord<>(topic, null, (long) i, "k1", "v1", null));
        }

        producer.flush();

        @Cleanup
        final KafkaConsumer<String, String> consumer = newKafkaConsumer(topic, subscription);
        consumer.subscribe(Collections.singleton(topic));
        List<ConsumerRecord<String, String>> consumerRecords = receiveRecords(consumer, numRecords);
        assertEquals(consumerRecords.size(), numRecords);
        for (int i = 0; i < numRecords; i++) {
            assertEquals(consumerRecords.get(i).key(), "k1");
            assertEquals(consumerRecords.get(i).value(), "v1");
            assertEquals(consumerRecords.get(i).timestamp(), i);
        }

        // Test first record has specified timestamp
        producer.send(new ProducerRecord<>(topic, null, 1L, "k1", "v1", null));
        for (int i = 0; i < numRecords; i++) {
            producer.send(new ProducerRecord<>(topic, null, "k1", "v1", null));
        }

        consumerRecords = receiveRecords(consumer, numRecords);
        assertEquals(consumerRecords.size(), numRecords + 1);
        assertEquals(consumerRecords.get(0).key(), "k1");
        assertEquals(consumerRecords.get(0).value(), "v1");
        assertEquals(consumerRecords.get(0).timestamp(), 1L);
        for (int i = 1; i < numRecords + 1; i++) {
            assertEquals(consumerRecords.get(i).key(), "k1");
            assertEquals(consumerRecords.get(i).value(), "v1");
            assertTrue(consumerRecords.get(i).timestamp() > 0);
        }
    }
}
