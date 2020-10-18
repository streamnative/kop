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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) {
        Map<String, String> map = System.getenv();
        final String broker = map.getOrDefault("KOP_BROKER", "localhost:9092");
        final String topic = map.getOrDefault("KOP_TOPIC", "kafka-client");
        final int limit = Integer.parseInt(map.getOrDefault("KOP_LIMIT", "10"));
        final boolean shouldProduce = Boolean.parseBoolean(map.getOrDefault("KOP_PRODUCE", "false"));
        final boolean shouldConsume = Boolean.parseBoolean(map.getOrDefault("KOP_CONSUME", "false"));
        final String stringSerializer = "org.apache.kafka.common.serialization.StringSerializer";
        final String stringDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
        final String group = "Subscription";

        if (shouldProduce) {
            System.out.println("starting to produce");

            final Properties props = new Properties();
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
            props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializer);
            props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializer);

            final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

            AtomicInteger numMessagesSent = new AtomicInteger(0);
            for (int i = 0; i < limit; i++) {
                producer.send(new ProducerRecord<>(topic, "hello from kafka-client"),
                        (recordMetadata, e) -> {
                            if (e == null) {
                                System.out.println("Send to " + recordMetadata);
                                numMessagesSent.incrementAndGet();
                            } else {
                                System.out.println("Failed to send: " + e.getMessage());
                            }
                        });
            }

            producer.flush();
            producer.close();
            if (numMessagesSent.get() == limit) {
                System.out.println("produced all messages successfully");
            }
        }

        if (shouldConsume) {
            System.out.println("starting to consume");

            final Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, stringDeserializer);
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, stringDeserializer);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singleton(topic));

            int i = 0;
            while (i < limit) {
                ConsumerRecords<String, String> records = consumer.poll(3000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Receive " + record);
                }
                i += records.count();
            }

            consumer.close();
            System.out.println("consumed all messages successfully");
        }

        System.out.println("exiting normally");
    }
}
