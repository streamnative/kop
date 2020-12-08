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
import static org.testng.Assert.assertNull;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit test for KafkaStream.
 */
@Slf4j
public class KafkaStreamsTest extends KopProtocolHandlerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 10000)
    public void testWordCount() throws PulsarAdminException {
        final String bootstrapServers = "localhost:" + getKafkaBrokerPort();
        final String inputTopic = "testWordCount-input";
        final String outputTopic = "testWordCount-output";

        // We must create topics first because KafkaStreams cannot create topics automatically.
        admin.topics().createPartitionedTopic(inputTopic, 1);
        admin.topics().createPartitionedTopic(outputTopic, 1);

        // 1. Create a KafkaStreams to compute word's count, it read records' value as words from the input topic,
        //   then use KTable to write word count to the output topic.
        final Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-test");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // Records should be flushed every 5 seconds so that the word count result can be retrieved after 5 seconds.
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5 * 1000);

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, String> textLines = streamsBuilder.stream(inputTopic);
        final KTable<String, Long> wordCounts = textLines.groupBy((ignored, value) -> value).count();
        wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        @Cleanup
        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), streamsConfig);
        streams.cleanUp();
        streams.start();

        // 2. Send some words to the input topic
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig);

        final Map<String, Long> wordCountMap = ImmutableMap.<String, Long>builder()
                .put("hello", 5L)
                .put("world", 2L)
                .put("kop", 4L)
                .build();
        final List<String> words = wordCountMap.entrySet().stream()
                .flatMap(entry -> Collections.nCopies(entry.getValue().intValue(), entry.getKey()).stream())
                .collect(Collectors.toList());
        Collections.shuffle(words);

        words.forEach(word -> producer.send(new ProducerRecord<>(inputTopic, word), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                assertNull(exception);
                log.info("Send '{}' to {}-{}@{}", word, metadata.topic(), metadata.partition(), metadata.offset());
            }
        }));
        log.info("Send words: {}", words);
        producer.flush();

        // 3. Consume the word count result from the output topic
        final Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        @Cleanup
        final KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Collections.singleton(outputTopic));

        int numToReceive = wordCountMap.size();
        final Set<String> wordSet = new HashSet<>();
        while (numToReceive > 0) {
            for (ConsumerRecord<String, Long> record : consumer.poll(Duration.ofSeconds(3))) {
                log.info("Receive {} => {}", record.key(), record.value());
                assertEquals(record.value(), wordCountMap.get(record.key()));
                wordSet.add(record.key());
                numToReceive--;
            }
        }
        assertEquals(wordSet.size(), wordCountMap.size());
    }
}
