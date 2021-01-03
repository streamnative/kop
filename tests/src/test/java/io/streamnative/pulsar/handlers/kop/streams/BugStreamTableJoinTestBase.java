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
package io.streamnative.pulsar.handlers.kop.streams;

import static org.testng.Assert.assertEquals;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * Reproduce the bug.
 */
@Slf4j
public abstract class BugStreamTableJoinTestBase extends KopProtocolHandlerTestBase {
    private final String inputTopicLeft = "inputTopicLeft";
    private final String inputTopicRight = "inputTopicRight";
    protected final String outputTopic = "outputTopic";

    protected static String appID = "stream-table-join-integration-test";
    private static final Long COMMIT_INTERVAL = 100L;

    private static final Properties PRODUCER_CONFIG = new Properties();
    private static final Properties RESULT_CONSUMER_CONFIG = new Properties();
    protected static final Properties STREAMS_CONFIG = new Properties();

    private KafkaProducer<Long, String> producer;
    private StreamsBuilder builder;
    protected KStream<Long, String> leftStream;
    protected KTable<Long, String> rightTable;

    private final List<Input<String>> input = Arrays.asList(
            new Input<>(inputTopicLeft, (String) null),
            new Input<>(inputTopicRight, (String) null),
            new Input<>(inputTopicLeft, "A"),
            new Input<>(inputTopicRight, "a")
    );

    final ValueJoiner<String, String, String> valueJoiner = new ValueJoiner<String, String, String>() {
        @Override
        public String apply(final String value1, final String value2) {
            return value1 + "-" + value2;
        }
    };

    public BugStreamTableJoinTestBase(final String entryFormat) {
        super(entryFormat);
    }

    private String bootstrapServers() {
        return "localhost:" + getKafkaBrokerPort();
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        appID = "stream-table-join-integration-test";
        PRODUCER_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG.put(ProducerConfig.RETRIES_CONFIG, 0);
        PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG,
                appID + "stream-table-join-integration-test-result-consumer");
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        RESULT_CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put("internal.leave.group.on.close", true);
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @BeforeMethod
    protected void prepareTopology() throws Exception {
        admin.topics().createPartitionedTopic(inputTopicLeft, 1);
        admin.topics().createPartitionedTopic(inputTopicRight, 1);
        admin.topics().createPartitionedTopic(outputTopic, 1);
        STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        producer = new KafkaProducer<>(PRODUCER_CONFIG);
        builder = new StreamsBuilder();
        leftStream = builder.stream(inputTopicLeft);
        rightTable = builder.table(inputTopicRight);
    }

    @AfterMethod
    protected void closeProducer() throws Exception {
        producer.close(1, TimeUnit.SECONDS);
    }

    void runTest(final List<List<String>> expectedResult) throws Exception {
        assert expectedResult.size() == input.size();

        TestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
        final KafkaStreams streams = new KafkaStreams(builder.build(), STREAMS_CONFIG);

        try {
            streams.start();

            long ts = System.currentTimeMillis();

            final Iterator<List<String>> resultIterator = expectedResult.iterator();
            for (final Input<String> singleInput : input) {
                producer.send(new ProducerRecord<>(singleInput.topic, null, ++ts, singleInput.record.key,
                        singleInput.record.value)).get();

                List<String> expected = resultIterator.next();
                if (expected != null) {
                    checkResult(expected);
                }
            }
        } finally {
            streams.close();
        }
    }

    private void checkResult(final List<String> expectedResult) throws InterruptedException {
        final Properties filtered = new Properties();
        filtered.putAll(RESULT_CONSUMER_CONFIG);
        filtered.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        filtered.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        final Consumer<Object, String> consumer = new KafkaConsumer<>(filtered);
        consumer.subscribe(Collections.singleton(outputTopic));

        final List<String> actualResult = new ArrayList<>();
        int numReceived = 0;
        while (numReceived < expectedResult.size()) {
            //final ConsumerRecords<Object, String> records = consumer.poll(Duration.ofMillis(100));
            final ConsumerRecords<Object, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Object, String> record : records) {
                actualResult.add(record.value());
                log.info("Receive from outputTopic: '{}'", record.value());
            }
            numReceived += records.count();
        }
        assertEquals(actualResult, expectedResult);
    }

    private final class Input<V> {
        String topic;
        KeyValue<Long, V> record;

        Input(final String topic, final V value) {
            this.topic = topic;
            record = KeyValue.pair(0L, value);
        }
    }
}
