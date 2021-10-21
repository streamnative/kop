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

import static io.streamnative.pulsar.handlers.kop.streams.IntegrationTestUtils.waitForCompletion;
import static io.streamnative.pulsar.handlers.kop.streams.TestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE;
import static io.streamnative.pulsar.handlers.kop.streams.TestUtils.purgeLocalStreamsState;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import lombok.NonNull;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class InternalTopicIntegrationTest extends KafkaStreamsTestBase {
    private static final String APP_ID = "internal-topics-integration-test";
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";

    @Override
    protected void createTopics() throws Exception {
        createTopics(DEFAULT_INPUT_TOPIC);
    }

    @Override
    protected @NonNull String getApplicationIdPrefix() {
        return APP_ID;
    }

    @Override
    protected void extraSetup() throws Exception {
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    }

    @Override
    protected Class<?> getKeySerdeClass() {
        return null;
    }

    @Override
    protected Class<?> getValueSerdeClass() {
        return null;
    }

    @AfterMethod
    public void after() throws IOException {
        // Remove any state from previous test runs
        purgeLocalStreamsState(streamsConfiguration);
    }

    private void produceData(final List<String> inputValues) throws Exception {
        final Properties producerProp = new Properties();
        producerProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProp.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProp.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(DEFAULT_INPUT_TOPIC, inputValues, producerProp, mockTime);
    }

    @Test
    public void shouldCompactTopicsForKeyValueStoreChangelogs() throws Exception {
        final String appID = APP_ID + "-compact";
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        //
        // Step 1: Configure and start a simple word count topology
        //
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        textLines.flatMapValues(new ValueMapper<String, Iterable<String>>() {
                    @Override
                    public Iterable<String> apply(final String value) {
                        return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
                    }
                })
                .groupBy(MockMapper.<String, String>selectValueMapper())
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("Counts"));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        produceData(Arrays.asList("hello", "world", "world", "hello world"));

        //
        // Step 3: Verify the state changelog topics are compact
        //
        waitForCompletion(streams, 2, 30000);
        streams.close();

    }

    @Test
    public void shouldCompactAndDeleteTopicsForWindowStoreChangelogs() throws Exception {
        final String appID = APP_ID + "-compact-delete";
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);

        //
        // Step 1: Configure and start a simple word count topology
        //
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        final int durationMs = 2000;

        textLines.flatMapValues(new ValueMapper<String, Iterable<String>>() {
                    @Override
                    public Iterable<String> apply(final String value) {
                        return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
                    }
                })
                .groupBy(MockMapper.<String, String>selectValueMapper())
                .windowedBy(TimeWindows.of(1000).until(2000))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("CountWindows"));

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        produceData(Arrays.asList("hello", "world", "world", "hello world"));

        //
        // Step 3: Verify the state changelog topics are compact
        //
        waitForCompletion(streams, 2, 30000);
        streams.close();
    }
}
