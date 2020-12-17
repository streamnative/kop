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
import io.streamnative.pulsar.handlers.kop.utils.timer.MockTime;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


/**
 * Test all available joins of Kafka Streams DSL.
 */
@Slf4j
public class GlobalKTableTest extends KopProtocolHandlerTestBase {
    private String bootstrapServers;
    private static int testNo = 0;
    private final MockTime mockTime = new MockTime();
    private final KeyValueMapper<String, Long, Long> keyMapper = (key, value) -> value;
    private final ValueJoiner<Long, String, String> joiner = (value1, value2) -> value1 + "+" + value2;
    private final String globalStore = "globalStore";
    private final Map<String, String> results = new HashMap<>();
    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String globalTableTopic;
    private String streamTopic;
    private GlobalKTable<Long, String> globalTable;
    private KStream<String, Long> stream;
    private ForeachAction<String, String> foreachAction;

    @BeforeSuite
    protected void setupCluster() throws Exception {
        super.internalSetup();
        bootstrapServers = "localhost:" + getKafkaBrokerPort();
    }

    @AfterSuite
    protected void cleanupCluster() throws Exception {
        super.internalCleanup();
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        testNo++;
        createTopics();
        builder = new StreamsBuilder();
        streamsConfiguration = new Properties();
        final String applicationId = "globalTableTopic-table-test-" + testNo;
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(TestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        globalTable = builder.globalTable(globalTableTopic, Consumed.with(Serdes.Long(), Serdes.String()),
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as(globalStore)
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(Serdes.String()));
        final Consumed<String, Long> stringLongConsumed = Consumed.with(Serdes.String(), Serdes.Long());
        stream = builder.stream(streamTopic, stringLongConsumed);
        foreachAction = results::put;
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        TestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test(timeOut = 20000)
    public void shouldKStreamGlobalKTableLeftJoin() throws Exception {
        final KStream<String, String> streamTableJoin = stream.leftJoin(globalTable, keyMapper, joiner);
        streamTableJoin.foreach(foreachAction);
        produceInitialGlobalTableValues();
        startStreams();
        produceTopicValues(streamTopic);

        final Map<String, String> expected = new HashMap<>();
        expected.put("a", "1+A");
        expected.put("b", "2+B");
        expected.put("c", "3+C");
        expected.put("d", "4+D");
        expected.put("e", "5+null");

        TestUtils.waitForCondition(() -> results.equals(expected), 30000L, "waiting for initial values");

        produceGlobalTableValues();

        final ReadOnlyKeyValueStore<Long, String> replicatedStore =
                kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());

        TestUtils.waitForCondition(() -> "J".equals(replicatedStore.get(5L)),
                30000, "waiting for data in replicated store");
        produceTopicValues(streamTopic);

        expected.put("a", "1+F");
        expected.put("b", "2+G");
        expected.put("c", "3+H");
        expected.put("d", "4+I");
        expected.put("e", "5+J");

        TestUtils.waitForCondition(() -> results.equals(expected), 30000L, "waiting for final values");
    }

    @Test(timeOut = 20000)
    public void shouldKStreamGlobalKTableJoin() throws Exception {
        final KStream<String, String> streamTableJoin = stream.join(globalTable, keyMapper, joiner);
        streamTableJoin.foreach(foreachAction);
        produceInitialGlobalTableValues();
        startStreams();
        produceTopicValues(streamTopic);

        final Map<String, String> expected = new HashMap<>();
        expected.put("a", "1+A");
        expected.put("b", "2+B");
        expected.put("c", "3+C");
        expected.put("d", "4+D");

        TestUtils.waitForCondition(() -> results.equals(expected), 30000L, "waiting for initial values");

        produceGlobalTableValues();

        final ReadOnlyKeyValueStore<Long, String> replicatedStore =
                kafkaStreams.store(globalStore, QueryableStoreTypes.<Long, String>keyValueStore());

        TestUtils.waitForCondition(() -> "J".equals(replicatedStore.get(5L)),
                30000, "waiting for data in replicated store");

        produceTopicValues(streamTopic);

        expected.put("a", "1+F");
        expected.put("b", "2+G");
        expected.put("c", "3+H");
        expected.put("d", "4+I");
        expected.put("e", "5+J");

        TestUtils.waitForCondition(() -> results.equals(expected), 30000L, "waiting for final values");
    }

    @Test(timeOut = 20000)
    public void shouldRestoreGlobalInMemoryKTableOnRestart() throws Exception {
        builder = new StreamsBuilder();
        globalTable = builder.globalTable(
                globalTableTopic,
                Consumed.with(Serdes.Long(), Serdes.String()),
                Materialized.as(Stores.inMemoryKeyValueStore(globalStore)));

        produceInitialGlobalTableValues();

        startStreams();
        Thread.sleep(1000); // NOTE: it may take a few milliseconds to wait streams started

        ReadOnlyKeyValueStore<Long, String> store =
                kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());
        assertEquals(store.approximateNumEntries(), 4L);
        kafkaStreams.close();

        startStreams();
        Thread.sleep(1000); // NOTE: it may take a few milliseconds to wait streams started
        store = kafkaStreams.store(globalStore, QueryableStoreTypes.keyValueStore());
        assertEquals(store.approximateNumEntries(), 4L);
    }

    private void createTopics() throws PulsarAdminException {
        streamTopic = "stream-" + testNo;
        globalTableTopic = "globalTable-" + testNo;
        admin.topics().createPartitionedTopic(streamTopic, 1);
        admin.topics().createPartitionedTopic(globalTableTopic, 2);
    }

    private void startStreams() {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }

    private void produceTopicValues(final String topic) throws Exception {
        TestUtils.produceKeyValuesSynchronously(
                topic,
                Arrays.asList(
                        new KeyValue<>("a", 1L),
                        new KeyValue<>("b", 2L),
                        new KeyValue<>("c", 3L),
                        new KeyValue<>("d", 4L),
                        new KeyValue<>("e", 5L)),
                TestUtils.producerConfig(
                        bootstrapServers,
                        StringSerializer.class,
                        LongSerializer.class,
                        new Properties()),
                mockTime);
    }

    private void produceInitialGlobalTableValues() throws Exception {
        TestUtils.produceKeyValuesSynchronously(
                globalTableTopic,
                Arrays.asList(
                        new KeyValue<>(1L, "A"),
                        new KeyValue<>(2L, "B"),
                        new KeyValue<>(3L, "C"),
                        new KeyValue<>(4L, "D")
                ),
                TestUtils.producerConfig(
                        bootstrapServers,
                        LongSerializer.class,
                        StringSerializer.class
                ),
                mockTime);
    }

    private void produceGlobalTableValues() throws Exception {
        TestUtils.produceKeyValuesSynchronously(
                globalTableTopic,
                Arrays.asList(
                        new KeyValue<>(1L, "F"),
                        new KeyValue<>(2L, "G"),
                        new KeyValue<>(3L, "H"),
                        new KeyValue<>(4L, "I"),
                        new KeyValue<>(5L, "J")),
                TestUtils.producerConfig(
                        bootstrapServers,
                        LongSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                mockTime);
    }
}
