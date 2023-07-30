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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
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
import org.testng.annotations.Test;


/**
 * Test all available joins of Kafka Streams DSL.
 */
@Slf4j
public class GlobalKTableTest extends KafkaStreamsTestBase {
    private final KeyValueMapper<String, Long, Long> keyMapper = (key, value) -> value;
    private final ValueJoiner<Long, String, String> joiner = (value1, value2) -> value1 + "+" + value2;
    private final String globalStore = "globalStore";
    private final Map<String, String> results = new HashMap<>();
    private String globalTableTopic;
    private String streamTopic;
    private GlobalKTable<Long, String> globalTable;
    private KStream<String, Long> stream;
    private ForeachAction<String, String> foreachAction;

    @Override
    protected void createTopics() throws Exception {
        streamTopic = "stream-" + getTestNo();
        globalTableTopic = "globalTable-" + getTestNo();
        admin.topics().createPartitionedTopic(streamTopic, 1);
        admin.topics().createPartitionedTopic(globalTableTopic, 2);
    }

    @Override
    protected String getApplicationIdPrefix() {
        return "globalTableTopic-table-test";
    }

    @Override
    protected void extraSetup() throws Exception {
        globalTable = builder.globalTable(globalTableTopic, Consumed.with(Serdes.Long(), Serdes.String()),
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as(globalStore)
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(Serdes.String()));
        final Consumed<String, Long> stringLongConsumed = Consumed.with(Serdes.String(), Serdes.Long());
        stream = builder.stream(streamTopic, stringLongConsumed);
        foreachAction = results::put;
    }

    @Override
    protected Class<?> getKeySerdeClass() {
        return null;
    }

    @Override
    protected Class<?> getValueSerdeClass() {
        return null;
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
                kafkaStreams.store(
                        StoreQueryParameters.fromNameAndType(globalStore, QueryableStoreTypes.keyValueStore()));

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
                kafkaStreams.store(
                        StoreQueryParameters
                                .fromNameAndType(globalStore, QueryableStoreTypes.<Long, String>keyValueStore()));

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
                kafkaStreams.store(
                        StoreQueryParameters.fromNameAndType(globalStore, QueryableStoreTypes.keyValueStore()));
        assertEquals(store.approximateNumEntries(), 4L);
        kafkaStreams.close();

        startStreams();
        Thread.sleep(1000); // NOTE: it may take a few milliseconds to wait streams started
        store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(globalStore, QueryableStoreTypes.keyValueStore()));
        assertEquals(store.approximateNumEntries(), 4L);
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
