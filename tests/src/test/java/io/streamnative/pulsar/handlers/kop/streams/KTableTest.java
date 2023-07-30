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
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.testng.annotations.Test;


/**
 * Test usage of KTable from the Kafka Streams DSL.
 */
@Slf4j
public class KTableTest extends KafkaStreamsTestBase {
    private final String store = "store";
    private final Map<String, String> results = new HashMap<>();
    private String tableTopic;
    private KTable<Long, String> table;

    @Override
    protected void createTopics() throws Exception {
        String streamTopic = "stream-" + getTestNo();
        tableTopic = "table-" + getTestNo();
        admin.topics().createPartitionedTopic(streamTopic, 1);
        admin.topics().createPartitionedTopic(tableTopic, 2);
    }

    @Override
    protected String getApplicationIdPrefix() {
        return "tableTopic-table-test";
    }

    @Override
    protected void extraSetup() {
        table = builder.table(tableTopic, Consumed.with(Serdes.Long(), Serdes.String()),
                Materialized.<Long, String, KeyValueStore<Bytes, byte[]>>as(store).withKeySerde(Serdes.Long())
                        .withValueSerde(Serdes.String()));
    }

    @Override
    protected Class<?> getKeySerdeClass() {
        return null;
    }

    @Override
    protected Class<?> getValueSerdeClass() {
        return null;
    }

    @Test(timeOut = 60000)
    public void kTableReceivesAndUpdatesValues() throws Exception {
        produceInitialTableValues();
        table.mapValues((k, v) -> {
            results.put(String.valueOf(k), v);
            return v;
        });

        startStreams();

        final Map<String, String> expected = new HashMap<>();
        expected.put("1", "A");
        expected.put("2", "B");
        expected.put("3", "C");
        expected.put("4", "D");

        TestUtils.waitForCondition(() -> results.equals(expected), 300000L, "waiting for initial values");

        produceTableValues();

        expected.put("1", "F");
        expected.put("2", "G");
        expected.put("3", "H");
        expected.put("4", "I");
        expected.put("5", "J");

        TestUtils.waitForCondition(() -> results.equals(expected), 30000L, "waiting for final values");
    }

    @Test(timeOut = 20000)
    public void shouldRestoreInMemoryKTableOnRestart() throws Exception {
        builder = new StreamsBuilder();
        table = builder.table(tableTopic, Consumed.with(Serdes.Long(), Serdes.String()),
                Materialized.as(Stores.inMemoryKeyValueStore(store)));

        produceInitialTableValues();

        startStreams();
        Thread.sleep(1000); // NOTE: it may take a few milliseconds to wait streams started
        final ReadOnlyKeyValueStore<Long, String> store =
                kafkaStreams.store(
                        StoreQueryParameters.fromNameAndType(this.store, QueryableStoreTypes.keyValueStore()));
        TestUtils.waitForCondition(() -> store.approximateNumEntries() == 4L, 30000L, "waiting for values");
        kafkaStreams.close();

        startStreams();
        Thread.sleep(1000); // NOTE: it may take a few milliseconds to wait streams started
        final ReadOnlyKeyValueStore<Long, String> recoveredStore =
                kafkaStreams.store(
                        StoreQueryParameters.fromNameAndType(this.store, QueryableStoreTypes.keyValueStore()));
        TestUtils.waitForCondition(() -> {
                         try {
                             return recoveredStore.approximateNumEntries() == 4L;
                         } catch (InvalidStateStoreException err) {
                             log.info("Ignore error {}", err + "");
                             return false;
                         }
                }, 30000L,
                "waiting for recovered values");
    }

    private void produceInitialTableValues() throws Exception {
        TestUtils.produceKeyValuesSynchronously(tableTopic,
                Arrays.asList(new KeyValue<>(1L, "A"), new KeyValue<>(2L, "B"), new KeyValue<>(3L, "C"),
                        new KeyValue<>(4L, "D")),
                TestUtils.producerConfig(bootstrapServers, LongSerializer.class, StringSerializer.class), mockTime);
    }

    private void produceTableValues() throws Exception {
        TestUtils.produceKeyValuesSynchronously(tableTopic,
                Arrays.asList(new KeyValue<>(1L, "F"), new KeyValue<>(2L, "G"), new KeyValue<>(3L, "H"),
                        new KeyValue<>(4L, "I"), new KeyValue<>(5L, "J")),
                TestUtils.producerConfig(bootstrapServers, LongSerializer.class, StringSerializer.class,
                        new Properties()), mockTime);
    }
}
