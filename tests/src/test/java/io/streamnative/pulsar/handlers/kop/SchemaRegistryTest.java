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
import static org.testng.Assert.fail;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Test for KoP with Confluent Schema Registry.
 */
@Slf4j
public class SchemaRegistryTest extends KopProtocolHandlerTestBase {

    protected String bootstrapServers;
    protected boolean applyAvroSchemaOnDecode;

    @Factory
    public static Object[] instances() {
        return new Object[] {
                new SchemaRegistryTest("pulsar", false),
                new SchemaRegistryTest("pulsar", true),
                new SchemaRegistryTest("kafka", false),
                new SchemaRegistryTest("kafka", true)
        };
    }

    public SchemaRegistryTest(String entryFormat, boolean applyAvroSchemaOnDecode) {
        super(entryFormat);
        this.applyAvroSchemaOnDecode = applyAvroSchemaOnDecode;
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.enableSchemaRegistry = true;
        this.conf.setKafkaApplyAvroSchemaOnDecode(applyAvroSchemaOnDecode);
        this.internalSetup();
        bootstrapServers = "localhost:" + getKafkaBrokerPort();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        this.internalCleanup();
    }

    private IndexedRecord createAvroRecord() {
        String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
                + "\"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("name", "testUser");
        return avroRecord;
    }

    private KafkaProducer<Integer, Object> createAvroProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, restConnect);
        return new KafkaProducer<>(props);
    }

    private <K, V> KafkaConsumer<K, V> createAvroConsumer() {
        return createAvroConsumer(IntegerDeserializer.class, KafkaAvroDeserializer.class);
    }

    private <K, V> KafkaConsumer<K, V> createAvroConsumer(Class deserializer, Class serializer) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avroGroup");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, serializer);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, restConnect);
        return new KafkaConsumer<>(props);
    }

    @Test(timeOut = 120000)
    public void testAvroProduceAndConsume() throws Throwable {
        String topic = "SchemaRegistryTest-testAvroProduceAndConsume_" + entryFormat + "_" + applyAvroSchemaOnDecode;
        IndexedRecord avroRecord = createAvroRecord();
        Object[] objects = new Object[]{avroRecord, true, 130, 345L, 1.23f, 2.34d, "abc", "def".getBytes()};
        @Cleanup
        KafkaProducer<Integer, Object> producer = createAvroProducer();
        for (int i = 0; i < objects.length; i++) {
            final Object object = objects[i];
            log.info("Sending {}", object);
            producer.send(new ProducerRecord<>(topic, i, object), (metadata, e) -> {
                if (e != null) {
                    log.error("Failed to send {}: {}", object, e.getMessage());
                    fail("Failed to send " + object + ": " + e.getMessage());
                } else {
                    log.info("Success send {} to {}-partition-{}@{}",
                            object, metadata.topic(), metadata.partition(), metadata.offset());
                }
            }).get(10, TimeUnit.SECONDS);
            log.info("Success send final {}", object);
        }
        producer.close();
        log.info("finished sending");

        @Cleanup
        KafkaConsumer<Integer, Object> consumer = createAvroConsumer();
        consumer.subscribe(Collections.singleton(topic));
        int i = 0;
        while (i < objects.length) {
            for (ConsumerRecord<Integer, Object> record : consumer.poll(Duration.ofSeconds(3))) {
                assertEquals(record.key().intValue(), i);
                assertEquals(record.value(), objects[i]);
                i++;
            }
        }
    }

    @Data
    @AllArgsConstructor
    public static class Pojo {
        private String name;
        private int age;
    }

    @Data
    @AllArgsConstructor
    public static class PojoKey {
        private long pk;
    }

    @DataProvider(name = "enableBatchingProvider")
    protected static Object[][] batchProvider() {
        // isBatch
        return new Object[][]{
                {true},
                {false}
        };
    }

    @Test(timeOut = 120000, dataProvider = "enableBatchingProvider")
    public void testProduceAvroPulsarAndConsume(boolean enableBatching) throws Throwable {
        if (!applyAvroSchemaOnDecode) {
            return;
        }
        String topic = "SchemaRegistryTest-testProduceAvroPulsarAndConsume_"
                + entryFormat + "_" + applyAvroSchemaOnDecode + "_" + enableBatching;
        @Cleanup
        Producer<Pojo> pojoProducer = pulsarClient.newProducer(org.apache.pulsar.client.api.Schema.AVRO(Pojo.class))
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(10)
                .blockIfQueueFull(true)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();

        final int numMessages = 100;
        List<CompletableFuture<?>> handles = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            handles.add(pojoProducer.newMessage().value(new Pojo("foo", 1)).key("test").sendAsync());
        }
        FutureUtil.waitForAll(handles).get();

        log.info("finished sending");

        @Cleanup
        KafkaConsumer<String, GenericRecord> consumer = createAvroConsumer(StringDeserializer.class,
                KafkaAvroDeserializer.class);
        consumer.subscribe(Collections.singleton(topic));
        int i = 0;
        while (i < numMessages){
            for (ConsumerRecord<String, GenericRecord> record : consumer.poll(Duration.ofSeconds(3))) {
               assertEquals(record.key(), "test");
               assertEquals(record.value().get("name").toString(), "foo");
               assertEquals(record.value().get("age"), 1);
                i++;
            }
        }
        assertEquals(numMessages, i);
    }

    @Test(timeOut = 120000, dataProvider = "enableBatchingProvider")
    public void testProduceAvroKeyValuePulsarAndConsume(boolean enableBatching) throws Throwable {
        if (!applyAvroSchemaOnDecode) {
            return;
        }
        String topic = "SchemaRegistryTest-testProduceAvroKeyValuePulsarAndConsume_"
                + entryFormat + "_" + applyAvroSchemaOnDecode + "_" + enableBatching;
        @Cleanup
        Producer<KeyValue<PojoKey, Pojo>> pojoProducer = pulsarClient.newProducer(
                org.apache.pulsar.client.api.Schema.KeyValue(
                  org.apache.pulsar.client.api.Schema.AVRO(PojoKey.class),
                  org.apache.pulsar.client.api.Schema.AVRO(Pojo.class),
                        KeyValueEncodingType.SEPARATED)
                )
                .topic(topic)
                .enableBatching(enableBatching)
                .batchingMaxMessages(10)
                .blockIfQueueFull(true)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();

        final int numMessages = 100;
        List<CompletableFuture<?>> handles = new ArrayList<>();
        for (int i = 0; i < numMessages; i++) {
            handles.add(pojoProducer.newMessage().value(
                            new KeyValue<>(new PojoKey(12314L),
                                    new Pojo("foo", 1)))
                    .sendAsync());
        }
        FutureUtil.waitForAll(handles).get();

        log.info("finished sending");

        @Cleanup
        KafkaConsumer<GenericRecord, GenericRecord> consumer = createAvroConsumer(KafkaAvroDeserializer.class,
                KafkaAvroDeserializer.class);
        consumer.subscribe(Collections.singleton(topic));
        int i = 0;
        while (i < numMessages){
            for (ConsumerRecord<GenericRecord, GenericRecord> record : consumer.poll(Duration.ofSeconds(3))) {
                assertEquals(record.key().get("pk"), 12314L);
                assertEquals(record.value().get("name").toString(), "foo");
                assertEquals(record.value().get("age"), 1);
                i++;
            }
        }
        assertEquals(numMessages, i);
    }
}
