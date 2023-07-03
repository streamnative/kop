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
package io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

/**
 * Test for KoP with Confluent Schema Registry.
 */
@Slf4j
public class SchemaRegistryTest extends KopProtocolHandlerTestBase {

    private static final String USER_SCHEMA = "{\"type\":\"record\",\"name\":\"User\","
            + "\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
    protected String bootstrapServers;

    public SchemaRegistryTest() {
        super("pulsar");
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.enableSchemaRegistry = true;
        this.internalSetup();
        bootstrapServers = "localhost:" + getKafkaBrokerPort();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        this.internalCleanup();
    }

    private IndexedRecord createAvroRecord() {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);
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

    private KafkaConsumer<Integer, Object> createAvroConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avroGroup");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, restConnect);
        return new KafkaConsumer<>(props);
    }

    @Test(timeOut = 120000)
    public void testAvroProduceAndConsume() throws Throwable {
        String topic = "SchemaRegistryTest-testAvroProduceAndConsume";
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
        consumer.close();
    }

    @Test
    public void testGetLatestSchemaMetadata() throws Throwable {
        final String topic = "SchemaRegistryTest-testGetLatestSchemaMetadata";
        final String subject = topic + "-value";
        @Cleanup final KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        final Map<String, String> configs = new HashMap<>();
        configs.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, restConnect);
        serializer.configure(configs, false);

        final Field field = AbstractKafkaAvroSerDe.class.getDeclaredField("schemaRegistry");
        field.setAccessible(true);
        final SchemaRegistryClient client = (SchemaRegistryClient) field.get(serializer);
        try {
            client.getLatestSchemaMetadata(subject);
        } catch (RestClientException e) {
            assertEquals(e.getErrorCode(), 404);
            assertTrue(e.getMessage().contains("Not found"));
        }

        @Cleanup final var producer = createAvroProducer();
        producer.send(new ProducerRecord<>(topic, createAvroRecord())).get();

        final var schemaMetadata = client.getLatestSchemaMetadata(subject);
        assertEquals(schemaMetadata.getVersion(), 1);
        assertEquals(schemaMetadata.getSchema(), USER_SCHEMA);
    }
}
