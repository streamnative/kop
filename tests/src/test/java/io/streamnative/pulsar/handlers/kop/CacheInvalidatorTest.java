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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Validate CacheInvalidator.
 */
@Slf4j
public class CacheInvalidatorTest extends KopProtocolHandlerTestBase {


    @Test
    public void testCacheInvalidatorIsTriggered() throws Exception {
        String topicName = "testCacheInvalidatorIsTriggered";
        String kafkaServer = "localhost:" + getKafkaBrokerPort();
        String transactionalId = "xxxx";

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000 * 10);
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topicName, 1, "value")).get();
            producer.commitTransaction();
        }

        try (KConsumer kConsumer = new KConsumer(topicName, getKafkaBrokerPort(), true)) {
            kConsumer.getConsumer().subscribe(Collections.singleton(topicName));
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(5));
            assertNotNull(records);
            assertEquals(1, records.count());
            ConsumerRecord<Integer, String> record = records.iterator().next();
            assertEquals(1, record.key().intValue());
            assertEquals("value", record.value());
        }

        assertFalse(KopBrokerLookupManager.LOOKUP_CACHE.isEmpty());

        BundlesData bundles = pulsar.getAdminClient().namespaces().getBundles(
                conf.getKafkaTenant() + "/" + conf.getKafkaNamespace());
        List<String> boundaries = bundles.getBoundaries();
        for (int i = 0; i < boundaries.size() - 1; i++) {
            String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
            pulsar.getAdminClient().namespaces()
                    .unloadNamespaceBundle(conf.getKafkaTenant() + "/" + conf.getKafkaNamespace(), bundle);
        }

        Awaitility.await().untilAsserted(() -> {
            log.info("LOOKUP_CACHE {}", KopBrokerLookupManager.LOOKUP_CACHE);
            assertTrue(KopBrokerLookupManager.LOOKUP_CACHE.isEmpty());
        });

    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setEnableTransactionCoordinator(true);
        super.internalSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }
}
