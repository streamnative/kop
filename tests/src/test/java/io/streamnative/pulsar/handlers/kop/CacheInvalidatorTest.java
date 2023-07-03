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


import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

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
        log.info("Before unload, ReplicaManager log size: {}", getProtocolHandler().getReplicaManager().size());

        log.info("Before unload, LOOKUP_CACHE is {}", KopBrokerLookupManager.LOOKUP_CACHE);
        String namespace = conf.getKafkaTenant() + "/" + conf.getKafkaNamespace();
        BundlesData bundles = pulsar.getAdminClient().namespaces().getBundles(namespace);
        List<String> boundaries = bundles.getBoundaries();
        for (int i = 0; i < boundaries.size() - 1; i++) {
            String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
            pulsar.getAdminClient().namespaces().unloadNamespaceBundle(namespace, bundle);
        }
        namespace = conf.getKafkaMetadataTenant() + "/" + conf.getKafkaMetadataNamespace();
        bundles = pulsar.getAdminClient().namespaces().getBundles(namespace);
        boundaries = bundles.getBoundaries();
        for (int i = 0; i < boundaries.size() - 1; i++) {
            String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
            pulsar.getAdminClient().namespaces().unloadNamespaceBundle(namespace, bundle);
        }

        Awaitility.await().untilAsserted(() -> {
            assertTrue(KopBrokerLookupManager.LOOKUP_CACHE.isEmpty());
        });

        Awaitility.await().untilAsserted(() -> {
            assertEquals(getProtocolHandler().getReplicaManager().size(), 0);
        });

    }

    @DataProvider(name = "testEvents")
    protected static Object[][] testEvents() {
        // isBatch
        return new Object[][]{
                {List.of(TopicOwnershipListener.EventType.LOAD), false, false,
                        List.of(TopicOwnershipListener.EventType.LOAD)},
                {List.of(TopicOwnershipListener.EventType.UNLOAD), true, false,
                        List.of(TopicOwnershipListener.EventType.UNLOAD)},
                {List.of(TopicOwnershipListener.EventType.UNLOAD), false, true,
                        List.of()},
                {List.of(TopicOwnershipListener.EventType.UNLOAD), true, true,
                        List.of(TopicOwnershipListener.EventType.UNLOAD)},
                {List.of(TopicOwnershipListener.EventType.DELETE), true, true,
                        List.of(TopicOwnershipListener.EventType.DELETE)},
                {List.of(TopicOwnershipListener.EventType.DELETE), false, true,
                        List.of(TopicOwnershipListener.EventType.DELETE)},
                // PartitionLog listener
                {List.of(TopicOwnershipListener.EventType.UNLOAD, TopicOwnershipListener.EventType.DELETE),
                        false, true,
                        List.of(TopicOwnershipListener.EventType.DELETE)},
                {List.of(TopicOwnershipListener.EventType.UNLOAD, TopicOwnershipListener.EventType.DELETE),
                        true, true,
                        List.of(TopicOwnershipListener.EventType.UNLOAD, TopicOwnershipListener.EventType.DELETE)},
                {List.of(TopicOwnershipListener.EventType.UNLOAD, TopicOwnershipListener.EventType.DELETE),
                        true, false,
                        List.of(TopicOwnershipListener.EventType.UNLOAD)},
                {List.of(TopicOwnershipListener.EventType.UNLOAD, TopicOwnershipListener.EventType.DELETE),
                        false, false,
                        List.of()},
                // Group and TransactionCoordinators
                {List.of(TopicOwnershipListener.EventType.LOAD,
                        TopicOwnershipListener.EventType.UNLOAD), true, true,
                        List.of(TopicOwnershipListener.EventType.LOAD,
                        TopicOwnershipListener.EventType.UNLOAD)},
                {List.of(TopicOwnershipListener.EventType.LOAD,
                        TopicOwnershipListener.EventType.UNLOAD), false, true,
                        List.of(TopicOwnershipListener.EventType.LOAD)},
                {List.of(TopicOwnershipListener.EventType.LOAD,
                        TopicOwnershipListener.EventType.UNLOAD), true, false,
                        List.of(TopicOwnershipListener.EventType.LOAD,
                        TopicOwnershipListener.EventType.UNLOAD)},
                {List.of(TopicOwnershipListener.EventType.LOAD,
                        TopicOwnershipListener.EventType.UNLOAD), false, false,
                        List.of(TopicOwnershipListener.EventType.LOAD)}
        };
    }

    @Test(dataProvider = "testEvents")
    public void testEvents(List<TopicOwnershipListener.EventType> eventTypes, boolean unload, boolean delete,
                           List<TopicOwnershipListener.EventType> exepctedEventTypes)
            throws Exception {

        KafkaProtocolHandler protocolHandler = getProtocolHandler();

        NamespaceBundleOwnershipListenerImpl bundleListener = protocolHandler.getBundleListener();

        String namespace = tenant + "/" + "my-namespace_test_" + UUID.randomUUID();
        admin.namespaces().createNamespace(namespace, 10);
        NamespaceName namespaceName = NamespaceName.get(namespace);

        Map<TopicOwnershipListener.EventType, List<TopicName>> events = new ConcurrentHashMap<>();

        bundleListener.addTopicOwnershipListener(new TopicOwnershipListener() {
            @Override
            public String name() {
                return "tester";
            }

            @Override
            public void whenLoad(TopicName topicName) {
                log.info("whenLoad {}", topicName);
                events.computeIfAbsent(EventType.LOAD, e -> new CopyOnWriteArrayList<>())
                        .add(topicName);
            }

            @Override
            public void whenUnload(TopicName topicName) {
                log.info("whenUnload {}", topicName);
                events.computeIfAbsent(EventType.UNLOAD, e -> new CopyOnWriteArrayList<>())
                        .add(topicName);
            }

            @Override
            public void whenDelete(TopicName topicName) {
                log.info("whenDelete {}", topicName);
                events.computeIfAbsent(EventType.DELETE, e -> new CopyOnWriteArrayList<>())
                        .add(topicName);
            }

            @Override
            public boolean interestedInEvent(NamespaceName theNamespaceName, EventType event) {
                log.info("interestedInEvent {} {}", theNamespaceName, event);
                return namespaceName.equals(theNamespaceName) && eventTypes.contains(event);
            }
        });

        int numPartitions = 10;
        String topicName = namespace + "/test-topic-" + UUID.randomUUID();
        admin.topics().createPartitionedTopic(topicName, numPartitions);
        admin.lookups().lookupPartitionedTopic(topicName);

        if (unload) {
            admin.topics().unload(topicName);
        }

        if (delete) {
            // DELETE triggers also UNLOAD so we do delete only
            admin.topics().deletePartitionedTopic(topicName);
        }
        if (exepctedEventTypes.isEmpty()) {
            Awaitility.await().during(5, TimeUnit.SECONDS).untilAsserted(() -> {
                log.info("Events {}", events);
                assertTrue(events.isEmpty());
            });
        } else {
            Awaitility.await().untilAsserted(() -> {
                log.info("Events {}", events);
                assertEquals(events.size(), exepctedEventTypes.size());
                for (TopicOwnershipListener.EventType eventType : exepctedEventTypes) {
                    assertEquals(events.get(eventType).size(), numPartitions);
                }
            });
        }

        admin.namespaces().deleteNamespace(namespace, true);

    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setKafkaTransactionCoordinatorEnabled(true);
        conf.setTopicLevelPoliciesEnabled(false);
        super.internalSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }
}
