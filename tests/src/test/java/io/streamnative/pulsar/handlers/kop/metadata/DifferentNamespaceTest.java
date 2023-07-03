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
package io.streamnative.pulsar.handlers.kop.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.internals.Topic;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * test topics in different namespaces.
 */
@Slf4j
public class DifferentNamespaceTest extends KopProtocolHandlerTestBase {

    private static final String DEFAULT_TENANT = "default-tenant";
    private static final String DEFAULT_NAMESPACE = "default-ns";
    private static final String ANOTHER_TENANT = "my-tenant";
    private static final String ANOTHER_NAMESPACE = "my-ns";
    private static final String NOT_ALLOWED_TENANT = "non-kop-tenant";
    private static final String NOT_ALLOWED_NAMESPACE = "non-kop-ns";

    @DataProvider(name = "topics")
    public static Object[][] topics() {
        return new Object[][] {
                { "topic-0" },
                { DEFAULT_TENANT + "/" + DEFAULT_NAMESPACE + "/" + "topic-1" },
                { "persistent://" + DEFAULT_TENANT + "/" + DEFAULT_NAMESPACE + "/" + "topic-2" },
                { ANOTHER_TENANT + "/" + ANOTHER_NAMESPACE + "/" + "topic-3" },
                { "persistent://" + ANOTHER_TENANT + "/" + ANOTHER_NAMESPACE + "/" + "topic-4" },
        };
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setKafkaTenant(DEFAULT_TENANT);
        conf.setKafkaNamespace(DEFAULT_NAMESPACE);
        conf.setKafkaMetadataTenant(DEFAULT_TENANT);
        conf.setKafkaMetadataNamespace(DEFAULT_NAMESPACE);
        super.internalSetup();

        resetAllowdNamespaces();
        admin.tenants().createTenant(ANOTHER_TENANT,
                TenantInfo.builder()
                        .adminRoles(Collections.singleton("admin_user"))
                        .allowedClusters(Collections.singleton(configClusterName))
                        .build());
        admin.namespaces().createNamespace(ANOTHER_TENANT + "/" + ANOTHER_NAMESPACE);
        admin.tenants().createTenant(NOT_ALLOWED_TENANT,
                TenantInfo.builder()
                        .adminRoles(Collections.singleton("admin_user"))
                        .allowedClusters(Collections.singleton(configClusterName))
                        .build());
        admin.namespaces().createNamespace(NOT_ALLOWED_TENANT + "/" + NOT_ALLOWED_NAMESPACE);
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @BeforeMethod
    protected void resetAllowdNamespaces() {
        conf.setKopAllowedNamespaces(Sets.newHashSet(
                DEFAULT_TENANT + "/" + DEFAULT_NAMESPACE,
                ANOTHER_TENANT + "/" + ANOTHER_NAMESPACE)
        );
    }

    @Test(timeOut = 20000, dataProvider = "topics")
    void testSimpleProduceAndConsume(String topic) {
        final int numMessages = 10;
        final String messagePrefix = topic + "-msg-";

        @Cleanup
        KProducer kProducer = new KProducer(topic, false, getKafkaBrokerPort());
        for (int i = 0; i < numMessages; i++) {
            final int key = i;
            kProducer.getProducer().send(new ProducerRecord<>(topic, key, messagePrefix + key),
                    (recordMetadata, e) -> {
                if (e == null) {
                    log.info("Successfully send {} to {}-partition-{}",
                            key, recordMetadata.topic(), recordMetadata.partition());
                } else {
                    log.error("Failed to send {}: {}", key, e.getMessage());
                }
            });
        }

        @Cleanup
        KConsumer kConsumer = new KConsumer(topic, getKafkaBrokerPort(), true);
        kConsumer.getConsumer().subscribe(Collections.singleton(topic));

        int i = 0;
        List<Integer> suffixes = new ArrayList<>();
        while (i < numMessages) {
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                log.info("Receive message [key = {}, value = '{}']", record.key(), record.value());
                assertEquals(messagePrefix + record.key(), record.value());
                suffixes.add(record.key());
                i++;
            }
        }
        assertEquals(i, numMessages);

        List<Integer> sortedSuffixes = suffixes.stream().sorted().collect(Collectors.toList());
        List<Integer> expectedSuffixes = IntStream.range(0, numMessages).boxed().collect(Collectors.toList());
        assertEquals(sortedSuffixes, expectedSuffixes);
    }

    @Test
    public void testListNonexistentNamespace() throws Exception {
        final String defaultNamespace = DEFAULT_TENANT + "/" + DEFAULT_NAMESPACE;
        final String nonexistentNamespace = "xxxxxxx/yyyyyyy";
        conf.setKopAllowedNamespaces(Sets.newHashSet(defaultNamespace, nonexistentNamespace));

        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        @Cleanup
        final AdminClient kafkaAdmin = AdminClient.create(props);

        final String topic = "test-list-nonexistent-namespace";
        kafkaAdmin.createTopics(
                Collections.singletonList(new NewTopic(defaultNamespace + "/" + topic, 1, (short) 1))
        ).all().get();

        // Even if kopAllowedNamespaces contain a nonexistent namespace, it doesn't affect the process for other
        // existent namespaces
        final Set<String> topics = kafkaAdmin.listTopics().names().get();
        assertTrue(topics.contains(topic)); // no namespace prefix for topics in default namespace
        kafkaAdmin.close();
    }

    @Test
    public void testListTopics() throws Exception {
        final String defaultNamespacePrefix = DEFAULT_TENANT + "/" + DEFAULT_NAMESPACE + "/";
        final String anotherNamespacePrefix = ANOTHER_TENANT + "/" + ANOTHER_NAMESPACE + "/";
        final String notAllowedNamespacePrefix = NOT_ALLOWED_TENANT + "/" + NOT_ALLOWED_NAMESPACE + "/";
        final String topicPrefix = "test-list-topics-";

        final List<String> topics = Arrays.asList(
                topicPrefix + "topic-0",
                defaultNamespacePrefix + topicPrefix + "topic-1",
                anotherNamespacePrefix + topicPrefix + "topic-2",
                notAllowedNamespacePrefix + topicPrefix + "topic-3",
                notAllowedNamespacePrefix + topicPrefix + "topic-4"
        );

        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        @Cleanup
        final AdminClient kafkaAdmin = AdminClient.create(props);

        kafkaAdmin.createTopics(topics.stream().map(topic -> new NewTopic(topic, 1, (short) 1))
                .collect(Collectors.toList()));

        // Only first 3 topics are in the allowed namespaces
        final Map<String, Boolean> expectedTopicIsInternalMap = new HashMap<>();
        expectedTopicIsInternalMap.put(topicPrefix + "topic-0", false);
        expectedTopicIsInternalMap.put(topicPrefix + "topic-1", false);
        expectedTopicIsInternalMap.put("persistent://" + anotherNamespacePrefix + topicPrefix + "topic-2", false);
        expectedTopicIsInternalMap.put(Topic.GROUP_METADATA_TOPIC_NAME, true);
        final List<String> expectedNames =
                expectedTopicIsInternalMap.keySet().stream().sorted().collect(Collectors.toList());

        // list internal topics as well
        final ListTopicsResult listTopicsResult = kafkaAdmin.listTopics(new ListTopicsOptions().listInternal(true));
        final List<String> names = listTopicsResult.names().get().stream()
                .filter(expectedNames::contains)
                .sorted().collect(Collectors.toList());
        final Collection<TopicListing> topicListings = listTopicsResult.listings().get();
        final Map<String, Boolean> topicIsInternalMap = topicListings.stream()
                .filter(topicListing -> expectedNames.contains(topicListing.name()))
                .collect(Collectors.toMap(TopicListing::name, TopicListing::isInternal));

        assertEquals(names, expectedNames);
        assertEquals(topicIsInternalMap, expectedTopicIsInternalMap);
    }

    @Test(timeOut = 30000)
    void testCommitOffset() throws Exception {
        final String topic = ANOTHER_TENANT + "/" + ANOTHER_NAMESPACE + "/test-commit-offset";
        final int numMessages = 50;
        final String messagePrefix = topic + "-msg-";

        @Cleanup
        KProducer kProducer = new KProducer(topic, false, getKafkaBrokerPort());
        for (int i = 0; i < numMessages; i++) {
            kProducer.getProducer().send(new ProducerRecord<>(topic, i, messagePrefix + i));
        }

        // disable auto commit
        KConsumer kConsumer1 = new KConsumer(topic, getKafkaBrokerPort(), true);
        kConsumer1.getConsumer().subscribe(Collections.singleton(topic));

        int numMessagesReceived = 0;
        while (numMessagesReceived < numMessages / 2) {
            ConsumerRecords<Integer, String> records = kConsumer1.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                log.info("[Loop 1] Receive message [key = {}, value = '{}']", record.key(), record.value());
                assertEquals(messagePrefix + record.key(), record.value());
                numMessagesReceived++;
            }
        }

        // manual commit first half of messages
        kConsumer1.getConsumer().commitSync(Duration.ofSeconds(2));
        kConsumer1.close();

        // disable auto commit
        KConsumer kConsumer2 = new KConsumer(topic, getKafkaBrokerPort(), false);
        kConsumer2.getConsumer().subscribe(Collections.singleton(topic));
        while (numMessagesReceived < numMessages) {
            ConsumerRecords<Integer, String> records = kConsumer2.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                log.info("[Loop 2] Receive message [key = {}, value = '{}']", record.key(), record.value());
                assertEquals(messagePrefix + record.key(), record.value());
                numMessagesReceived++;
            }
        }

        // manual commit the rest messages
        kConsumer2.getConsumer().commitSync(Duration.ofSeconds(2));
        kConsumer2.close();

        @Cleanup
        KConsumer kConsumer3 = new KConsumer(topic, getKafkaBrokerPort(), false);
        kConsumer3.getConsumer().subscribe(Collections.singleton(topic));
        // the offset is the latest message now, so no records would be received
        ConsumerRecords<Integer, String> records = kConsumer3.getConsumer().poll(Duration.ofSeconds(5));
        assertEquals(records.count(), 0);
    }
}
