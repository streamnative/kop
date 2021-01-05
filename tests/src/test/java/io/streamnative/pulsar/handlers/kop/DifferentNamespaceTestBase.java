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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;

import io.streamnative.pulsar.handlers.kop.utils.KopTopic;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * test topics in different namespaces.
 */
@Slf4j
public abstract class DifferentNamespaceTestBase extends KopProtocolHandlerTestBase {

    private static final String DEFAULT_TENANT = "public";
    private static final String DEFAULT_NAMESPACE = "default";
    private static final String ANOTHER_TENANT = "my-tenant";
    private static final String ANOTHER_NAMESPACE = "my-ns";

    public DifferentNamespaceTestBase(final String entryFormat) {
        super(entryFormat);
    }

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
        super.internalSetup();

        admin.tenants().createTenant(ANOTHER_TENANT,
                new TenantInfo(Sets.newHashSet("admin_user"), Sets.newHashSet(super.configClusterName)));
        admin.namespaces().createNamespace(ANOTHER_TENANT + "/" + ANOTHER_NAMESPACE);
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
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
                    log.error("Failed to send {}", key);
                    fail("Failed to send " + key);
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

    @Test(timeOut = 20000)
    void testListTopics() throws Exception {
        final String topic1ShortName = "list-topics-1";
        final String topic1 = DEFAULT_TENANT + "/" + DEFAULT_NAMESPACE + "/" + topic1ShortName;
        final int numPartitions1 = 3;
        final String topic2 = ANOTHER_TENANT + "/" + ANOTHER_NAMESPACE + "/list-topics-2";
        final int numPartitions2 = 5;

        admin.topics().createPartitionedTopic(topic1, numPartitions1);
        admin.topics().createPartitionedTopic(topic2, numPartitions2);

        @Cleanup
        KConsumer kConsumer = new KConsumer("", getKafkaBrokerPort(), true);
        Map<String, List<PartitionInfo>> topicMap = kConsumer.getConsumer().listTopics(Duration.ofSeconds(5));
        log.info("topicMap: {}", topicMap);

        assertTrue(topicMap.containsKey(topic1ShortName));
        assertEquals(topicMap.get(topic1ShortName).size(), numPartitions1);

        final String key2 = new KopTopic(topic2).getFullName();
        assertTrue(topicMap.containsKey(key2));
        assertEquals(topicMap.get(key2).size(), numPartitions2);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        AdminClient kafkaAdmin = AdminClient.create(props);
        Set<String> topicSet = kafkaAdmin.listTopics().names().get();
        log.info("topicSet: {}", topicSet);
        assertTrue(topicSet.contains(topic1ShortName));
        assertTrue(topicSet.contains(key2));
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
