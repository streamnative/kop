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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * test topics in different namespaces.
 */
@Slf4j
public class DifferentNamespaceTest extends KopProtocolHandlerTestBase {

    private static final String DEFAULT_TENANT = "public";
    private static final String DEFAULT_NAMESPACE = "default";
    private static final String ANOTHER_TENANT = "my-tenant";
    private static final String ANOTHER_NAMESPACE = "my-ns";


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
        final String topic1 = "list-topics-1";
        final int numPartitions1 = 3;
        final String topic2 = ANOTHER_TENANT + "/" + ANOTHER_NAMESPACE + "/list-topics-2";
        final int numPartitions2 = 5;

        admin.topics().createPartitionedTopic(topic1, numPartitions1);
        admin.topics().createPartitionedTopic(topic2, numPartitions2);

        @Cleanup
        KConsumer kConsumer = new KConsumer("", getKafkaBrokerPort(), true);
        Map<String, List<PartitionInfo>> topicMap = kConsumer.getConsumer().listTopics(Duration.ofSeconds(5));
        log.info("topicMap: {}", topicMap);

        final String key1 = new KopTopic(topic1).getFullName();
        assertTrue(topicMap.containsKey(key1));
        assertEquals(topicMap.get(key1).size(), numPartitions1);

        final String key2 = new KopTopic(topic2).getFullName();
        assertTrue(topicMap.containsKey(key2));
        assertEquals(topicMap.get(key2).size(), numPartitions2);
    }
}
