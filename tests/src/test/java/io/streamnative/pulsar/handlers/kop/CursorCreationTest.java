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

import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test the creation of non-durable cursors after fetching messages.
 */
@Slf4j
public class CursorCreationTest extends KopProtocolHandlerTestBase {

    public CursorCreationTest() {
        super("pulsar");
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000)
    public void testCursorCountForMultiGroups() throws Exception {
        final String topic = "test-cursor-count-for-multi-groups";
        final String partitionName = new KopTopic(topic).getPartitionName(0);
        final int numMessages = 100;
        final int numConsumers = 5;

        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());
        for (int i = 0; i < numMessages; i++) {
            producer.send(new ProducerRecord<>(topic, "msg-" + i)).get();
        }
        producer.close();

        final List<KafkaConsumer<String, String>> consumers = IntStream.range(0, numConsumers)
                .mapToObj(i -> {
                    final Properties props = newKafkaConsumerProperties();
                    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + i);
                    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                    consumer.subscribe(Collections.singleton(topic));
                    return consumer;
                }).collect(Collectors.toList());

        final ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
        final List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            final int index = i;
            final KafkaConsumer<String, String> consumer = consumers.get(i);
            futures.add(executor.submit(() -> {
                int numReceived = 0;
                while (numReceived < numMessages) {
                    final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                    records.forEach(record -> {
                        if (log.isDebugEnabled()) {
                            log.debug("Group {} received message {}", index, record.value());
                        }
                    });
                    numReceived += records.count();
                }
            }));
        }
        for (Future<?> future : futures) {
            future.get();
        }

        final PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                .getTopicIfExists(KopTopic.toString(new TopicPartition(topic, 0)))
                .get()
                .orElse(null);
        Assert.assertNotNull(persistentTopic);

        final List<KafkaTopicConsumerManager> tcmList =
                KafkaTopicConsumerManagerCache.getInstance().getTopicConsumerManagers(partitionName);
        Assert.assertEquals(tcmList.size(), 1);

        final KafkaTopicConsumerManager tcm = tcmList.get(0);
        Assert.assertNotNull(tcm);

        //Assert.assertEquals(tcm.getNumCreatedCursors(), numConsumers);
        Assert.assertEquals(tcm.getCreatedCursors().size(), 1);

        // Since consumer close will make connection disconnected and all TCMs will be cleared, we should call it after
        // the test is verified.
        consumers.forEach(KafkaConsumer::close);
        Assert.assertEquals(tcm.getCreatedCursors().size(), 0);
    }
}
