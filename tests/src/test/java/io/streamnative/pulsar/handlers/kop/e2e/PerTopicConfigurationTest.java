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
package io.streamnative.pulsar.handlers.kop.e2e;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for custom topic configuration.
 */
@Slf4j
public class PerTopicConfigurationTest extends KopProtocolHandlerTestBase {


    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setEntryFormat("kafka");
        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "entryFormats")
    public static Object[][] entryFormats() {
        return new Object[][] {
                { "pulsar", 0},
                { "kafka" , 0},
                { "pulsar", 1},
                { "kafka", 1}
        };
    }

    @Test(timeOut = 20000, dataProvider = "entryFormats")
    public void testProduceConsume(String entryFormat, int numPartitions) throws Exception {
        String kafkaTopicName = "testProduceConsume_" + entryFormat + "_" + numPartitions;
        String pulsarTopicName = "persistent://public/default/" + kafkaTopicName;

        Map<String, String> topicProperties = new HashMap<>();
        topicProperties.put("kafkaEntryFormat", entryFormat);
        if (numPartitions > 0) {
            admin.topics().createPartitionedTopic(pulsarTopicName, numPartitions, topicProperties);
        } else {
            admin.topics().createNonPartitionedTopic(pulsarTopicName, topicProperties);
        }
        Map<String, String> properties = admin.topics().getProperties(pulsarTopicName);
        assertEquals(entryFormat, properties.get("kafkaEntryFormat"));
        assertEquals(numPartitions, admin.topics().getPartitionedTopicMetadata(pulsarTopicName).partitions);

        // 1. produce message with Kafka producer.
        @Cleanup
        KProducer kProducer = new KProducer(kafkaTopicName, false, getKafkaBrokerPort());
        int totalMsgs = 10;
        String messageStrPrefix = "test";
        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            ProducerRecord record = new ProducerRecord<>(
                kafkaTopicName,
                i,
                messageStr);

            kProducer.getProducer().send(record).get();

            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
            }
        }

        // 2. Consume messages use Pulsar client Consumer
        if (entryFormat.equals("pulsar")) {
            @Cleanup
            Consumer<byte[]> consumer = pulsarClient.newConsumer()
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .topic(pulsarTopicName)
                    .subscriptionName("test_produce_consume_multi_ledger_sub")
                    .subscribe();

            int i = 0;
            while (i < totalMsgs) {
                Message<byte[]> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
                if (msg == null) {
                    continue;
                }
                Integer key = kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey()));
                assertEquals(messageStrPrefix + key.toString(), new String(msg.getValue()));
                assertEquals(i, key.intValue());
                consumer.acknowledge(msg);
                i++;
            }

            // verify have received all messages
            Message<byte[]> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(msg);
        }

        @Cleanup
        KConsumer kConsumer = new KConsumer(kafkaTopicName, getKafkaBrokerPort());
        List<TopicPartition> topicPartitions = IntStream.range(0, 1)
            .mapToObj(i -> new TopicPartition(kafkaTopicName, i)).collect(Collectors.toList());
        kConsumer.getConsumer().assign(topicPartitions);

        int i = 0;
        while (i < totalMsgs) {
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                i++;
            }
        }
        assertEquals(i, totalMsgs);
    }
}
