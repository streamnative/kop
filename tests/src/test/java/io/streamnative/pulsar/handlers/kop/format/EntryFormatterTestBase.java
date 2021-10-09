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
package io.streamnative.pulsar.handlers.kop.format;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class EntryFormatterTestBase extends KopProtocolHandlerTestBase {

    private static final String group1 = "test-format-group1";
    private static final String group2 = "test-format-group2";
    private static final String topic = "test-format-topic";

    public EntryFormatterTestBase(final String entryFormat) {
        super(entryFormat);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected void testChangeEntryFormat(final String format) throws Exception {
        // 1. create topic
        int numPartitions = 1;
        admin.topics().createPartitionedTopic(topic, numPartitions);

        // 2. create producer
        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());
        // 3. send messages
        int total = 5;
        for (int i = 0; i < total; i++) {
            String key = "test-format-key-" + i;
            String value = "test-format-value-" + i;
            producer.send(new ProducerRecord<>(topic, key, value));
        }
        producer.close();

        // 4. consume messages use group1 from earliest
        final KafkaConsumer<String, String> consumer1 = new KafkaConsumer<>(newKafkaConsumerProperties(group1));

        consumer1.subscribe(Collections.singleton(topic));
        int consumedMessages = 0;
        while (consumedMessages < total) {
            ConsumerRecords<String, String> records = consumer1.poll(Duration.ofMillis(2000));
            consumedMessages += records.count();
        }
        Assert.assertEquals(total, consumedMessages);
        consumer1.close();

        // 5. change entry format to pulsar from kafka
        changeEntryFormatAndRestart(format);

        // 6. consume messages use group2 from earliest
        KafkaConsumer<String, String> consumer2 = new KafkaConsumer<>(newKafkaConsumerProperties(group2));
        consumer2.subscribe(Collections.singleton(topic));
        consumedMessages = 0;
        while (consumedMessages < total) {
            ConsumerRecords<String, String> records = consumer2.poll(Duration.ofMillis(2000));
            consumedMessages += records.count();
        }
        Assert.assertEquals(total, consumedMessages);
        consumer2.close();
    }

    protected void testChangeKafkaEntryFormat() throws Exception {
        testChangeEntryFormat("pulsar");
    }

    protected void testChangePulsarEntryFormat() throws Exception {
        testChangeEntryFormat("kafka");
    }

    protected void changeEntryFormatAndRestart(final String entryFormat) throws Exception {
        super.changeEntryFormat(entryFormat);
        super.restartBroker();
    }

}
