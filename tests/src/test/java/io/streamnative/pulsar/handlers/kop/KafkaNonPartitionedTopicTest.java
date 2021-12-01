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

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.Cleanup;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test unit for non-partitioned topic.
 */
public class KafkaNonPartitionedTopicTest extends KopProtocolHandlerTestBase {

    private static final String TENANT = "KafkaNonPartitionedTopicTest";
    private static final String NAMESPACE = "ns1";


    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.resetConfig();

        conf.setKafkaTenant(TENANT);
        conf.setKafkaNamespace(NAMESPACE);
        conf.setKafkaMetadataTenant("internal");
        conf.setKafkaMetadataNamespace("__kafka");
        conf.setKafkaTransactionCoordinatorEnabled(true);

        conf.setClusterName(super.configClusterName);
        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 30000)
    public void testNonPartitionedTopic() throws PulsarAdminException {
        String topic = "persistent://" + TENANT + "/" + NAMESPACE + "/" + "testNonPartitionedTopic";
        admin.topics().createNonPartitionedTopic(topic);
        try {
            @Cleanup
            KProducer kProducer = new KProducer(topic, false, getKafkaBrokerPort());

            int totalMsgs = 50;
            String messageStrPrefix = topic + "_message_";

            for (int i = 0; i < totalMsgs; i++) {
                String messageStr = messageStrPrefix + i;
                kProducer.getProducer().send(new ProducerRecord<>(topic, i, messageStr));
            }
            @Cleanup
            KConsumer kConsumer = new KConsumer(topic, getKafkaBrokerPort(), "DemoKafkaOnPulsarConsumer");

            kConsumer.getConsumer().subscribe(Collections.singleton(topic));

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

            // No more records
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofMillis(200));
            assertTrue(records.isEmpty());

            // Ensure that we can list the topic
            Map<String, List<PartitionInfo>> result = kConsumer
                    .getConsumer().listTopics(Duration.ofSeconds(1));
            assertEquals(result.size(), 1);
        } finally {
            admin.topics().delete(topic);
        }
    }

}
