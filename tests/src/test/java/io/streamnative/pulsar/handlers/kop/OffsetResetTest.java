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
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for handling out-of-range cases.
 **/
@Slf4j
public class OffsetResetTest extends KopProtocolHandlerTestBase {

    public OffsetResetTest() {
        super("kafka");
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

    @Test(timeOut = 30000)
    public void testResetOffset() throws Exception {
        final String topic = "persistent://public/default/test-reset-offset-topic";
        final String group = "test-reset-offset-groupid";
        final int numPartitions = 1;

        // step1: create topic, produce some messages and consume until the end
        admin.topics().createPartitionedTopic(topic, numPartitions);

        KProducer kProducer = new KProducer(topic, false, getKafkaBrokerPort());

        int totalMsgs = 10;
        String messageStrPrefix = topic + "_message_";

        for (int i = 0; i < totalMsgs; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer().send(new ProducerRecord<>(topic, i, messageStr));
        }
        kProducer.close();
        log.info("finish producing");

        KConsumer kConsumer = new KConsumer(topic, getKafkaBrokerPort(), group);
        kConsumer.getConsumer().subscribe(Collections.singleton(topic));

        int msgs = 0;
        while (msgs < totalMsgs) {
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                msgs++;
            }
        }
        assertEquals(msgs, totalMsgs);
        kConsumer.getConsumer().commitSync();
        log.info("finish consuming");

        // no more records
        ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofMillis(200));
        assertTrue(records.isEmpty());
        kConsumer.close();

        // step2: delete the topic
        admin.topics().deletePartitionedTopic(topic);
        log.info("finish deleting");

        // step3: re-create the topic
        admin.topics().createPartitionedTopic(topic, numPartitions);
        log.info("finish re-creating");

        // step4: re-produce the half of total messages
        kProducer = new KProducer(topic, false, getKafkaBrokerPort());
        for (int i = 0; i < totalMsgs / 2; i++) {
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer().send(new ProducerRecord<>(topic, i, messageStr));
        }
        log.info("finish re-producing");

        // step5: check offset info
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
        AdminClient adminClient = AdminClient.create(properties);

        kConsumer = new KConsumer(topic, getKafkaBrokerPort(), group);
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        long offset = adminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get()
                .get(topicPartition).offset();
        long leo = (long) kConsumer.getConsumer().endOffsets(Collections.singletonList(topicPartition))
                .get(topicPartition);
        log.info("offset:{}, leo:{}, lag:{}", offset, leo, leo - offset);

        // step6: re-consume
        kConsumer.getConsumer().subscribe(Collections.singleton(topic));
        msgs = 0;
        while (msgs < totalMsgs / 2) {
            records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                assertEquals(messageStrPrefix + key.toString(), record.value());
                msgs++;
            }
        }
        assertEquals(msgs, totalMsgs / 2);
        kConsumer.getConsumer().commitSync();
        log.info("finish re-consuming");

        offset = adminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get()
                .get(topicPartition).offset();
        leo = (long) kConsumer.getConsumer().endOffsets(Collections.singletonList(topicPartition))
                .get(topicPartition);
        log.info("offset:{}, leo:{}, lag:{}", offset, leo, leo - offset);

        kProducer.close();
        kConsumer.close();
        adminClient.close();
    }
}
