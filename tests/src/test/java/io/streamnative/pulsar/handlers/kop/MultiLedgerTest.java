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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.time.Duration;
import java.util.Base64;
import java.util.List;
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
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Unit test for Different kafka produce messages.
 */
@Slf4j
public class MultiLedgerTest extends KopProtocolHandlerTestBase {

    public MultiLedgerTest(final String entryFormat) {
        super(entryFormat);
    }

    @Factory
    public static Object[] instances() {
        return new Object[] {
                new MultiLedgerTest("pulsar"),
                new MultiLedgerTest("kafka")
        };
    }

    @Override
    protected void resetConfig() {
        super.resetConfig();
        // set to easy split more ledgers.
        this.conf.setManagedLedgerMaxEntriesPerLedger(5);
        this.conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000)
    public void testProduceConsumeMultiLedger() throws Exception {
        String kafkaTopicName = "kopProduceConsumeMultiLedger";
        String pulsarTopicName = "persistent://public/default/" + kafkaTopicName;

        // create partitioned topic with 1 partition.
        admin.topics().createPartitionedTopic(kafkaTopicName, 1);

        // 1. produce message with Kafka producer.
        @Cleanup
        KProducer kProducer = new KProducer(kafkaTopicName, false, getKafkaBrokerPort());

        int totalMsgs = 50;
        String messageStrPrefix = "Message_Kop_ProduceConsumeMultiLedger_";

        // send in sync mod, each message not batched.
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

        // 2. Consume messages use Pulsar client Consumer. verify content and key is in order
        //    also verify messages are in different ledgers.
        if (conf.getEntryFormat().equals("pulsar")) {
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
                    if (log.isDebugEnabled()) {
                        log.debug("Received Message is null, because receive timeout. Skipped.");
                    }
                    continue;
                }
                Integer key = kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey()));
                assertEquals(messageStrPrefix + key.toString(), new String(msg.getValue()));

                MessageIdImpl messageId =
                        (MessageIdImpl) (((TopicMessageIdImpl) msg.getMessageId()).getInnerMessageId());
                if (log.isDebugEnabled()) {
                    log.info("Pulsar consumer get i: {} , messageId: {}, message: {}, key: {}",
                            i,
                            ((TopicMessageIdImpl) msg.getMessageId()).getInnerMessageId(),
                            new String(msg.getData()),
                            kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey())).toString());
                }
                assertEquals(i, key.intValue());

                // each ledger should only have 5 entry.
                assertEquals(messageId.getEntryId() / 5, 0);

                consumer.acknowledge(msg);
                i++;
            }

            // verify have received all messages
            Message<byte[]> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(msg);
        }

        // 3. use kafka consumer to consume. messages in order.
        @Cleanup
        KConsumer kConsumer = new KConsumer(kafkaTopicName, getKafkaBrokerPort());
        List<TopicPartition> topicPartitions = IntStream.range(0, 1)
            .mapToObj(i -> new TopicPartition(kafkaTopicName, i)).collect(Collectors.toList());
        log.info("Partition size: {}", topicPartitions.size());
        kConsumer.getConsumer().assign(topicPartitions);

        int i = 0;
        while (i < totalMsgs) {
            if (log.isDebugEnabled()) {
                log.debug("start poll message: {}", i);
            }
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Integer key = record.key();
                // verify message in order.
                assertTrue(key.equals(i));
                assertEquals(messageStrPrefix + key.toString(), record.value());

                if (log.isDebugEnabled()) {
                    log.debug("Kafka consumer get message: {}, key: {} at offset {}",
                        record.key(), record.value(), record.offset());
                }
                i++;
            }
        }
        assertEquals(i, totalMsgs);
    }



}
