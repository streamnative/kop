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


import static org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test for Different kafka produce messages.
 */
@Slf4j
public class MultiLedgerTest extends KopProtocolHandlerTestBase {

    @Override
    protected void resetConfig() {
        super.resetConfig();
        // set to easy split more ledgers.
        this.conf.setManagedLedgerMaxEntriesPerLedger(5);
        this.conf.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");
    }

    @AfterClass
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

    @Test
    public void testListOffsetForEmptyRolloverLedger() throws Exception {
        final String topic = "test-list-offset-for-empty-rollover-ledger";
        final String partitionName = TopicName.get(topic).getPartition(0).toString();

        admin.topics().createPartitionedTopic(topic, 1);
        admin.lookups().lookupTopic(topic); // trigger the creation of PersistentTopic

        final ManagedLedgerImpl managedLedger = pulsar.getBrokerService().getTopicIfExists(partitionName).get()
                .map(topicObject -> (ManagedLedgerImpl) ((PersistentTopic) topicObject).getManagedLedger())
                .orElse(null);
        assertNotNull(managedLedger);
        managedLedger.getConfig().setMaxEntriesPerLedger(2);

        @Cleanup
        final KafkaProducer<String, String> producer = new KafkaProducer<>(newKafkaProducerProperties());
        final int numLedgers = 5;
        final int numMessages = numLedgers * managedLedger.getConfig().getMaxEntriesPerLedger();
        for (int i = 0; i < numMessages; i++) {
            producer.send(new ProducerRecord<>(partitionName, "msg-" + i)).get();
        }
        assertEquals(managedLedger.getLedgersInfo().size(), numLedgers);

        // Rollover and delete the old ledgers, wait until there is only one empty ledger
        managedLedger.getConfig().setRetentionTime(0, TimeUnit.MILLISECONDS);
        managedLedger.rollCurrentLedgerIfFull();
        Awaitility.await().atMost(Duration.ofSeconds(3))
                .until(() -> managedLedger.getLedgersInfo().size() == 1);
        final List<LedgerInfo> ledgerInfoList = managedLedger.getLedgersInfoAsList();
        assertEquals(ledgerInfoList.size(), 1);
        assertEquals(ledgerInfoList.get(0).getEntries(), 0);

        // Verify listing offsets for earliest returns a correct offset
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(newKafkaConsumerProperties());
        final TopicPartition topicPartition = new TopicPartition(topic, 0);
        try {
            final Map<TopicPartition, Long> partitionToOffset =
                    consumer.beginningOffsets(Collections.singleton(topicPartition), Duration.ofSeconds(2));
            assertTrue(partitionToOffset.containsKey(topicPartition));
            assertEquals(partitionToOffset.get(topicPartition).intValue(), numMessages);
        } catch (Exception e) {
            log.error("Failed to get beginning offsets: {}", e.getMessage());
            fail(e.getMessage());
        }

        // Verify consumer can start consuming from the correct position
        consumer.subscribe(Collections.singleton(topic));
        producer.send(new ProducerRecord<>(topic, "hello"));
        final List<String> receivedValues = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                records.forEach(record -> receivedValues.add(record.value()));
                break;
            }
        }
        assertEquals(receivedValues.size(), 1);
        assertEquals(receivedValues.get(0), "hello");
    }
}
