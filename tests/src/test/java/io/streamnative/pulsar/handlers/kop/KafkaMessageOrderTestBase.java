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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test for Different kafka produce messages.
 */
@Slf4j
public abstract class KafkaMessageOrderTestBase extends KopProtocolHandlerTestBase {

    public KafkaMessageOrderTestBase(final String entryFormat) {
        super(entryFormat);
    }

    @DataProvider(name = "batchSizeList")
    public static Object[][] batchSizeList() {
        // For the messageStrPrefix in testKafkaProduceMessageOrder(), 100 messages will be split to 50, 34, 25, 20
        // batches associated with following batch.size config.
        return new Object[][] { { 200 }, { 250 }, { 300 }, { 350 } };
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.namespaces().getNamespaces("public").contains("public/__kafka")) {
            admin.namespaces().createNamespace("public/__kafka");
            admin.namespaces().setNamespaceReplicationClusters("public/__kafka", Sets.newHashSet("test"));
            admin.namespaces().setRetention("public/__kafka",
                new RetentionPolicies(-1, -1));
        }
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 20000, dataProvider = "batchSizeList")
    public void testKafkaProduceMessageOrder(int batchSize) throws Exception {
        String topicName = "kopKafkaProducePulsarConsumeMessageOrder-" + batchSize;
        String pulsarTopicName = "persistent://public/default/" + topicName;

        // create partitioned topic with 1 partition.
        pulsar.getAdminClient().topics().createPartitionedTopic(topicName, 1);

        Consumer<byte[]> consumer = null;
        try {
            if (conf.getEntryFormat().equals("pulsar")) {
                // start the Pulsar Consumer only if we are using Pulsar format
                // otherwise it will receive messages that cannot be deserialized in the background
                // consumer loop.
                consumer = pulsarClient.newConsumer()
                        .topic(pulsarTopicName)
                        .subscriptionName("testKafkaProduce-PulsarConsume")
                        .subscribe();
            }

            final Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getClientPort());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize); // avoid all messages are in a single batch

            // 1. produce message with Kafka producer.
            @Cleanup
            KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

            int totalMsgs = 100;
            String messageStrPrefix = "Message_Kop_KafkaProducePulsarConsumeOrder_";

            for (int i = 0; i < totalMsgs; i++) {
                final int index = i;
                producer.send(new ProducerRecord<>(topicName, i, messageStrPrefix + i), (recordMetadata, e) -> {
                    assertNull(e);
                    log.info("Success write message {} to offset {}", index, recordMetadata.offset());
                });
            }

            // 2. Consume messages use Pulsar client Consumer.
            if (conf.getEntryFormat().equals("pulsar")) {
                Message<byte[]> msg = null;
                int numBatches = 0;
                for (int i = 0; i < totalMsgs; i++) {
                    msg = consumer.receive(1000, TimeUnit.MILLISECONDS);
                    assertNotNull(msg);
                    Integer key = kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey()));
                    assertEquals(messageStrPrefix + key.toString(), new String(msg.getValue()));

                    if (log.isDebugEnabled()) {
                        log.debug("Pulsar consumer get i: {} message: {}, key: {}",
                                i,
                                new String(msg.getData()),
                                kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey())).toString());
                    }
                    assertEquals(i, key.intValue());

                    consumer.acknowledge(msg);

                    BatchMessageIdImpl id =
                            (BatchMessageIdImpl) ((TopicMessageIdImpl) msg.getMessageId()).getInnerMessageId();
                    if (id.getBatchIndex() == 0) {
                        numBatches++;
                    }
                }

                // verify have received all messages
                msg = consumer.receive(100, TimeUnit.MILLISECONDS);
                assertNull(msg);
                // Check number of batches is in range (1, totalMsgs) to avoid each batch has only one message or all
                // messages are batched into a single batch.
                log.info("Successfully write {} batches of {} messages to bookie", numBatches, totalMsgs);
                assertTrue(numBatches > 1 && numBatches < totalMsgs);
            }

            // 3. Consume messages use Kafka consumer.
            @Cleanup
            KConsumer kConsumer = new KConsumer(topicName, getClientPort(), "testKafkaProduce-KafkaConsume");
            kConsumer.getConsumer().subscribe(Collections.singleton(topicName));
            for (int i = 0; i < totalMsgs; ) {
                ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
                if (log.isDebugEnabled()) {
                    for (ConsumerRecord<Integer, String> record : records) {
                        log.debug("Kafka consumer get i: {} message: {}, key: {}", i, record.value(), record.key());
                        assertEquals(record.key().intValue(), i);
                        i++;
                    }
                } else {
                    i += records.count();
                }
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }


}
