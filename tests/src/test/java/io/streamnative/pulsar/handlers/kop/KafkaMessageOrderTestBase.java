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
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.util.FutureUtil;
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
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaBrokerPort());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.LINGER_MS_CONFIG, 100); // give some time to build bigger batches
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize); // avoid all messages are in a single batch

            // 1. produce message with Kafka producer.
            @Cleanup
            KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

            int totalMsgs = batchSize * 2 + batchSize / 2;
            String messageStrPrefix = "Message_Kop_KafkaProducePulsarConsumeOrder_";

            List<CompletableFuture<RecordMetadata>> handles = new ArrayList<>();
            for (int i = 0; i < totalMsgs; i++) {
                final int index = i;
                CompletableFuture<RecordMetadata> result = new CompletableFuture<>();
                handles.add(result);
                producer.send(new ProducerRecord<>(topicName, i, messageStrPrefix + i), (recordMetadata, e) -> {
                    if (e != null) {
                        result.completeExceptionally(e);
                    } else {
                        log.info("Success written message {} to offset {}", index, recordMetadata.offset());
                        result.complete(recordMetadata);
                    }
                });
            }
            FutureUtil.waitForAll(handles).get();

            // 2. Consume messages use Pulsar client Consumer.
            if (conf.getEntryFormat().equals("pulsar")) {
                Message<byte[]> msg = null;
                int numBatches = 0;
                int maxBatchSize = 0;
                List<Integer> receivedKeys = new ArrayList<>();
                for (int i = 0; i < totalMsgs; i++) {
                    msg = consumer.receive(1000, TimeUnit.MILLISECONDS);
                    assertNotNull(msg);
                    Integer key = kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey()));
                    receivedKeys.add(key);
                    assertEquals(messageStrPrefix + key.toString(), new String(msg.getValue()));

                    if (log.isDebugEnabled()) {
                        log.debug("Pulsar consumer get i: {} message: {}, key: {}, msgId {}",
                                i,
                                new String(msg.getData()),
                                kafkaIntDeserialize(Base64.getDecoder().decode(msg.getKey())).toString(),
                                msg.getMessageId());
                    }
                    assertEquals(i, key.intValue(), "Received " + receivedKeys + " at i=" + i);

                    consumer.acknowledge(msg);

                    BatchMessageIdImpl id =
                            (BatchMessageIdImpl) ((TopicMessageIdImpl) msg.getMessageId()).getInnerMessageId();
                    if (id.getBatchIndex() == 0) {
                        numBatches++;
                    }
                    maxBatchSize = Math.max(maxBatchSize, id.getBatchIndex() + 1);
                }

                // verify have received all messages
                msg = consumer.receive(100, TimeUnit.MILLISECONDS);
                assertNull(msg);
                // Check number of batches is in range (1, totalMsgs) to avoid each batch has only one message or all
                // messages are batched into a single batch.
                log.info("Successfully written {} batches, total {} messages to kafka, maxBatchSize is {}",
                        numBatches, totalMsgs, maxBatchSize);
                assertTrue(numBatches > 1 && numBatches < totalMsgs);
            }

            // 3. Consume messages use Kafka consumer.
            @Cleanup
            KConsumer kConsumer = new KConsumer(topicName, getKafkaBrokerPort(), "testKafkaProduce-KafkaConsume");
            kConsumer.getConsumer().subscribe(Collections.singleton(topicName));
            int[] receivedKeys = new int[totalMsgs];
            for (int i = 0; i < totalMsgs; ) {
                ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(5));
                if (records.isEmpty()) {
                    break;
                }
                for (ConsumerRecord<Integer, String> record : records) {
                    if (log.isDebugEnabled()) {
                        log.debug("Kafka consumer get i: {} offset: {}, message: {}, key: {}",
                                i, record.offset(), record.value(), record.key());
                    }
                    receivedKeys[i++] = record.key();
                }
            }
            log.info("Received keys: {}", receivedKeys);
            for (int i = 0; i < totalMsgs; i++) {
                assertEquals(receivedKeys[i], i);
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }


}
