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

import static org.testng.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Test for publish time when entry format is pulsar.
 */
public class EntryPublishTimePulsarFormatTest extends EntryPublishTimeTest {
    private static final Logger log = LoggerFactory.getLogger(EntryPublishTimePulsarFormatTest.class);


    public EntryPublishTimePulsarFormatTest() {
        super("pulsar");
    }

    @Test
    public void testPublishTime() throws Exception {
        String topicName = "publishTime";
        TopicPartition tp = new TopicPartition(topicName, 0);

        // use producer to create some message to get Limit Offset.
        String pulsarTopicName = "persistent://public/default/" + topicName;

        // create partitioned topic.
        admin.topics().createPartitionedTopic(topicName, 1);

        // 1. prepare topic:
        //    use kafka producer to produce 10 messages.
        @Cleanup
        KProducer kProducer = new KProducer(topicName, false, getKafkaBrokerPort());
        int totalMsgs = 10;
        String messageStrPrefix = topicName + "_message_";
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < totalMsgs; i++) {
            Thread.sleep(10);
            String messageStr = messageStrPrefix + i;
            kProducer.getProducer()
                    .send(new ProducerRecord<>(
                            topicName,
                            0,
                            System.currentTimeMillis(),
                            i,
                            messageStr))
                    .get();
            if (log.isDebugEnabled()) {
                log.debug("Kafka Producer Sent message: ({}, {})", i, messageStr);
            }
        }

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(pulsarTopicName)
                .subscriptionName(topicName + "_sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        for (int i = 0; i < totalMsgs; i++) {
            Message<byte[]> msg = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertTrue(msg.getPublishTime() < System.currentTimeMillis());
            assertTrue(msg.getPublishTime() > startTime);
        }
    }
}
