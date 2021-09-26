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


import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Test for publish time when entry format is kafka.
 */
public class EntryPublishTimeKafkaFormatTest extends EntryPublishTimeTest {
    private static final Logger log = LoggerFactory.getLogger(EntryPublishTimeKafkaFormatTest.class);

    public EntryPublishTimeKafkaFormatTest() {
        super("kafka");
    }


    @Test
    public void testPublishTime() throws Exception {
        String topicName = "publishTime";
        TopicPartition tp = new TopicPartition(topicName, 0);

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

        KConsumer kConsumer = new KConsumer(topicName, "localhost", getKafkaBrokerPort(), false,
                null, null, "KafkaEntryConsumerGroup");
        kConsumer.getConsumer().subscribe(Collections.singleton(topicName));

        for (int i = 0; i < totalMsgs; i++) {
            ConsumerRecords<Integer, String> records = kConsumer.getConsumer().poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                assertTrue(record.timestamp() > startTime);
                assertTrue(record.timestamp() < System.currentTimeMillis());
            }
        }

        // time before first message
        Map<TopicPartition, Long> targetTimes = Maps.newHashMap();
        targetTimes.put(tp, startTime);

        ListOffsetRequest.Builder builder = ListOffsetRequest.Builder
                .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                .setTargetTimes(targetTimes);

        KafkaCommandDecoder.KafkaHeaderAndRequest request = buildRequest(builder);
        CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
        kafkaRequestHandler.handleListOffsetRequest(request, responseFuture);

        AbstractResponse response = responseFuture.get();
        ListOffsetResponse listOffsetResponse = (ListOffsetResponse) response;
        assertEquals(listOffsetResponse.responseData().get(tp).error, Errors.NONE);
        assertEquals((long) listOffsetResponse.responseData().get(tp).offset, 0);
    }

    KafkaCommandDecoder.KafkaHeaderAndRequest buildRequest(AbstractRequest.Builder builder) {
        AbstractRequest request = builder.build();
        builder.apiKey();

        ByteBuffer serializedRequest = request
                .serialize(new RequestHeader(builder.apiKey(), request.version(), "fake_client_id", 0));

        ByteBuf byteBuf = Unpooled.copiedBuffer(serializedRequest);

        RequestHeader header = RequestHeader.parse(serializedRequest);

        ApiKeys apiKey = header.apiKey();
        short apiVersion = header.apiVersion();
        Struct struct = apiKey.parseRequest(apiVersion, serializedRequest);
        AbstractRequest body = AbstractRequest.parseRequest(apiKey, apiVersion, struct);
        return new KafkaCommandDecoder.KafkaHeaderAndRequest(header, body, byteBuf, serviceAddress);
    }

}
