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

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pulsar.broker.service.Topic;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test class for message publish buffer throttle from kop side.
 * */

public abstract class MessagePublishBufferThrottleTestBase extends KopProtocolHandlerTestBase{

    public MessagePublishBufferThrottleTestBase(final String entryFormat) {
        super(entryFormat);
    }

    @Test
    public void testMessagePublishBufferThrottleDisabled() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(-1);
        conf.setMessagePublishBufferCheckIntervalInMillis(10);
        super.internalSetup();

        final String topic = "testMessagePublishBufferThrottleDisabled";
        final String pulsarTopic = "persistent://public/default/" + topic + "-partition-0";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaBrokerPort);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("delivery.timeout.ms", 300000);
        KafkaProducer<byte[], byte[]> producer = new org.apache.kafka.clients.producer.KafkaProducer(properties);

        // send one record to make sure InternalProducer build on broker side
        producer.send(new ProducerRecord<>(topic, "test".getBytes())).get();

        Topic topicRef = pulsar.getBrokerService().getTopicReference(pulsarTopic).get();
        Assert.assertNotNull(topicRef);
        InternalServerCnx internalServerCnx = (InternalServerCnx)
                ((InternalProducer) topicRef.getProducers().values().toArray()[0]).getCnx();
        internalServerCnx.setMessagePublishBufferSize(Long.MAX_VALUE / 2);
        // sleep to make sure the publish buffer check task has been executed
        Thread.sleep(conf.getMessagePublishBufferCheckIntervalInMillis() * 2);
        Assert.assertFalse(pulsar.getBrokerService().isReachMessagePublishBufferThreshold());
        // Make sure the producer can publish succeed.
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(topic, new byte[1024])).get();
        }
        // sleep to make sure the publish buffer check task has been executed
        Thread.sleep(conf.getMessagePublishBufferCheckIntervalInMillis() * 2);
        Assert.assertFalse(pulsar.getBrokerService().isReachMessagePublishBufferThreshold());
        super.internalCleanup();
    }

    @Test
    public void testMessagePublishBufferThrottleEnable() throws Exception {
        // set size for max publish buffer before broker start
        conf.setMaxMessagePublishBufferSizeInMB(1);
        conf.setMessagePublishBufferCheckIntervalInMillis(100);
        super.internalSetup();
        Assert.assertFalse(pulsar.getBrokerService().isReachMessagePublishBufferThreshold());

        final String topic = "testMessagePublishBufferThrottleEnable";
        final String pulsarTopic = "persistent://public/default/" + topic + "-partition-0";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaBrokerPort);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("delivery.timeout.ms", 300000);
        KafkaProducer<byte[], byte[]> producer = new org.apache.kafka.clients.producer.KafkaProducer(properties);

        // send one record to make sure InternalProducer build on broker side
        producer.send(new ProducerRecord<>(topic, "test".getBytes())).get();

        Topic topicRef = pulsar.getBrokerService().getTopicReference(pulsarTopic).get();
        Assert.assertNotNull(topicRef);
        InternalServerCnx internalServerCnx = (InternalServerCnx)
                ((InternalProducer) topicRef.getProducers().values().toArray()[0]).getCnx();
        internalServerCnx.setMessagePublishBufferSize(Long.MAX_VALUE / 2);
        Assert.assertFalse(pulsar.getBrokerService().isReachMessagePublishBufferThreshold());
        // The first message can publish success, but the second message should be blocked
        producer.send(new ProducerRecord<>(topic, new byte[1024])).get(1, TimeUnit.SECONDS);
        // sleep to make sure the publish buffer check task has been executed
        Thread.sleep(conf.getMessagePublishBufferCheckIntervalInMillis() * 2);
        Assert.assertTrue(pulsar.getBrokerService().isReachMessagePublishBufferThreshold());

        internalServerCnx.setMessagePublishBufferSize(0);
        // sleep to make sure the publish buffer check task has been executed
        Thread.sleep(conf.getMessagePublishBufferCheckIntervalInMillis() * 2);
        Assert.assertFalse(pulsar.getBrokerService().isReachMessagePublishBufferThreshold());
        // Make sure the producer can publish succeed.
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(topic, new byte[1024])).get();
        }
        Assert.assertFalse(pulsar.getBrokerService().isReachMessagePublishBufferThreshold());
        Assert.assertEquals(internalServerCnx.getMessagePublishBufferSize(), 0);
        super.internalCleanup();
    }

    @Override
    protected void setup() throws Exception {

    }

    @Override
    protected void cleanup() throws Exception {

    }
}
