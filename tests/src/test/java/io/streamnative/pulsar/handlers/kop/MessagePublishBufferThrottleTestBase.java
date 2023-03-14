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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mockStatic;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.pulsar.broker.PulsarService;
import org.awaitility.Awaitility;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test class for message publish buffer throttle from kop side.
 * */

@Slf4j
public abstract class MessagePublishBufferThrottleTestBase extends KopProtocolHandlerTestBase{

    public MessagePublishBufferThrottleTestBase(final String entryFormat) {
        super(entryFormat);
    }

    @Test
    public void testMessagePublishBufferThrottleDisabled() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(-1);
        super.internalSetup();

        final String topic = "testMessagePublishBufferThrottleDisabled";
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaBrokerPort);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);

        mockBookKeeper.addEntryDelay(1, TimeUnit.SECONDS);

        final byte[] payload = new byte[1024 * 256];
        final int numMessages = 50;
        final AtomicInteger numSend = new AtomicInteger(0);
        for (int i = 0; i < numMessages; i++) {
            final int index = i;
            producer.send(new ProducerRecord<>(topic, payload), (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send {}: {}", index, exception.getMessage());
                    return;
                }
                numSend.getAndIncrement();
            });
        }

        Assert.assertEquals(pulsar.getBrokerService().getPausedConnections(), 0);
        Awaitility.await().untilAsserted(() -> Assert.assertEquals(numSend.get(), numMessages));
        producer.close();
        super.internalCleanup();
    }

    @Test
    public void testMessagePublishBufferThrottleEnable() throws Exception {
        AtomicBoolean pausedCalled = new AtomicBoolean();
        AtomicBoolean resumeCalled = new AtomicBoolean();
        try (MockedStatic<KafkaRequestHandler> utilities = mockStatic(KafkaRequestHandler.class)) {

            utilities.when(() -> {
                KafkaRequestHandler.setPausedConnections(any(PulsarService.class), anyInt());
            }).then(invocation -> {
                    pausedCalled.set(true);
                    int pausedConnections = (int) invocation.getArguments()[0];
                    pulsar.getBrokerService().pausedConnections(pausedConnections);
                    return null;
            });

            utilities.when(() -> {
                KafkaRequestHandler.setPausedConnections(any(PulsarService.class), anyInt());
            }).then(invocation -> {
                resumeCalled.set(true);
                int pausedConnections = (int) invocation.getArguments()[0];
                pulsar.getBrokerService().resumedConnections(pausedConnections);
                return null;
            });

            conf.setMaxMessagePublishBufferSizeInMB(1);
            super.internalSetup();

            final String topic = "testMessagePublishBufferThrottleEnable";
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaBrokerPort);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
            final KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);

            mockBookKeeper.addEntryDelay(1, TimeUnit.SECONDS);

            final byte[] payload = new byte[1024 * 512];
            final int numMessages = 50;
            final AtomicInteger numSend = new AtomicInteger(0);
            for (int i = 0; i < numMessages; i++) {
                final int index = i;
                producer.send(new ProducerRecord<>(topic, payload), (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to send {}: {}", index, exception.getMessage());
                        return;
                    }
                    numSend.getAndIncrement();
                });
            }

            Awaitility.await().untilAsserted(
                    () -> pausedCalled.get());
            Awaitility.await().untilAsserted(() -> Assert.assertEquals(numSend.get(), numMessages));
            Awaitility.await().untilAsserted(
                    () -> resumeCalled.get());
            producer.close();
            super.internalCleanup();
        }
    }

    @Override
    protected void setup() throws Exception {

    }

    @Override
    protected void cleanup() throws Exception {

    }
}
