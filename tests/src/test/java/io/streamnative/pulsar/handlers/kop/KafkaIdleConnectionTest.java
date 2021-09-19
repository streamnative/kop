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

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import io.streamnative.kafka.client.api.KafkaVersion;
import io.streamnative.kafka.client.api.ProducerConfiguration;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.KafkaChannel;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test for KoP Idle connection close.
 */
public class KafkaIdleConnectionTest extends KopProtocolHandlerTestBase {

    protected static final int BUFFER_SIZE = 4 * 1024;
    private static final long DEFAULT_CONNECTION_MAX_IDLE_MS = 9 * 60 * 1000;
    private static final long DEFAULT_BROKER_CONNECTION_MAX_IDLE_MS = 1000;

    private Selector selector;
    private Time time;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setConnectionMaxIdleMs(DEFAULT_BROKER_CONNECTION_MAX_IDLE_MS);
        super.internalSetup();

        time = Time.SYSTEM;
        Metrics metrics = new Metrics(time);
        ProducerConfiguration producerConfiguration = ProducerConfiguration.builder()
                .bootstrapServers("localhost:" + getKafkaBrokerPort())
                .keySerializer(KafkaVersion.DEFAULT.getStringSerializer())
                .valueSerializer(KafkaVersion.DEFAULT.getStringSerializer())
                .build();
        ChannelBuilder channelBuilder =
                ClientUtils.createChannelBuilder(new ProducerConfig(producerConfiguration.toProperties()));
        String clientId = "clientId";
        selector = new Selector(
                DEFAULT_CONNECTION_MAX_IDLE_MS,
                metrics,
                time,
                "test-selector",
                channelBuilder,
                new LogContext(String.format("[Test Selector clientId=%s] ", clientId)));
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testConnectionExceedMaxIdle() throws IOException {
        String id = "0";
        blockingConnect(id);
        long startTimeMs = time.milliseconds();

        assertEquals(selector.channels().size(), 1);
        KafkaChannel channel = selector.channel(id);
        assertTrue(channel.isConnected());

        Awaitility.await().until(() -> {
            poll(selector);
            return !channel.isConnected();
        });

        assertTrue(time.milliseconds() >= (startTimeMs + DEFAULT_BROKER_CONNECTION_MAX_IDLE_MS));
    }

    // connect and wait for the connection to complete
    private void blockingConnect(String node) throws IOException {
        blockingConnect(node, new InetSocketAddress("localhost", getKafkaBrokerPort()));
    }

    protected void blockingConnect(String node, InetSocketAddress serverAddr) throws IOException {
        selector.connect(node, serverAddr, BUFFER_SIZE, BUFFER_SIZE);
        while (!selector.connected().contains(node)) {
            selector.poll(10000L);
        }
        while (!selector.isChannelReady(node)) {
            selector.poll(10000L);
        }
    }

    private void poll(Selector selector) {
        try {
            selector.poll(50);
        } catch (IOException e) {
            Assert.fail("Caught unexpected exception " + e);
        }
    }

}
