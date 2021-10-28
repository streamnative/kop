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

import static io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import static org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test for kafkaListenerName config.
 */
@Slf4j
public class KafkaListenerNameTest extends KopProtocolHandlerTestBase {

    @Override
    protected void setup() throws Exception {
        // Set up in the test method
    }

    @Override
    protected void cleanup() throws Exception {
        // Clean up in the test method
    }

    @Test(timeOut = 30000)
    public void testMetadataRequestForMultiListeners() throws Exception {
        final Map<Integer, InetSocketAddress> bindPortToAdvertisedAddress = new HashMap<>();
        final int anotherKafkaPort = PortManager.nextFreePort();
        bindPortToAdvertisedAddress.put(kafkaBrokerPort,
                InetSocketAddress.createUnresolved("192.168.0.1", PortManager.nextFreePort()));
        bindPortToAdvertisedAddress.put(anotherKafkaPort,
                InetSocketAddress.createUnresolved("192.168.0.2", PortManager.nextFreePort()));

        super.resetConfig();
        conf.setKafkaListeners("PLAINTEXT://0.0.0.0:" + kafkaBrokerPort + ",GW://0.0.0.0:" + anotherKafkaPort);
        conf.setKafkaProtocolMap("PLAINTEXT:PLAINTEXT,GW:PLAINTEXT");

        conf.setKafkaAdvertisedListeners(String.format("PLAINTEXT://%s,GW://%s",
                bindPortToAdvertisedAddress.get(kafkaBrokerPort),
                bindPortToAdvertisedAddress.get(anotherKafkaPort)));
        super.internalSetup();

        final String topic = "persistent://public/default/test-metadata-request-for-multi-listeners";
        final int numPartitions = 3;
        admin.topics().createPartitionedTopic(topic, numPartitions);

        final KafkaProtocolHandler protocolHandler = (KafkaProtocolHandler)
                pulsar.getProtocolHandlers().protocol(KafkaProtocolHandler.PROTOCOL_NAME);
        protocolHandler.getChannelInitializerMap().forEach((inetSocketAddress, channelInitializer) -> {
            try {
                final KafkaRequestHandler requestHandler = ((KafkaChannelInitializer) channelInitializer).newCnx();
                ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
                doReturn(mock(Channel.class)).when(mockCtx).channel();
                requestHandler.ctx = mockCtx;

                final InetSocketAddress expectedAddress = bindPortToAdvertisedAddress.get(inetSocketAddress.getPort());

                final KafkaHeaderAndRequest metadataRequest = KafkaApisTest.buildRequest(
                        new MetadataRequest.Builder(Collections.singletonList(topic), true),
                        inetSocketAddress);
                final CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
                requestHandler.handleTopicMetadataRequest(metadataRequest, future);
                final MetadataResponse metadataResponse = (MetadataResponse) future.get();
                Assert.assertEquals(metadataResponse.brokers().size(), 1);
                final List<Node> brokers = new ArrayList<>(metadataResponse.brokers());
                Assert.assertEquals(brokers.size(), 1);
                Assert.assertEquals(brokers.get(0).host(), expectedAddress.getHostName());
                Assert.assertEquals(brokers.get(0).port(), expectedAddress.getPort());

                final List<MetadataResponse.TopicMetadata> topicMetadataList =
                        new ArrayList<>(metadataResponse.topicMetadata());
                Assert.assertEquals(topicMetadataList.size(), 1);
                Assert.assertEquals(topicMetadataList.get(0).topic(), topic);

                final List<PartitionMetadata> partitionMetadataList = topicMetadataList.get(0).partitionMetadata();
                Assert.assertEquals(partitionMetadataList.size(), numPartitions);
                for (int i = 0; i < numPartitions; i++) {
                    final PartitionMetadata partitionMetadata = partitionMetadataList.get(i);
                    Assert.assertEquals(partitionMetadata.error(), Errors.NONE);
                    Assert.assertEquals(partitionMetadata.leader().host(), expectedAddress.getHostName());
                    Assert.assertEquals(partitionMetadata.leader().port(), expectedAddress.getPort());
                }
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        });

        super.internalCleanup();
    }

    @Test(timeOut = 30000)
    public void testListenerName() throws Exception {
        super.resetConfig();
        conf.setAdvertisedAddress(null);
        final String localAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(null);
        conf.setInternalListenerName("pulsar");
        final String advertisedListeners =
                "pulsar:pulsar://" + localAddress + ":" + brokerPort
                        + ",kafka:pulsar://localhost:" + kafkaBrokerPort;
        conf.setAdvertisedListeners(advertisedListeners);
        log.info("Set advertisedListeners to {}", advertisedListeners);
        super.internalSetup();

        kafkaProducerSend("localhost:" + kafkaBrokerPort);

        super.internalCleanup();
    }

    @Test(timeOut = 30000)
    public void testMultipleListenerName() throws Exception {
        super.resetConfig();
        conf.setAdvertisedAddress(null);
        final String localAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(null);
        conf.setInternalListenerName("pulsar");
        final String kafkaProtocolMap = "kafka:PLAINTEXT,kafka_external:PLAINTEXT";
        conf.setKafkaProtocolMap(kafkaProtocolMap);
        int externalPort = PortManager.nextFreePort();
        final String kafkaListeners = "kafka://0.0.0.0:" + kafkaBrokerPort
                + ",kafka_external://0.0.0.0:" + externalPort;
        conf.setKafkaListeners(kafkaListeners);
        final String advertisedListeners =
                "pulsar:pulsar://" + localAddress + ":" + brokerPort
                        + ",kafka:pulsar://localhost:" + kafkaBrokerPort
                        + ",kafka_external:pulsar://localhost:" + externalPort;
        conf.setAdvertisedListeners(advertisedListeners);
        log.info("Set advertisedListeners to {}", advertisedListeners);
        super.internalSetup();

        kafkaProducerSend("localhost:" + kafkaBrokerPort);
        kafkaProducerSend("localhost:" + externalPort);

        super.internalCleanup();
    }

    @Test(timeOut = 20000)
    public void testConnectListenerNotExist() throws Exception {
        final int externalPort = PortManager.nextFreePort();
        super.resetConfig();
        conf.setAdvertisedAddress(null);
        conf.setKafkaListeners("kafka://0.0.0.0:" + kafkaBrokerPort + ",kafka_external://0.0.0.0:" + externalPort);
        conf.setKafkaProtocolMap("kafka:PLAINTEXT,kafka_external:PLAINTEXT");
        conf.setAdvertisedListeners("pulsar:pulsar://localhost:" + brokerPort
                + ",kafka:pulsar://localhost:" + kafkaBrokerPort
                + ",kafka_external:pulsar://localhost:" + externalPort);
        super.internalSetup();
        final Properties props = newKafkaProducerProperties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaBrokerPort);
        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        RecordMetadata recordMetadata = producer.send(new ProducerRecord<>("my-topic", "hello")).get();
        Assert.assertNotNull(recordMetadata);
        Assert.assertEquals(0, recordMetadata.offset());
        producer.close();
        super.internalCleanup();
    }


    private void kafkaProducerSend(String server) throws ExecutionException, InterruptedException, TimeoutException {
        final Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>("my-topic", "my-message"), (metadata, exception) -> {
            if (exception == null) {
                log.info("Send to {}", metadata);
            } else {
                log.error("Send failed: {}", exception.getMessage());
            }
        }).get(30, TimeUnit.SECONDS);
        producer.close();
    }
}
