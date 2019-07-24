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
package io.streamnative.kop;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.handler.ssl.SslContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceUtil;
import org.apache.pulsar.broker.service.DistributedIdGenerator;
import org.apache.pulsar.broker.service.PulsarChannelInitializer;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

/**
 * Main class for Pulsar kafkaBroker service.
 */
@Slf4j
public class KafkaBrokerService extends BrokerService {

    private final KafkaService kafkaService;

    public KafkaBrokerService(KafkaService kafkaService) throws Exception {
        super(kafkaService);
        this.kafkaService = kafkaService;
    }

    @Override
    public void start() throws Exception {
        KafkaServiceConfiguration serviceConfig = kafkaService.getKafkaConfig();

        DistributedIdGenerator producerNameGenerator =
            new DistributedIdGenerator(
                kafkaService.getZkClient(),
                "/counters/producer-name",
                kafkaService.getConfiguration().getClusterName());

        setProducerNameGenerator(producerNameGenerator);

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
        bootstrap.group(
            getAcceptorGroup(),
            getWorkerGroup());
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);

        bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(
            getWorkerGroup()
        ));
        EventLoopUtil.enableTriggeredMode(bootstrap);

        bootstrap.childHandler(new KafkaChannelInitializer(kafkaService, false));

        Optional<Integer> port = serviceConfig.getKafkaServicePort();
        if (port.isPresent()) {
            // Bind and start to accept incoming connections.
            InetSocketAddress addr = new InetSocketAddress(kafkaService.getBindAddress(), port.get());
            try {
                bootstrap.bind(addr).sync();
            } catch (Exception e) {
                throw new IOException("Failed to bind Kop Broker on " + addr, e);
            }
            log.info("Started Kop Broker service on port {}", port.get());
        }


        // start original Pulsar Broker service
        ServerBootstrap pulsarBootstrap = new ServerBootstrap();
        pulsarBootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
        pulsarBootstrap.group(
            getAcceptorGroup(),
            getWorkerGroup());
        pulsarBootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        pulsarBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
            new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024));

        pulsarBootstrap.channel(EventLoopUtil.getServerSocketChannelClass(getWorkerGroup()));
        EventLoopUtil.enableTriggeredMode(pulsarBootstrap);

        pulsarBootstrap.childHandler(new PulsarChannelInitializer(kafkaService, false));

        Optional<Integer> pulsarPort = serviceConfig.getBrokerServicePort();
        if (port.isPresent()) {
            // Bind and start to accept incoming connections.
            InetSocketAddress addr = new InetSocketAddress(kafkaService.getBindAddress(), pulsarPort.get());
            try {
                pulsarBootstrap.bind(addr).sync();
            } catch (Exception e) {
                throw new IOException("Failed to bind Pulsar broker on " + addr, e);
            }
            log.info("Started Pulsar Broker service on port {}", pulsarPort.get());
        }

        Optional<Integer> tlsPort = serviceConfig.getBrokerServicePortTls();
        if (tlsPort.isPresent()) {
            ServerBootstrap tlsBootstrap = pulsarBootstrap.clone();
            tlsBootstrap.childHandler(new PulsarChannelInitializer(kafkaService, true));
            tlsBootstrap.bind(new InetSocketAddress(kafkaService.getBindAddress(), tlsPort.get())).sync();
            log.info("Started Pulsar Broker TLS service on port {} - TLS provider: {}", tlsPort.get(),
                SslContext.defaultServerProvider());
        }

        // start other housekeeping functions
        BrokerServiceUtil.startStatsUpdater(
            this,
            serviceConfig.getStatsUpdateInitialDelayInSecs(),
            serviceConfig.getStatsUpdateFrequencyInSecs());

        startInactivityMonitor();
        startMessageExpiryMonitor();
        startCompactionMonitor();
        startBacklogQuotaChecker();
    }
}
