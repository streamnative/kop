/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.streamnative.kop;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.DistributedIdGenerator;
import org.apache.pulsar.broker.zookeeper.aspectj.ClientCnxnAspect;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

/**
 * Main class for Pulsar kafkaBroker service
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

        setProducerNameGenerator(new DistributedIdGenerator(kafkaService.getZkClient(), producerNameGeneratorPath,
            serviceConfig.getClusterName()));

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
        bootstrap.group(getAcceptorGroup(), getWorkerGroup());
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);

        bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(getWorkerGroup()));
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

        // start other housekeeping functions
        this.startStatsUpdater(
            serviceConfig.getStatsUpdateInitialDelayInSecs(),
            serviceConfig.getStatsUpdateFrequencyInSecs());
        this.startInactivityMonitor();
        this.startMessageExpiryMonitor();
        this.startCompactionMonitor();
        this.startBacklogQuotaChecker();
    }
}
