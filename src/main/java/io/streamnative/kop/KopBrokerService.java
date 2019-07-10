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
 * Main class for Pulsar kopBroker service
 */

@Slf4j
public class KopBrokerService extends BrokerService {

    private final KopService kopService;

    public KopBrokerService(KopService kopService) throws Exception {
        super(kopService);
        this.kopService = kopService;
    }

    @Override
    public void start() throws Exception {
        KopServiceConfiguration serviceConfig = kopService.getKopConfig();

        setProducerNameGenerator(new DistributedIdGenerator(kopService.getZkClient(), producerNameGeneratorPath,
            serviceConfig.getClusterName()));

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
        bootstrap.group(getAcceptorGroup(), getWorkerGroup());
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true);

        bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(getWorkerGroup()));
        EventLoopUtil.enableTriggeredMode(bootstrap);

        bootstrap.childHandler(new KopChannelInitializer(kopService, false));

        Optional<Integer> port = serviceConfig.getBrokerServicePort();
        if (port.isPresent()) {
            // Bind and start to accept incoming connections.
            InetSocketAddress addr = new InetSocketAddress(kopService.getBindAddress(), port.get());
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
        // register listener to capture zk-latency
        ClientCnxnAspect.addListener(getZkStatsListener());
        ClientCnxnAspect.registerExecutor(getPulsar().getExecutor());
    }
}
