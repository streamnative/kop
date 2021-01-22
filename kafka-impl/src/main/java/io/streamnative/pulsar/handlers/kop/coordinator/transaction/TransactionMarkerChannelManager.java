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
package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.utils.ssl.SSLUtils;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.netty.ChannelFutures;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Transaction marker channel manager.
 */
@Slf4j
public class TransactionMarkerChannelManager {

    private final KafkaServiceConfiguration kafkaConfig;
    private final PulsarService pulsarService;
    private final EventLoopGroup eventLoopGroup;
    private final boolean enableTls;
    private final SslContextFactory sslContextFactory;

    private final Bootstrap bootstrap;

    private Map<InetSocketAddress, CompletableFuture<TransactionMarkerChannelHandler>> handlerMap = new HashMap<>();

    public TransactionMarkerChannelManager(KafkaServiceConfiguration kafkaConfig,
                                           PulsarService pulsarService,
                                           boolean enableTls) {
        this.kafkaConfig = kafkaConfig;
        this.pulsarService = pulsarService;
        this.enableTls = enableTls;
        if (this.enableTls) {
            sslContextFactory = SSLUtils.createSslContextFactory(kafkaConfig);
        } else {
            sslContextFactory = null;
        }
        eventLoopGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new TransactionMarkerChannelInitializer(kafkaConfig, pulsarService, enableTls));
    }

    public CompletableFuture<TransactionMarkerChannelHandler> getChannel(InetSocketAddress socketAddress) {
        return handlerMap.computeIfAbsent(socketAddress, address -> {
            CompletableFuture<TransactionMarkerChannelHandler> handlerFuture = new CompletableFuture<>();
            ChannelFutures.toCompletableFuture(bootstrap.connect(socketAddress))
                    .thenAccept(channel -> {
                        handlerFuture.complete(
                                (TransactionMarkerChannelHandler) channel.pipeline().get("handler"));
                    }).exceptionally(e -> {
                handlerFuture.completeExceptionally(e);
                return null;
            });
            return handlerFuture;
        });
    }

}
