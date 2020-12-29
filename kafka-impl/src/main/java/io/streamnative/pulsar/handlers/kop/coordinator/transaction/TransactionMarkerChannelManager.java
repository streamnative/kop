package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.utils.ssl.SSLUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.netty.ChannelFutures;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

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
                    });
            return handlerFuture;
        });
    }

}
