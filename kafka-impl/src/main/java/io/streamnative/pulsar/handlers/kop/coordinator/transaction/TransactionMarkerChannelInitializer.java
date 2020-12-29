package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslHandler;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.utils.ssl.SSLUtils;
import org.apache.pulsar.broker.PulsarService;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import static io.streamnative.pulsar.handlers.kop.KafkaChannelInitializer.MAX_FRAME_LENGTH;
import static io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler.TLS_HANDLER;

public class TransactionMarkerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final KafkaServiceConfiguration kafkaConfig;
    private final PulsarService pulsarService;
    private final boolean enableTls;
    private final SslContextFactory sslContextFactory;

    public TransactionMarkerChannelInitializer(KafkaServiceConfiguration kafkaConfig,
                                               PulsarService pulsarService,
                                               boolean enableTls) {
        this.kafkaConfig = kafkaConfig;
        this.pulsarService = pulsarService;
        this.enableTls = enableTls;
        if (enableTls) {
            sslContextFactory = SSLUtils.createSslContextFactory(kafkaConfig);
        } else {
            sslContextFactory = null;
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if (this.enableTls) {
            ch.pipeline().addLast(TLS_HANDLER, new SslHandler(SSLUtils.createSslEngine(sslContextFactory)));
        }
        ch.pipeline().addLast(new LengthFieldPrepender(4));
        ch.pipeline().addLast("frameDecoder",
                new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        ch.pipeline().addLast("handler", new TransactionMarkerChannelHandler());
    }
}
