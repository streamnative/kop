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

import static io.streamnative.pulsar.handlers.kop.KafkaChannelInitializer.MAX_FRAME_LENGTH;
import static io.streamnative.pulsar.handlers.kop.KafkaProtocolHandler.TLS_HANDLER;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslHandler;
import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.utils.ssl.SSLUtils;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Transaction marker channel initializer.
 */
public class TransactionMarkerChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final KafkaServiceConfiguration kafkaConfig;
    private final boolean enableTls;
    private final SslContextFactory.Server sslContextFactory;

    public TransactionMarkerChannelInitializer(KafkaServiceConfiguration kafkaConfig,
                                               boolean enableTls) {
        this.kafkaConfig = kafkaConfig;
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
        ch.pipeline().addLast("txnHandler", new TransactionMarkerChannelHandler());
    }
}
