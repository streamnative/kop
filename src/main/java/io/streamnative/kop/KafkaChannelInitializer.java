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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

/**
 * A channel initializer that initialize channels for kafka protocol.
 */
public class KafkaChannelInitializer extends ChannelInitializer<SocketChannel> {

    static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024; // 100MB

    private final KafkaService kafkaService;
    // TODO: handle TLS -- https://github.com/streamnative/kop/issues/2
    private final boolean enableTls;

    public KafkaChannelInitializer(KafkaService kafkaService, boolean enableTLS) throws Exception {
        super();
        this.kafkaService = kafkaService;
        this.enableTls = enableTLS;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new LengthFieldPrepender(4));
        ch.pipeline().addLast("frameDecoder",
            new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        ch.pipeline().addLast("handler", new KafkaRequestHandler(kafkaService));
    }
}
