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
import io.streamnative.kop.coordinator.group.GroupCoordinator;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;

/**
 * A channel initializer that initialize channels for kafka protocol.
 */
public class KafkaChannelInitializer extends ChannelInitializer<SocketChannel> {

    static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024; // 100MB

    @Getter
    private final PulsarService pulsarService;
    @Getter
    private final KafkaServiceConfiguration kafkaConfig;
    @Getter
    private final KafkaTopicManager kafkaTopicManager;
    @Getter
    private final GroupCoordinator groupCoordinator;
    // TODO: handle TLS -- https://github.com/streamnative/kop/issues/2
    //      can turn into get this config from kafkaConfig.
    private final boolean enableTls;

    public KafkaChannelInitializer(PulsarService pulsarService,
                                   KafkaServiceConfiguration kafkaConfig,
                                   KafkaTopicManager kafkaTopicManager,
                                   GroupCoordinator groupCoordinator,
                                   boolean enableTLS) throws Exception {
        super();
        this.pulsarService = pulsarService;
        this.kafkaConfig = kafkaConfig;
        this.kafkaTopicManager = kafkaTopicManager;
        this.groupCoordinator = groupCoordinator;
        this.enableTls = enableTLS;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new LengthFieldPrepender(4));
        ch.pipeline().addLast("frameDecoder",
            new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        ch.pipeline().addLast("handler",
            new KafkaRequestHandler(pulsarService, kafkaConfig, kafkaTopicManager, groupCoordinator));
    }
}
