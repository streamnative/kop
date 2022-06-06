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
package io.streamnative.pulsar.handlers.kop.schemaregistry;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;

/**
 * A channel initializer that initialize channels for the Schema Registry service.
 */
@AllArgsConstructor
public class SchemaRegistryChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final int MAX_FRAME_LENGTH = 5 * 1024 * 1024; // 5MB

    private final SchemaRegistryHandler schemaRegistryHandler;
    private final Consumer<ChannelPipeline> pipelineCustomizer;

    public SchemaRegistryChannelInitializer(SchemaRegistryHandler schemaRegistryHandler) {
        this.schemaRegistryHandler = schemaRegistryHandler;
        this.pipelineCustomizer = null;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        if (pipelineCustomizer != null) {
            pipelineCustomizer.accept(p);
        }
        p.addLast(new HttpServerCodec());
        p.addLast(new HttpObjectAggregator(MAX_FRAME_LENGTH));
        p.addLast(schemaRegistryHandler);
    }

}
