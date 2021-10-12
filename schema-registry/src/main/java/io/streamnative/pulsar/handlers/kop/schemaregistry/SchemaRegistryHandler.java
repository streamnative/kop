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

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ChannelHandler.Sharable
public class SchemaRegistryHandler extends SimpleChannelInboundHandler {

    private List<HttpRequestProcessor> processors = new ArrayList<>();

    public SchemaRegistryHandler addProcessor(HttpRequestProcessor processor) {
        this.processors.add(processor);
        return this;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (log.isDebugEnabled()) {
            log.debug("SchemaRegistry {}", msg);
        }
        FullHttpRequest request = (FullHttpRequest) msg;
        boolean done = false;
        for (HttpRequestProcessor processor : processors) {
            FullHttpResponse fullHttpResponse = processor.processRequest(request);
            if (fullHttpResponse != null) {
                ctx.writeAndFlush(fullHttpResponse);
                done = true;
                break;
            }
        }
        if (!done) {
            FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1,
                    NOT_FOUND,
                    Unpooled.copiedBuffer("Not found - " + request.uri(), CharsetUtil.UTF_8));
            httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
            ctx.writeAndFlush(httpResponse);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Unhandled error, closing conection to {}", ctx.channel(), cause);
        ctx.close();
    }
}
