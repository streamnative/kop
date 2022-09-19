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
package io.streamnative.pulsar.handlers.kop.http;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract handler for HTTP requests.
 */
@Slf4j
@ChannelHandler.Sharable
public abstract class HttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
    private final List<HttpRequestProcessor> processors = new ArrayList<>();

    /**
     * Add a processor to this handler.
     * @param processor the processor to add
     * @return this handler
     */
    public HttpHandler addProcessor(HttpRequestProcessor processor) {
        this.processors.add(processor);
        return this;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
        if (log.isDebugEnabled()) {
            log.debug("{} at {} request {}", getName(), ctx.channel().localAddress(), request);
        }
        log.info("{} {} {} from {}", getName(), request.method(), request.uri(), ctx.channel().localAddress());

        Optional<HttpRequestProcessor> processorOr =
                processors.stream().filter(p -> p.acceptRequest(request)).findAny();
        if (!processorOr.isPresent()) {
            FullHttpResponse notFoundResponse = getNotFoundResponse(request, ctx);
            ctx.writeAndFlush(notFoundResponse);
            return;
        }

        HttpRequestProcessor processor = processorOr.get();

        CompletableFuture<FullHttpResponse> fullHttpResponse = processor.processRequest(request);
        fullHttpResponse.thenAccept(resp -> {
            if (log.isDebugEnabled()) {
                log.debug("{} at {} request {} response {}", getName(), ctx.channel().localAddress(), request,
                        resp);
            }
            log.info("{} {} {} from {} response {} {}", getName(), request.method(), request.uri(),
                    ctx.channel().localAddress(),
                    resp.status().code(), resp.status().reasonPhrase());
            ctx.writeAndFlush(resp);
        }).exceptionally(err -> {
            FullHttpResponse resp = processor.buildJsonErrorResponse(err);
            if (log.isDebugEnabled()) {
                log.debug("{} at {} request {} response {}", getName(), ctx.channel().localAddress(), request,
                        resp);
            }
            log.info("{} {} {} from {} response {} {}", getName(), request.method(), request.uri(),
                    ctx.channel().localAddress(),
                    resp.status().code(), resp.status().reasonPhrase());
            ctx.writeAndFlush(resp);
            return null;
        });
    }

    protected abstract FullHttpResponse getNotFoundResponse(FullHttpRequest request,
                                                            ChannelHandlerContext ctx);

    protected abstract String getName();

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Unhandled error, closing connection to {}", ctx.channel(), cause);
        ctx.close();
    }
}
