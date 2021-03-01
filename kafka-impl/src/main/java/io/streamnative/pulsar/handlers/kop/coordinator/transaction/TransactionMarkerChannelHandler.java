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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestUtils;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;

/**
 * Transaction marker channel handler.
 */
@Slf4j
public class TransactionMarkerChannelHandler extends ChannelInboundHandlerAdapter {

    private CompletableFuture<ChannelHandlerContext> cnx = new CompletableFuture<>();

    private final Queue<TxnMarkerRequestResponse> requestQueue = new LinkedBlockingQueue<>();
    private final Queue<TxnMarkerRequestResponse> requestResponseQueue = new LinkedBlockingQueue<>();

    public CompletableFuture<WriteTxnMarkersResponse> enqueueRequest(WriteTxnMarkersRequest request) {
        log.info("enqueueRequest");
        TxnMarkerRequestResponse txnMarkerRequestResponse = new TxnMarkerRequestResponse(request);
        if (requestQueue.offer(txnMarkerRequestResponse)) {
            pollRequest();
            return txnMarkerRequestResponse.responseFuture;
        } else {
            txnMarkerRequestResponse.responseFuture.completeExceptionally(
                    new Exception("The transaction markers queue is full"));
            return txnMarkerRequestResponse.responseFuture;
        }
    }

    public void pollRequest() {
        log.info("poll request queue: {}", requestQueue.size());
        this.cnx.thenAccept(cnxFuture -> {
            TxnMarkerRequestResponse request = requestQueue.poll();
            while (request != null) {
                requestResponseQueue.offer(request);
                ByteBuf byteBuf = request.getRequestData();
                log.info("byteBuff {}", byteBuf);
                cnxFuture.writeAndFlush(byteBuf);
                log.info("poll request write and flush");
                request = requestQueue.poll();
            }
        });
    }

    private static class TxnMarkerRequestResponse {
        private final WriteTxnMarkersRequest request;
        private final CompletableFuture<WriteTxnMarkersResponse> responseFuture = new CompletableFuture<>();

        public TxnMarkerRequestResponse(WriteTxnMarkersRequest request) {
            this.request = request;
        }

        public ByteBuf getRequestData() {
            RequestHeader requestHeader = new RequestHeader(ApiKeys.WRITE_TXN_MARKERS, request.version(), "", -1);
            return RequestUtils.serializeRequest(request.version(), requestHeader, request);
        }

        public void onComplete(ByteBuf byteBuf) {
            WriteTxnMarkersResponse response = WriteTxnMarkersResponse
                    .parse(byteBuf.skipBytes(4).nioBuffer(), ApiKeys.WRITE_TXN_MARKERS.latestVersion());
            responseFuture.complete(response);
        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext channelHandlerContext) throws Exception {
        log.info("[TransactionMarkerChannelHandler] channelRegistered");
        super.channelRegistered(channelHandlerContext);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext channelHandlerContext) throws Exception {
        log.info("[TransactionMarkerChannelHandler] channelUnregistered");
        super.channelUnregistered(channelHandlerContext);
    }

    @Override
    public void channelActive(ChannelHandlerContext channelHandlerContext) throws Exception {
        log.info("[TransactionMarkerChannelHandler] channelActive");
        this.cnx.complete(channelHandlerContext);
        super.channelActive(channelHandlerContext);
    }

    @Override
    public void channelInactive(ChannelHandlerContext channelHandlerContext) throws Exception {
        log.info("[TransactionMarkerChannelHandler] channelInactive");
        super.channelInactive(channelHandlerContext);
    }

    @Override
    public void channelRead(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
        log.info("[TransactionMarkerChannelHandler] channelRead");
        TxnMarkerRequestResponse requestResponse = requestResponseQueue.poll();
        if (requestResponse != null) {
            requestResponse.onComplete((ByteBuf) o);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext channelHandlerContext) throws Exception {
        log.info("[TransactionMarkerChannelHandler] channelReadComplete");
        super.channelReadComplete(channelHandlerContext);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
        log.info("[TransactionMarkerChannelHandler] userEventTriggered");
        super.userEventTriggered(channelHandlerContext, o);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext channelHandlerContext) throws Exception {
        log.info("[TransactionMarkerChannelHandler] channelWritabilityChanged");
        super.channelWritabilityChanged(channelHandlerContext);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable throwable) throws Exception {
        log.info("[TransactionMarkerChannelHandler] exceptionCaught");
        super.exceptionCaught(channelHandlerContext, throwable);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext channelHandlerContext) throws Exception {
        log.info("[TransactionMarkerChannelHandler] handlerAdded");
        super.handlerAdded(channelHandlerContext);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext channelHandlerContext) throws Exception {
        log.info("[TransactionMarkerChannelHandler] handlerRemoved");
        super.handlerRemoved(channelHandlerContext);
    }

}
