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
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.KopRequestUtils;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;


/**
 * Transaction marker channel handler.
 */
@Slf4j
public class TransactionMarkerChannelHandler extends ChannelInboundHandlerAdapter {

    private final CompletableFuture<ChannelHandlerContext> cnx = new CompletableFuture<>();
    private final ConcurrentLongHashMap<InFlightRequest> inFlightRequestMap = new ConcurrentLongHashMap<>();

    private final AtomicInteger correlationId = new AtomicInteger(0);

    public void enqueueRequest(WriteTxnMarkersRequest request,
                               TransactionMarkerRequestCompletionHandler requestCompletionHandler) {
        this.cnx.thenAccept(cnxFuture -> {
            InFlightRequest inFlightRequest = new InFlightRequest(request, requestCompletionHandler);
            inFlightRequestMap.put(inFlightRequest.requestId, inFlightRequest);
            ByteBuf byteBuf = inFlightRequest.getRequestData();
            cnxFuture.writeAndFlush(byteBuf);
        });
    }

    private class InFlightRequest {

        private final long requestId;
        private final WriteTxnMarkersRequest request;
        private final TransactionMarkerRequestCompletionHandler requestCompletionHandler;

        public InFlightRequest(WriteTxnMarkersRequest request,
                               TransactionMarkerRequestCompletionHandler requestCompletionHandler) {
            this.request = request;
            this.requestCompletionHandler = requestCompletionHandler;
            this.requestId = correlationId.incrementAndGet();
        }

        public ByteBuf getRequestData() {
            RequestHeader requestHeader = new RequestHeader(
                    ApiKeys.WRITE_TXN_MARKERS, request.version(), "", (int) requestId);
            return KopRequestUtils.serializeRequest(request.version(), requestHeader, request);
        }

        public void onComplete(ByteBuffer nio) {
            WriteTxnMarkersResponse response = WriteTxnMarkersResponse
                    .parse(nio, ApiKeys.WRITE_TXN_MARKERS.latestVersion());
            requestCompletionHandler.onComplete(response);
        }

    }

    @Override
    public void channelActive(ChannelHandlerContext channelHandlerContext) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("channelActive");
        }
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
        ByteBuffer nio = ((ByteBuf) o).nioBuffer();
        ResponseHeader responseHeader = ResponseHeader.parse(nio);
        InFlightRequest inFlightRequest = inFlightRequestMap.get(responseHeader.correlationId());
        if (inFlightRequest == null) {
            log.error("Miss the inFlightRequest with correlationId {}.", responseHeader.correlationId());
            return;
        }
        inFlightRequest.onComplete(nio);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable throwable) throws Exception {
        log.error("Transaction marker channel handler caught exception.", throwable);
        super.exceptionCaught(channelHandlerContext, throwable);
    }

}
