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

import static org.apache.kafka.common.protocol.Errors.REQUEST_TIMED_OUT;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
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
    private final TransactionMarkerChannelManager transactionMarkerChannelManager;

    public TransactionMarkerChannelHandler(
            TransactionMarkerChannelManager transactionMarkerChannelManager) {
        this.transactionMarkerChannelManager = transactionMarkerChannelManager;
    }

    public void enqueueRequest(WriteTxnMarkersRequest request,
                               TransactionMarkerRequestCompletionHandler requestCompletionHandler) {
        InFlightRequest inFlightRequest = new InFlightRequest(request, requestCompletionHandler);
        this.cnx.thenAccept(cnxFuture -> {
            inFlightRequestMap.put(inFlightRequest.requestId, inFlightRequest);
            ByteBuf byteBuf = inFlightRequest.getRequestData();
            cnxFuture.writeAndFlush(byteBuf);
        }).exceptionally(err -> {
            log.error("Cannot send a WriteTxnMarkersRequest request", err);
            inFlightRequest.onError(err);
            return null;
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
            log.info("[TransactionMarkerChannelHandler] onComplete {}", response);
            try {
                requestCompletionHandler.onComplete(response);
            } catch (RuntimeException unhandledError) {
                onError(unhandledError);
            }
        }

        public void onError(Throwable error) {
            log.info("[TransactionMarkerChannelHandler] onError {}", error);
            final List<WriteTxnMarkersRequest.TxnMarkerEntry> markers = request.markers();
            Map<Long, Map<TopicPartition, Errors>> errors = new HashMap<>(markers.size());
            for (WriteTxnMarkersRequest.TxnMarkerEntry entry : markers) {
                Map<TopicPartition, Errors> errorsPerPartition = new HashMap<>(entry.partitions().size());
                for (TopicPartition partition : entry.partitions()) {
                    errorsPerPartition.put(partition, REQUEST_TIMED_OUT);
                    log.error("Handle error " + error
                            + " as REQUEST_TIMED_OUT for " + partition + " producer " + entry.producerId());
                }
                errors.put(entry.producerId(), errorsPerPartition);
            }
            WriteTxnMarkersResponse response = new WriteTxnMarkersResponse(errors);
            requestCompletionHandler.onComplete(response);
        }

    }

    @Override
    public void channelActive(ChannelHandlerContext channelHandlerContext) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("channelActive");
        }
        log.info("[TransactionMarkerChannelHandler] channelActive to {}", channelHandlerContext.channel());
        this.cnx.complete(channelHandlerContext);
        super.channelActive(channelHandlerContext);
    }

    @Override
    public void channelInactive(ChannelHandlerContext channelHandlerContext) throws Exception {
        log.info("[TransactionMarkerChannelHandler] channelInactive, failing {} pending requests",
                inFlightRequestMap.size());
        inFlightRequestMap.forEach((k, v) -> {
            v.onError(new Exception("Connection to remote broker closed"));
        });
        inFlightRequestMap.clear();
        transactionMarkerChannelManager.channelFailed((InetSocketAddress) channelHandlerContext
                .channel()
                .remoteAddress(), this);
        super.channelInactive(channelHandlerContext);
    }

    @Override
    public void channelRead(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
        ByteBuffer nio = ((ByteBuf) o).nioBuffer();
        ResponseHeader responseHeader = ResponseHeader.parse(nio);
        InFlightRequest inFlightRequest = inFlightRequestMap.remove(responseHeader.correlationId());
        if (inFlightRequest == null) {
            log.error("Miss the inFlightRequest with correlationId {}.", responseHeader.correlationId());
            return;
        }
        inFlightRequest.onComplete(nio);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable throwable) throws Exception {
        log.error("Transaction marker channel handler caught exception.", throwable);
        inFlightRequestMap.forEach((k, v) -> {
            v.onError(new Exception("Transaction marker channel handler caught exception: " + throwable, throwable));
        });
        inFlightRequestMap.clear();
        channelHandlerContext.close();
    }

    public void close() {
        log.info("[TransactionMarkerChannelHandler] closing");
        this.cnx.whenComplete((ctx, err) -> {
            if (ctx != null) {
                ctx.close();
            }
        });
    }

}
