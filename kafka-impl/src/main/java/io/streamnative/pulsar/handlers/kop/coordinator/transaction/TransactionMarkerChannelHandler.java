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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.protocol.Errors.REQUEST_TIMED_OUT;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.KopRequestUtils;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;


/**
 * Transaction marker channel handler.
 */
@Slf4j
public class TransactionMarkerChannelHandler extends ChannelInboundHandlerAdapter {

    private final CompletableFuture<ChannelHandlerContext> cnx = new CompletableFuture<>();
    private final ConcurrentLongHashMap<InFlightRequest> inFlightRequestMap = new ConcurrentLongHashMap<>();
    private final ConcurrentLongHashMap<PendingGenericRequest> genericRequestMap = new ConcurrentLongHashMap<>();

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

    @AllArgsConstructor
    private static final class PendingGenericRequest {
        CompletableFuture<AbstractResponse> response;
        ApiKeys apiKeys;
        short apiVersion;
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
        handleAuthentication(channelHandlerContext);
        super.channelActive(channelHandlerContext);
    }

    @Override
    public void channelInactive(ChannelHandlerContext channelHandlerContext) throws Exception {
        log.info("[TransactionMarkerChannelHandler] channelInactive, failing {} + {} pending requests",
                inFlightRequestMap.size(), genericRequestMap.size());
        final Exception exception = new Exception("Connection to remote broker closed");
        inFlightRequestMap.forEach((k, v) -> {
            v.onError(exception);
        });
        inFlightRequestMap.clear();
        genericRequestMap.forEach((k, v)-> {
            v.response.completeExceptionally(exception);
        });
        genericRequestMap.clear();
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
        if (inFlightRequest != null) {
            inFlightRequest.onComplete(nio);
            return;
        }
        PendingGenericRequest genericRequest = genericRequestMap.remove(responseHeader.correlationId());
        if (genericRequest != null) {
            Struct responseBody = genericRequest.apiKeys.parseResponse(genericRequest.apiVersion, nio);
            AbstractResponse response = AbstractResponse.parseResponse(genericRequest.apiKeys, responseBody);
            genericRequest.response.complete(response);
            return;
        }
        log.error("Miss the inFlightRequest with correlationId {}.", responseHeader.correlationId());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable throwable) throws Exception {
        log.error("Transaction marker channel handler caught exception.", throwable);
        final Exception exception =
                new Exception("Transaction marker channel handler caught exception: " + throwable, throwable);
        inFlightRequestMap.forEach((k, v) -> {
            v.onError(exception);
        });
        inFlightRequestMap.clear();
        genericRequestMap.forEach((k, v)-> {
            v.response.completeExceptionally(exception);
        });
        genericRequestMap.clear();
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

    public void handleAuthentication(ChannelHandlerContext channelHandlerContext) {
        if (!transactionMarkerChannelManager.getKafkaConfig().isAuthenticationEnabled()) {
            this.cnx.complete(channelHandlerContext);
            return;
        }
        saslHandshake(channelHandlerContext)
                .thenCompose(this::authenticate)
                .thenApply(cnx::complete)
                .exceptionally(err -> {
                    cnx.completeExceptionally(err);
                    return null;
                });
    }

    private void sendGenericRequestOnTheWire(ChannelHandlerContext channel,
                                             KafkaCommandDecoder.KafkaHeaderAndRequest request,
                                             CompletableFuture<AbstractResponse> result) {
        long correlationId = request.getHeader().correlationId();
        genericRequestMap.put(correlationId, new PendingGenericRequest(result,
                request.getHeader().apiKey(),
                request.getHeader().apiVersion()));
        channel.writeAndFlush(request.getBuffer())
                .addListener(writeFuture -> {
            if (!writeFuture.isSuccess()) {
                genericRequestMap.remove(correlationId);
                // cannot write, so we have to "close()" and trigger failure of every other
                // pending request and discard the reference to this connection
                channel.close();
                result.completeExceptionally(writeFuture.cause());
            }
        });
    }

    private CompletableFuture<ChannelHandlerContext> saslHandshake(ChannelHandlerContext channel) {
        KafkaCommandDecoder.KafkaHeaderAndRequest fullRequest = buildSASLRequest();
        CompletableFuture<AbstractResponse> result = new CompletableFuture<>();
        sendGenericRequestOnTheWire(channel, fullRequest, result);
        result.exceptionally(error -> {
            // ensure that we close the channel
            channel.close();
            return null;
        });
        return result.thenApply(response -> {
            log.debug("SASL Handshake completed with success");
            return channel;
        });
    }

    private KafkaCommandDecoder.KafkaHeaderAndRequest buildSASLRequest() {
        RequestHeader header = new RequestHeader(
                ApiKeys.SASL_HANDSHAKE,
                ApiKeys.SASL_HANDSHAKE.latestVersion(),
                "tx", //ignored
                correlationId.incrementAndGet()
        );
        SaslHandshakeRequest request = new SaslHandshakeRequest
                .Builder("PLAIN")
                .build();
        ByteBuffer buffer = request.serialize(header);
        KafkaCommandDecoder.KafkaHeaderAndRequest fullRequest = new KafkaCommandDecoder.KafkaHeaderAndRequest(
                header,
                request,
                Unpooled.wrappedBuffer(buffer),
                null
        );
        return fullRequest;
    }

    private CompletableFuture<ChannelHandlerContext> authenticate(final ChannelHandlerContext channel) {
        CompletableFuture<ChannelHandlerContext> internal = authenticateInternal(channel);
        // ensure that we close the channel
        internal.exceptionally(error -> {
            channel.close();
            return null;
        });
        return internal;
    }

    private CompletableFuture<ChannelHandlerContext> authenticateInternal(ChannelHandlerContext channel) {
        RequestHeader header = new RequestHeader(
                ApiKeys.SASL_AUTHENTICATE,
                ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                "tx", // ignored
                correlationId.incrementAndGet()
        );
        String prefix = "TX"; // the prefix TX means nothing, it is ignored by SaslUtils#parseSaslAuthBytes
        String authUsername = transactionMarkerChannelManager.getAuthenticationUsername();
        String authPassword = transactionMarkerChannelManager.getAuthenticationPassword();
        String usernamePassword = prefix
                + "\u0000" + authUsername
                + "\u0000" + authPassword;
        byte[] saslAuthBytes = usernamePassword.getBytes(UTF_8);
        SaslAuthenticateRequest request = new SaslAuthenticateRequest
                .Builder(ByteBuffer.wrap(saslAuthBytes))
                .build();

        ByteBuffer buffer = request.serialize(header);

        KafkaCommandDecoder.KafkaHeaderAndRequest fullRequest = new KafkaCommandDecoder.KafkaHeaderAndRequest(
                header,
                request,
                Unpooled.wrappedBuffer(buffer),
                null
        );
        CompletableFuture<AbstractResponse> result = new CompletableFuture<>();
        sendGenericRequestOnTheWire(channel, fullRequest, result);
        return result.thenApply(response -> {
            SaslAuthenticateResponse saslResponse = (SaslAuthenticateResponse) response;
            if (saslResponse.error() != Errors.NONE) {
                log.error("Failed authentication against KOP broker {}{}", saslResponse.error(),
                        saslResponse.errorMessage());
                close();
                throw new CompletionException(saslResponse.error().exception());
            } else {
                log.debug("Success step AUTH to KOP broker {} {} {}", saslResponse.error(),
                        saslResponse.errorMessage(), saslResponse.saslAuthBytes());
            }
            return channel;
        });
    }

}
