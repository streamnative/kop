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
import static org.apache.kafka.common.requests.WriteTxnMarkersRequest.TxnMarkerEntry;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import io.streamnative.pulsar.handlers.kop.security.PlainSaslServer;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import javax.security.sasl.SaslException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2;

/**
 * Transaction marker channel handler.
 */
@Slf4j
public class TransactionMarkerChannelHandler extends ChannelInboundHandlerAdapter {

    private final CompletableFuture<ChannelHandlerContext> cnxFuture = new CompletableFuture<>();
    private final CorrelationIdGenerator correlationIdGenerator = new CorrelationIdGenerator();
    private final ResponseContext responseContext = new ResponseContext();
    private final ConcurrentLongHashMap<PendingRequest> pendingRequestMap = new ConcurrentLongHashMap<>();

    private final TransactionMarkerChannelManager transactionMarkerChannelManager;
    private final String mechanism;

    public TransactionMarkerChannelHandler(
            TransactionMarkerChannelManager transactionMarkerChannelManager) {
        this.transactionMarkerChannelManager = transactionMarkerChannelManager;
        if (transactionMarkerChannelManager.getAuthentication() instanceof AuthenticationToken) {
            mechanism = PlainSaslServer.PLAIN_MECHANISM;
        } else if (transactionMarkerChannelManager.getAuthentication() instanceof AuthenticationOAuth2) {
            mechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
        } else {
            mechanism = "";
        }
    }

    private void enqueueRequest(ChannelHandlerContext channel, PendingRequest pendingRequest) {
        final long correlationId = pendingRequest.getCorrelationId();
        pendingRequestMap.put(correlationId, pendingRequest);
        channel.writeAndFlush(Unpooled.wrappedBuffer(pendingRequest.serialize())).addListener(writeFuture -> {
            if (!writeFuture.isSuccess()) {
                pendingRequest.completeExceptionally(writeFuture.cause());
                pendingRequestMap.remove(correlationId);
                // cannot write, so we have to "close()" and trigger failure of every other
                // pending request and discard the reference to this connection
                channel.close();
            }
        });
    }

    public void enqueueWriteTxnMarkers(final List<TxnMarkerEntry> txnMarkerEntries,
                                       final Consumer<ResponseContext> responseContextConsumer) {
        cnxFuture.whenComplete((cnx, e) -> {
            if (e == null) {
                enqueueRequest(cnx, new PendingRequest(
                        ApiKeys.WRITE_TXN_MARKERS,
                        correlationIdGenerator.next(),
                        newWriteTxnMarkers(txnMarkerEntries),
                        responseContextConsumer
                ));
            } else {
                log.error("Failed to enqueue request because the channel failed", e);
            }
        });
    }

    @Override
    public void channelActive(ChannelHandlerContext channelHandlerContext) throws Exception {
        log.info("[TransactionMarkerChannelHandler] Connected to broker {}", channelHandlerContext.channel());
        handleAuthentication(channelHandlerContext);
        super.channelActive(channelHandlerContext);
    }

    @Override
    public void channelInactive(ChannelHandlerContext channelHandlerContext) throws Exception {
        log.info("[TransactionMarkerChannelHandler] channelInactive, failing {} pending requests",
                pendingRequestMap.size());
        pendingRequestMap.forEach((correlationId, pendingRequest) -> {
                log.warn("Pending request ({}) was not sent when the txn marker channel is inactive", pendingRequest);
                pendingRequest.complete(responseContext.set(
                    channelHandlerContext.channel().remoteAddress(),
                    pendingRequest.getApiVersion(),
                    (int) correlationId,
                    pendingRequest.createErrorResponse(new NetworkException())
            ));
        });
        pendingRequestMap.clear();
        transactionMarkerChannelManager.channelFailed((InetSocketAddress) channelHandlerContext
                .channel()
                .remoteAddress(), this);
        super.channelInactive(channelHandlerContext);
    }

    @Override
    public void channelRead(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {
        ByteBuf buffer = (ByteBuf) o;
        try {
            ByteBuffer nio = buffer.nioBuffer();
            if (nio.remaining() < 4) {
                log.error("Short read from channel {}", channelHandlerContext.channel());
                channelHandlerContext.close();
                return;
            }
            int correlationId = nio.getInt(0);
            PendingRequest pendingRequest = pendingRequestMap.remove(correlationId);
            if (pendingRequest != null) {
                pendingRequest.complete(responseContext.set(
                        channelHandlerContext.channel().remoteAddress(),
                        pendingRequest.getApiVersion(),
                        correlationId,
                        pendingRequest.parseResponse(nio)
                ));
            } else {
                log.error("Miss the inFlightRequest with correlationId {}.", correlationId);
            }
        } finally {
            ReferenceCountUtil.safeRelease(buffer);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable throwable) throws Exception {
        log.error("Transaction marker channel handler caught exception.", throwable);
        pendingRequestMap.forEach((correlationId, pendingRequest) -> {
                log.warn("Pending request ({}) failed because the txn marker channel caught exception",
                        pendingRequest, throwable);
                    pendingRequest.complete(responseContext.set(
                            channelHandlerContext.channel().remoteAddress(),
                            pendingRequest.getApiVersion(),
                            (int) correlationId,
                            pendingRequest.createErrorResponse(new NetworkException(throwable))));
        });
        pendingRequestMap.clear();
        channelHandlerContext.close();
    }

    public void close() {
        log.info("[TransactionMarkerChannelHandler] closing");
        this.cnxFuture.whenComplete((ctx, err) -> {
            if (ctx != null) {
                ctx.close();
            }
        });
    }

    public void handleAuthentication(ChannelHandlerContext channelHandlerContext) {
        if (!transactionMarkerChannelManager.getKafkaConfig().isAuthenticationEnabled()) {
            this.cnxFuture.complete(channelHandlerContext);
            return;
        }
        saslHandshake(channelHandlerContext)
                .thenCompose(this::authenticate)
                .thenApply(cnxFuture::complete)
                .exceptionally(err -> {
                    cnxFuture.completeExceptionally(err);
                    return false;
                });
    }

    private CompletableFuture<ChannelHandlerContext> saslHandshake(ChannelHandlerContext channel) {
        final PendingRequest pendingRequest = new PendingRequest(ApiKeys.SASL_HANDSHAKE,
                correlationIdGenerator.next(),
                newSaslHandshake(mechanism),
                __ -> {});
        enqueueRequest(channel, pendingRequest);
        return pendingRequest.getSendFuture().thenApply(__ -> {
            log.debug("SASL Handshake completed with success");
            return channel;
        }).exceptionally(error -> {
            log.error("SASL handshake failed", error);
            channel.close();
            return null;
        });
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
        CompletableFuture<ChannelHandlerContext> result = new CompletableFuture<>();

        Authentication authentication = transactionMarkerChannelManager.getAuthentication();

        try {
            byte[] saslAuthBytes;
            String commandData = authentication.getAuthData().getCommandData();

            switch (mechanism) {
                case PlainSaslServer.PLAIN_MECHANISM:
                    String prefix = "TX"; // the prefix TX means nothing, it is ignored by SaslUtils#parseSaslAuthBytes
                    String authUsername = transactionMarkerChannelManager.getAuthenticationUsername();
                    String authPassword = "token:" + commandData;
                    String usernamePassword = prefix
                            + "\u0000" + authUsername
                            + "\u0000" + authPassword;
                    saslAuthBytes = usernamePassword.getBytes(UTF_8);
                    break;
                case OAuthBearerLoginModule.OAUTHBEARER_MECHANISM:
                    saslAuthBytes = new OAuthBearerClientInitialResponse(commandData, null, null)
                            .toBytes();
                    break;
                default:
                    log.error("No corresponding mechanism to {}", authentication.getClass().getName());
                    saslAuthBytes = new byte[0];
                    break;
            }

            final PendingRequest pendingRequest = new PendingRequest(
                    ApiKeys.SASL_AUTHENTICATE,
                    correlationIdGenerator.next(),
                    newSaslAuthenticate(saslAuthBytes),
                    __ -> {}
            );
            enqueueRequest(channel, pendingRequest);
            pendingRequest.getSendFuture().whenComplete((response, e) -> {
                if (e != null) {
                    result.completeExceptionally(e);
                    return;
                }
                final SaslAuthenticateResponse saslResponse = (SaslAuthenticateResponse) response;
                if (saslResponse.error() == Errors.NONE) {
                    log.debug("Success step AUTH to KOP broker {} {} {}", saslResponse.error(),
                            saslResponse.errorMessage(), saslResponse.saslAuthBytes());
                    result.complete(channel);
                } else {
                    log.error("Failed authentication against KOP broker {}{}", saslResponse.error(),
                            saslResponse.errorMessage());
                    result.completeExceptionally(saslResponse.error().exception());
                }
            });
        } catch (PulsarClientException | SaslException ex) {
            log.error("Transaction marker channel handler authentication failed.", ex);
            result.completeExceptionally(ex);
        }
        return result;
    }

    private static SaslHandshakeRequest newSaslHandshake(final String mechanism) {
        return new SaslHandshakeRequest.Builder(new SaslHandshakeRequestData()
                .setMechanism(mechanism)).build();
    }

    private static SaslAuthenticateRequest newSaslAuthenticate(final byte[] saslAuthBytes) {
        return new SaslAuthenticateRequest.Builder(new SaslAuthenticateRequestData()
                .setAuthBytes(saslAuthBytes)).build();
    }

    private static WriteTxnMarkersRequest newWriteTxnMarkers(final List<TxnMarkerEntry> txnMarkerEntries) {
        return new WriteTxnMarkersRequest.Builder(ApiKeys.WRITE_TXN_MARKERS.latestVersion(), txnMarkerEntries).build();
    }
}
