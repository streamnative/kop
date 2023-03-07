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
package io.streamnative.pulsar.handlers.kop;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.KopResponseUtils;
import org.apache.kafka.common.requests.ListOffsetRequestV0;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseCallbackWrapper;
import org.apache.kafka.common.requests.ResponseHeader;


/**
 * A decoder that decodes kafka requests and responses.
 */
@Slf4j
public abstract class KafkaCommandDecoder extends ChannelInboundHandlerAdapter {
    protected ChannelHandlerContext ctx;
    @Getter
    protected SocketAddress remoteAddress;
    @Getter
    protected AtomicBoolean isActive = new AtomicBoolean(false);
    // Queue to make response get responseFuture in order and limit the max request size
    private final LinkedBlockingQueue<ResponseAndRequest> requestQueue;
    @Getter
    @Setter
    protected volatile RequestStats requestStats;
    @Getter
    protected final KafkaServiceConfiguration kafkaConfig;

    private final OrderedScheduler sendResponseScheduler;

    public KafkaCommandDecoder(RequestStats requestStats,
                               KafkaServiceConfiguration kafkaConfig,
                               OrderedScheduler sendResponseScheduler) {
        this.requestStats = requestStats;
        this.kafkaConfig = kafkaConfig;
        this.requestQueue = new LinkedBlockingQueue<>(kafkaConfig.getMaxQueuedRequests());
        this.sendResponseScheduler = sendResponseScheduler;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.remoteAddress = ctx.channel().remoteAddress();
        this.ctx = ctx;
        isActive.set(true);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        // Handle idle connection closing
        if (evt instanceof IdleStateEvent) {
            if (log.isDebugEnabled()) {
                log.debug("About to close the idle connection from {} due to being idle for {} millis",
                        this.getRemoteAddress(), kafkaConfig.getConnectionMaxIdleMs());
            }
            this.close();
        } else {
            super.userEventTriggered(ctx, evt);
        }

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[{}] Got exception: {}", remoteAddress, cause.getMessage(), cause);
        close();
    }

    protected void close() {
        // Clear the request queue
        log.info("close channel {} with {} pending responses", ctx.channel(), requestQueue.size());
        while (true) {
            final ResponseAndRequest responseAndRequest = requestQueue.poll();
            if (responseAndRequest != null) {
                // Trigger writeAndFlushResponseToClient immediately, but it will do nothing because isActive is false
                responseAndRequest.cancel();
            } else {
                // queue is empty
                break;
            }

            // update request queue size stat
            RequestStats.REQUEST_QUEUE_SIZE_INSTANCE.decrementAndGet();
        }
        ctx.close();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Channel writability has changed to: {}", ctx.channel().isWritable());
        }
    }

    protected KafkaHeaderAndRequest byteBufToRequest(ByteBuf msg,
                                                     SocketAddress remoteAddress) {
        checkArgument(msg.readableBytes() > 0);
        ByteBuffer nio = msg.nioBuffer();
        RequestHeader header = RequestHeader.parse(nio);
        if (isUnsupportedApiVersionsRequest(header)) {
            ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest.Builder(header.apiVersion()).build();
            return new KafkaHeaderAndRequest(header, apiVersionsRequest, msg, remoteAddress);
        } else {
            ApiKeys apiKey = header.apiKey();
            short apiVersion = header.apiVersion();
            AbstractRequest body = AbstractRequest.parseRequest(apiKey, apiVersion, nio).request;
            return new KafkaHeaderAndRequest(header, body, msg, remoteAddress);
        }
    }

    protected ListOffsetRequestV0 byteBufToListOffsetRequestV0(ByteBuf buf) {
        checkArgument(buf.readableBytes() > 0);
        ByteBuffer nio = buf.nioBuffer();
        RequestHeader header = RequestHeader.parse(nio);
        short apiVersion = header.apiVersion();
        return ListOffsetRequestV0.parse(nio, apiVersion);
    }

    protected static ByteBuf responseToByteBuf(AbstractResponse response, KafkaHeaderAndRequest request) {
        try (KafkaHeaderAndResponse kafkaHeaderAndResponse =
                 KafkaHeaderAndResponse.responseForRequest(request, response)) {
            // Lowering Client API_VERSION request to the oldest API_VERSION KoP supports, this is to make \
            // Kafka-Clients 2.4.x and above compatible and prevent KoP from panicking \
            // when it comes across a higher API_VERSION.
            short apiVersion = kafkaHeaderAndResponse.getApiVersion();
            if (request.getHeader().apiKey() == API_VERSIONS){
                if (!ApiKeys.API_VERSIONS.isVersionSupported(apiVersion)) {
                    apiVersion = ApiKeys.API_VERSIONS.oldestVersion();
                }
            }
            return KopResponseUtils.serializeResponse(
                apiVersion,
                kafkaHeaderAndResponse.getHeader(),
                kafkaHeaderAndResponse.getResponse()
            );
        } finally {
            // the request is not needed any more.
            request.close();
        }
    }

    protected boolean channelReady() {
        return hasAuthenticated();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Get a buffer that contains the full frame
        ByteBuf buffer = (ByteBuf) msg;
        requestStats.getNetworkTotalBytesIn().add(buffer.readableBytes());

        // Update parse request latency metrics
        final BiConsumer<Long, Throwable> registerRequestParseLatency = (timeBeforeParse, throwable) -> {
            requestStats.getRequestParseLatencyStats().registerSuccessfulEvent(
                    MathUtils.elapsedNanos(timeBeforeParse), TimeUnit.NANOSECONDS);
        };

        // Update handle request latency metrics
        final BiConsumer<ApiKeys, Long> registerRequestLatency = (apiKey, startProcessTime) -> {
            requestStats.getRequestStatsLogger(apiKey, KopServerStats.REQUEST_LATENCY)
                    .registerSuccessfulEvent(MathUtils.elapsedNanos(startProcessTime), TimeUnit.NANOSECONDS);
        };

        // If kop is enabled for authentication and the client
        // has not completed the handshake authentication,
        // execute channelPrepare to complete authentication
        if (isActive.get() && !channelReady()) {
            try {
                channelPrepare(ctx, buffer, registerRequestParseLatency, registerRequestLatency);
                return;
            } catch (AuthenticationException e) {
                log.error("Failed authentication with [{}] ({})", this.remoteAddress, e.getMessage());
                maybeDelayCloseOnAuthenticationFailure();
                return;
            } finally {
                buffer.release();
            }
        }

        Channel channel = ctx.channel();
        SocketAddress remoteAddress = null;
        if (null != channel) {
            remoteAddress = channel.remoteAddress();
        }

        final long timeBeforeParse = MathUtils.nowInNano();
        KafkaHeaderAndRequest kafkaHeaderAndRequest = byteBufToRequest(buffer, remoteAddress);
        // potentially blocking until there is room in the queue for the request.
        registerRequestParseLatency.accept(timeBeforeParse, null);

        try {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received kafka cmd {}, the request content is: {}",
                    ctx.channel() != null ? ctx.channel().remoteAddress() : "Null channel",
                    kafkaHeaderAndRequest.getHeader(), kafkaHeaderAndRequest);
            }

            CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
            final long startProcessRequestTimestamp = MathUtils.nowInNano();
            responseFuture.whenComplete((response, e) -> {
                if (e instanceof CancellationException) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Request {} is cancelled",
                                ctx.channel(), kafkaHeaderAndRequest.getHeader());
                    }
                    // The response future is cancelled by `close` or `writeAndFlushResponseToClient` method, there's
                    // no need to call `writeAndFlushResponseToClient` again.
                    return;
                }

                registerRequestLatency.accept(kafkaHeaderAndRequest.getHeader().apiKey(),
                        startProcessRequestTimestamp);

                sendResponseScheduler.executeOrdered(channel.remoteAddress().hashCode(), () -> {
                    writeAndFlushResponseToClient(channel);
                });
            });
            // potentially blocking until there is room in the queue for the request.
            requestQueue.put(ResponseAndRequest.of(responseFuture, kafkaHeaderAndRequest));
            RequestStats.REQUEST_QUEUE_SIZE_INSTANCE.incrementAndGet();

            if (!isActive.get()) {
                handleInactive(kafkaHeaderAndRequest, responseFuture);
            } else {
                switch (kafkaHeaderAndRequest.getHeader().apiKey()) {
                    case API_VERSIONS:
                        handleApiVersionsRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case METADATA:
                        handleTopicMetadataRequest(kafkaHeaderAndRequest, responseFuture);
                        // this is special, wait Metadata command return, before execute other command?
                        // responseFuture.get();
                        break;
                    case PRODUCE:
                        handleProduceRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case FIND_COORDINATOR:
                        handleFindCoordinatorRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case LIST_OFFSETS:
                        handleListOffsetRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case OFFSET_FETCH:
                        handleOffsetFetchRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case OFFSET_COMMIT:
                        handleOffsetCommitRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case FETCH:
                        handleFetchRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case JOIN_GROUP:
                        handleJoinGroupRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case SYNC_GROUP:
                        handleSyncGroupRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case HEARTBEAT:
                        handleHeartbeatRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case LEAVE_GROUP:
                        handleLeaveGroupRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case DESCRIBE_GROUPS:
                        handleDescribeGroupRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case LIST_GROUPS:
                        handleListGroupsRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case DELETE_GROUPS:
                        handleDeleteGroupsRequest(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case SASL_HANDSHAKE:
                        handleSaslHandshake(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case SASL_AUTHENTICATE:
                        handleSaslAuthenticate(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case CREATE_TOPICS:
                        handleCreateTopics(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case INIT_PRODUCER_ID:
                        handleInitProducerId(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case ADD_PARTITIONS_TO_TXN:
                        handleAddPartitionsToTxn(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case ADD_OFFSETS_TO_TXN:
                        handleAddOffsetsToTxn(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case TXN_OFFSET_COMMIT:
                        handleTxnOffsetCommit(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case END_TXN:
                        handleEndTxn(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case WRITE_TXN_MARKERS:
                        handleWriteTxnMarkers(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case DESCRIBE_CONFIGS:
                        handleDescribeConfigs(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case ALTER_CONFIGS:
                        handleAlterConfigs(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case DELETE_TOPICS:
                        handleDeleteTopics(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case DELETE_RECORDS:
                        handleDeleteRecords(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case CREATE_PARTITIONS:
                        handleCreatePartitions(kafkaHeaderAndRequest, responseFuture);
                        break;
                    case DESCRIBE_CLUSTER:
                        handleDescribeCluster(kafkaHeaderAndRequest, responseFuture);
                        break;
                    default:
                        handleError(kafkaHeaderAndRequest, responseFuture);
                }
            }
        } catch (Exception e) {
            log.error("error while handle command:", e);
            close();
        } finally {
            // the kafkaHeaderAndRequest has already held the reference.
            buffer.release();
        }
    }

    // Write and flush continuously completed request back through channel.
    // This is to make sure request get responseFuture in the same order.
    protected void writeAndFlushResponseToClient(Channel channel) {
        // loop from first responseFuture.
        while (isActive.get()) {
            final ResponseAndRequest responseAndRequest = requestQueue.peek();
            if (responseAndRequest == null) {
                // requestQueue is empty
                break;
            }

            final CompletableFuture<AbstractResponse> responseFuture = responseAndRequest.getResponseFuture();
            final ApiKeys apiKey = responseAndRequest.getApiKey();
            final long nanoSecondsSinceCreated = responseAndRequest.nanoSecondsSinceCreated();
            final boolean expired =
                    (nanoSecondsSinceCreated > TimeUnit.MILLISECONDS.toNanos(kafkaConfig.getRequestTimeoutMs()));
            if (!responseFuture.isDone() && !expired) {
                // case 1: responseFuture is not completed or expired, stop polling responses from responseQueue
                requestStats.getResponseBlockedTimes().inc();
                long firstBlockTimestamp = responseAndRequest.getFirstBlockedTimestamp();
                if (firstBlockTimestamp == 0) {
                    responseAndRequest.setFirstBlockedTimestamp(MathUtils.nowInNano());
                }
                break;
            } else {
                if (requestQueue.remove(responseAndRequest)) {
                    RequestStats.REQUEST_QUEUE_SIZE_INSTANCE.decrementAndGet();
                } else { // it has been removed by another thread, skip this element
                    continue;
                }
            }

            if (responseAndRequest.getFirstBlockedTimestamp() != 0) {
                requestStats.getResponseBlockedLatency().registerSuccessfulEvent(
                        MathUtils.elapsedNanos(responseAndRequest.getFirstBlockedTimestamp()), TimeUnit.NANOSECONDS);
            }

            final KafkaHeaderAndRequest request = responseAndRequest.getRequest();

            // case 2: responseFuture is completed exceptionally
            if (responseFuture.isCompletedExceptionally()) {
                responseFuture.exceptionally(e -> {
                    log.error("[{}] request {} completed exceptionally", channel, request.getHeader(), e);
                    sendErrorResponse(request, channel, e);

                    requestStats.getRequestStatsLogger(apiKey, KopServerStats.REQUEST_QUEUED_LATENCY)
                            .registerFailedEvent(nanoSecondsSinceCreated, TimeUnit.NANOSECONDS);
                    return null;
                }); // send exception to client?
                continue;
            }

            // case 3: responseFuture is completed normally
            if (responseFuture.isDone()) {
                responseFuture.thenAccept(response -> {
                    if (response == null) {
                        // It should not be null, just check it for safety
                        log.error("[{}] Unexpected null completed future for request {}",
                                ctx.channel(), request.getHeader());
                        sendErrorResponse(request, channel, new ApiException("response is null"));
                        return;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Write kafka cmd to client."
                                        + " request content: {}"
                                        + " responseAndRequest content: {}",
                                request, response);
                    }

                    final ByteBuf result = responseToByteBuf(response, request);
                    final int resultSize = result.readableBytes();
                    channel.writeAndFlush(result).addListener(future -> {
                        if (response instanceof ResponseCallbackWrapper) {
                            ((ResponseCallbackWrapper) response).responseComplete();
                        }
                        if (!future.isSuccess()) {
                            log.error("[{}] Failed to write {}", channel, request.getHeader(), future.cause());
                        } else {
                            requestStats.getNetworkTotalBytesOut().add(resultSize);
                        }
                    });
                    requestStats.getRequestStatsLogger(apiKey, KopServerStats.REQUEST_QUEUED_LATENCY)
                            .registerSuccessfulEvent(nanoSecondsSinceCreated, TimeUnit.NANOSECONDS);
                });
                continue;
            }

            // case 4: responseFuture is expired
            if (expired) {
                log.error("[{}] request {} is not completed for {} ns (> {} ms)",
                        channel, request.getHeader(), nanoSecondsSinceCreated, kafkaConfig.getRequestTimeoutMs());
                responseFuture.cancel(true);
                sendErrorResponse(request, channel, new ApiException("request is expired from server side"));
                requestStats.getRequestStatsLogger(apiKey, KopServerStats.REQUEST_QUEUED_LATENCY)
                        .registerFailedEvent(nanoSecondsSinceCreated, TimeUnit.NANOSECONDS);
            }
        }
    }

    private void sendErrorResponse(KafkaHeaderAndRequest request, Channel channel, Throwable customError) {
        ByteBuf result = request.createErrorResponse(customError);
        final int resultSize = result.readableBytes();
        channel.writeAndFlush(result).addListener(future -> {
            if (future.isSuccess()) {
                requestStats.getNetworkTotalBytesOut().add(resultSize);
            }
        });
    }

    protected abstract boolean hasAuthenticated();

    protected abstract void channelPrepare(ChannelHandlerContext ctx,
                                           ByteBuf requestBuf,
                                           BiConsumer<Long, Throwable> registerRequestParseLatency,
                                           BiConsumer<ApiKeys, Long> registerRequestLatency)
            throws AuthenticationException;

    protected abstract void maybeDelayCloseOnAuthenticationFailure();

    protected abstract void completeCloseOnAuthenticationFailure();

    protected void handleError(KafkaHeaderAndRequest kafkaHeaderAndRequest,
                               CompletableFuture<AbstractResponse> resultFuture) {
        String err = String.format("Kafka API (%s) Not supported.",
                kafkaHeaderAndRequest.getHeader().apiKey());
        log.error(err);

        AbstractResponse apiResponse = kafkaHeaderAndRequest.getRequest()
                .getErrorResponse(new UnsupportedVersionException("API " + kafkaHeaderAndRequest.getHeader().apiKey()
                        + " is currently not supported"));
        resultFuture.complete(apiResponse);
    }

    protected abstract void
    handleInactive(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleApiVersionsRequest(KafkaHeaderAndRequest apiVersion, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleTopicMetadataRequest(KafkaHeaderAndRequest metadata, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleProduceRequest(KafkaHeaderAndRequest produce, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleFindCoordinatorRequest(KafkaHeaderAndRequest findCoordinator, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleListOffsetRequest(KafkaHeaderAndRequest listOffset, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleOffsetFetchRequest(KafkaHeaderAndRequest offsetFetch, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleOffsetCommitRequest(KafkaHeaderAndRequest offsetCommit, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleFetchRequest(KafkaHeaderAndRequest fetch, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleJoinGroupRequest(KafkaHeaderAndRequest joinGroup, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleSyncGroupRequest(KafkaHeaderAndRequest syncGroup, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleHeartbeatRequest(KafkaHeaderAndRequest heartbeat, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleLeaveGroupRequest(KafkaHeaderAndRequest leaveGroup, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleDescribeGroupRequest(KafkaHeaderAndRequest describeGroup, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleListGroupsRequest(KafkaHeaderAndRequest listGroups, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleDeleteGroupsRequest(KafkaHeaderAndRequest deleteGroups, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleSaslAuthenticate(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleSaslHandshake(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleCreateTopics(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleDescribeConfigs(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleAlterConfigs(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleInitProducerId(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleAddPartitionsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleAddOffsetsToTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleTxnOffsetCommit(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleEndTxn(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleWriteTxnMarkers(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleDeleteTopics(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleDeleteRecords(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleCreatePartitions(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

    protected abstract void
    handleDescribeCluster(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);


    public static class KafkaHeaderAndRequest {

        private static final String DEFAULT_CLIENT_HOST = "";

        private final RequestHeader header;
        private final AbstractRequest request;
        private final ByteBuf buffer;
        private final SocketAddress remoteAddress;

        public KafkaHeaderAndRequest(RequestHeader header,
                              AbstractRequest request,
                              ByteBuf buffer,
                              SocketAddress remoteAddress) {
            this.header = header;
            this.request = request;
            this.buffer = buffer.retain();
            this.remoteAddress = remoteAddress;
        }

        public ByteBuf getBuffer() {
            return buffer;
        }

        public RequestHeader getHeader() {
            return this.header;
        }

        public AbstractRequest getRequest() {
            return this.request;
        }

        public SocketAddress getRemoteAddress() {
            return this.remoteAddress;
        }

        public String getClientHost() {
            if (remoteAddress == null) {
                return DEFAULT_CLIENT_HOST;
            } else {
                return remoteAddress.toString();
            }
        }

        public ByteBuf createErrorResponse(Throwable e) {
            return responseToByteBuf(request.getErrorResponse(e), this);
        }

        @Override
        public String toString() {
            return String.format("KafkaHeaderAndRequest(header=%s, request=%s, remoteAddress=%s)",
                this.header, this.request, this.remoteAddress);
        }

        public void close() {
            ReferenceCountUtil.safeRelease(this.buffer);
        }
    }

    private static boolean isUnsupportedApiVersionsRequest(RequestHeader header) {
        return header.apiKey() == API_VERSIONS && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }

    public static class KafkaHeaderAndResponse implements Closeable {
        private final short apiVersion;
        private final ResponseHeader header;
        private final AbstractResponse response;

        private KafkaHeaderAndResponse(short apiVersion,
                                       ResponseHeader header,
                                       AbstractResponse response) {
            this.apiVersion = apiVersion;
            this.header = header;
            this.response = response;
        }

        public short getApiVersion() {
            return this.apiVersion;
        }

        public ResponseHeader getHeader() {
            return this.header;
        }

        public AbstractResponse getResponse() {
            return this.response;
        }

        static KafkaHeaderAndResponse responseForRequest(KafkaHeaderAndRequest request, AbstractResponse response) {
            return new KafkaHeaderAndResponse(
                request.getHeader().apiVersion(),
                request.getHeader().toResponseHeader(),
                response);
        }

        @Override
        public String toString() {
            return String.format("KafkaHeaderAndResponse(header=%s,responseFuture=%s)",
                this.header, this.response);
        }

        @Override
        public void close() {
        }
    }

    /**
     * A class that stores Kafka request and its related responseFuture.
     */
    static class ResponseAndRequest {
        @Getter
        private final CompletableFuture<AbstractResponse> responseFuture;
        @Getter
        private final KafkaHeaderAndRequest request;
        @Getter
        private final long createdTimestamp;

        @Getter
        @Setter
        private long firstBlockedTimestamp;

        public static ResponseAndRequest of(CompletableFuture<AbstractResponse> response,
                                            KafkaHeaderAndRequest request) {
            return new ResponseAndRequest(response, request);
        }

        public long nanoSecondsSinceCreated() {
            return MathUtils.elapsedNanos(createdTimestamp);
        }

        public ApiKeys getApiKey() {
            return request.getHeader().apiKey();
        }

        public boolean expired(final int requestTimeoutMs) {
            return MathUtils.elapsedNanos(createdTimestamp) > TimeUnit.MILLISECONDS.toNanos(requestTimeoutMs);
        }

        public void cancel() {
            responseFuture.cancel(true);
            request.close();
        }

        ResponseAndRequest(CompletableFuture<AbstractResponse> response, KafkaHeaderAndRequest request) {
            this.responseFuture = response;
            this.request = request;
            this.createdTimestamp = MathUtils.nowInNano();
            this.firstBlockedTimestamp = 0;
        }
    }
}
