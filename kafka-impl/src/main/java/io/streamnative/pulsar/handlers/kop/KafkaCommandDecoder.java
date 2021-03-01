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

import com.google.common.collect.Queues;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.naming.AuthenticationException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ResponseUtils;


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

    // Queue to make request get responseFuture in order.
    private Queue<ResponseAndRequest> requestsQueue;

    public KafkaCommandDecoder() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.remoteAddress = ctx.channel().remoteAddress();
        this.ctx = ctx;
        isActive.set(true);
        requestsQueue = Queues.newConcurrentLinkedQueue();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[{}] Got exception: {}", remoteAddress, cause.getMessage(), cause);
        close();
    }

    protected void close() {
        ctx.close();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Channel writability has changed to: {}", ctx.channel().isWritable());
        }
    }

    // turn input ByteBuf msg, which send from client side, into KafkaHeaderAndRequest
    protected KafkaHeaderAndRequest byteBufToRequest(ByteBuf msg) {
        return byteBufToRequest(msg, null);
    }

    protected KafkaHeaderAndRequest byteBufToRequest(ByteBuf msg,
                                                     SocketAddress remoteAddress) {
        checkArgument(msg.readableBytes() > 0);
        ByteBuffer nio = msg.nioBuffer();
        RequestHeader header = RequestHeader.parse(nio);
        if (isUnsupportedApiVersionsRequest(header)) {
            ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest((short) 0, header.apiVersion());
            return new KafkaHeaderAndRequest(header, apiVersionsRequest, msg, remoteAddress);
        } else {
            ApiKeys apiKey = header.apiKey();
            short apiVersion = header.apiVersion();
            Struct struct = apiKey.parseRequest(apiVersion, nio);
            AbstractRequest body = AbstractRequest.parseRequest(apiKey, apiVersion, struct);
            return new KafkaHeaderAndRequest(header, body, msg, remoteAddress);
        }
    }

    protected ByteBuf responseToByteBuf(AbstractResponse response, KafkaHeaderAndRequest request) {
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
            return ResponseUtils.serializeResponse(
                apiVersion,
                kafkaHeaderAndResponse.getHeader(),
                kafkaHeaderAndResponse.getResponse()
            );
        } finally {
            // the request is not needed any more.
            request.close();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Get a buffer that contains the full frame
        ByteBuf buffer = (ByteBuf) msg;

        Channel channel = ctx.channel();
        SocketAddress remoteAddress = null;
        if (null != channel) {
            remoteAddress = channel.remoteAddress();
        }

        KafkaHeaderAndRequest kafkaHeaderAndRequest = byteBufToRequest(buffer, remoteAddress);
        try {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received kafka cmd {}, the request content is: {}",
                    ctx.channel() != null ? ctx.channel().remoteAddress() : "Null channel",
                    kafkaHeaderAndRequest.getHeader(), kafkaHeaderAndRequest);
            }

            CompletableFuture<AbstractResponse> responseFuture = new CompletableFuture<>();
            responseFuture.whenComplete((response, e) -> {
                ctx.channel().eventLoop().execute(() -> {
                    writeAndFlushResponseToClient(channel);
                });
            });

            requestsQueue.add(ResponseAndRequest.of(responseFuture, kafkaHeaderAndRequest));

            if (!isActive.get()) {
                handleInactive(kafkaHeaderAndRequest, responseFuture);
            } else {
                if (!hasAuthenticated(kafkaHeaderAndRequest)) {
                    authenticate(kafkaHeaderAndRequest, responseFuture);
                    return;
                }
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
                    case DELETE_TOPICS:
                        handleDeleteTopics(kafkaHeaderAndRequest, responseFuture);
                        break;
                    default:
                        handleError(kafkaHeaderAndRequest, responseFuture);
                }
            }
        } catch (AuthenticationException e) {
            log.error("unexpected error in authenticate:", e);
            close();
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
        while (requestsQueue != null && requestsQueue.peek() != null
            && requestsQueue.peek().getResponseFuture().isDone() && isActive.get()) {
            ResponseAndRequest response = requestsQueue.remove();
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Write kafka cmd response back to client. \n"
                            + "\trequest content: {} \n"
                            + "\tresponse content: {}",
                        response.getRequest().toString(),
                        response.getResponseFuture().join().toString(response.getRequest().getRequest().version()));
                    log.debug("Write kafka cmd responseFuture back to client. request: {}",
                        response.getRequest().getHeader());
                }

                ByteBuf result = responseToByteBuf(response.getResponseFuture().get(), response.getRequest());
                channel.writeAndFlush(result);
            } catch (Exception e) {
                // should not comes here.
                log.error("error to get Response ByteBuf:", e);
            }
        }
    }

    // return all the current command before a channel close. return Error response for all pending request.
    protected void writeAndFlushWhenInactiveChannel(Channel channel) {
        // loop from first responseFuture, and return them all
        while (requestsQueue != null && requestsQueue.peek() != null) {
            try {
                ResponseAndRequest pair = requestsQueue.remove();

                if (log.isDebugEnabled()) {
                    log.debug("Channel Closing! Write kafka cmd responseFuture back to client. request: {}",
                        pair.getRequest().getHeader());
                }
                AbstractRequest request = pair.getRequest().getRequest();
                AbstractResponse apiResponse = request
                    .getErrorResponse(new LeaderNotAvailableException("Channel is closing!"));
                pair.getResponseFuture().complete(apiResponse);

                ByteBuf result = responseToByteBuf(pair.getResponseFuture().get(), pair.getRequest());
                channel.writeAndFlush(result);
            } catch (Exception e) {
                // should not comes here.
                log.error("error to get Response ByteBuf:", e);
            }
        }
    }

    protected abstract boolean hasAuthenticated(KafkaHeaderAndRequest kafkaHeaderAndRequest);

    protected abstract void
    authenticate(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response)
            throws AuthenticationException;

    protected abstract void
    handleError(KafkaHeaderAndRequest kafkaHeaderAndRequest, CompletableFuture<AbstractResponse> response);

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

    static class KafkaHeaderAndRequest implements Closeable {

        private static final String DEFAULT_CLIENT_HOST = "";

        private final RequestHeader header;
        private final AbstractRequest request;
        private final ByteBuf buffer;
        private final SocketAddress remoteAddress;

        KafkaHeaderAndRequest(RequestHeader header,
                              AbstractRequest request,
                              ByteBuf buffer,
                              SocketAddress remoteAddress) {
            this.header = header;
            this.request = request;
            this.buffer = buffer.retain();
            this.remoteAddress = remoteAddress;
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

        ByteBuf getBuffer() {
            return this.buffer;
        }

        public String toString() {
            return String.format("KafkaHeaderAndRequest(header=%s, request=%s, remoteAddress=%s)",
                this.header, this.request, this.remoteAddress);
        }

        @Override
        public void close() {
            this.buffer.release();
        }
    }

    private static boolean isUnsupportedApiVersionsRequest(RequestHeader header) {
        return header.apiKey() == API_VERSIONS && !API_VERSIONS.isVersionSupported(header.apiVersion());
    }

    static class KafkaHeaderAndResponse implements Closeable {
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

        public String toString() {
            return String.format("KafkaHeaderAndResponse(header=%s,responseFuture=%s)",
                this.header.toStruct().toString(), this.response.toString(this.getApiVersion()));
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
        private CompletableFuture<AbstractResponse> responseFuture;
        @Getter
        private KafkaHeaderAndRequest request;

        public static ResponseAndRequest of(CompletableFuture<AbstractResponse> response,
                                            KafkaHeaderAndRequest request) {
            return new ResponseAndRequest(response, request);
        }

        ResponseAndRequest(CompletableFuture<AbstractResponse> response, KafkaHeaderAndRequest request) {
            this.responseFuture = response;
            this.request = request;
        }
    }
}
