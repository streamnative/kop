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
package io.streamnative.kop;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.METADATA;

import com.google.common.collect.Queues;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
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


/**
 * A decoder that decodes kafka requests and responses.
 */
@Slf4j
public abstract class KafkaCommandDecoder extends ChannelInboundHandlerAdapter {
    protected ChannelHandlerContext ctx;
    protected SocketAddress remoteAddress;
    @Getter
    protected AtomicBoolean isActive = new AtomicBoolean(false);

    // Queue to make request get responseFuture in order.
    protected final ConcurrentHashMap<Channel, Queue<ResponseAndRequest>> responsesQueue =
        new ConcurrentHashMap();

    public KafkaCommandDecoder() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.remoteAddress = ctx.channel().remoteAddress();
        this.ctx = ctx;
        isActive.set(true);
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

            ByteBuffer serialized = kafkaHeaderAndResponse
                .getResponse()
                .serialize(kafkaHeaderAndResponse.getApiVersion(), kafkaHeaderAndResponse.getHeader());

            // Already converted the ByteBuf into ByteBuffer now, release ByteBuf
            kafkaHeaderAndResponse.buffer.release();
            return Unpooled.wrappedBuffer(serialized);
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

        CompletableFuture<AbstractResponse> responseFuture;

        try (KafkaHeaderAndRequest kafkaHeaderAndRequest = byteBufToRequest(buffer, remoteAddress)){
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received kafka cmd {}, the request content is: {}",
                    ctx.channel() != null ? ctx.channel().remoteAddress() : "Null channel",
                    kafkaHeaderAndRequest.getHeader(), kafkaHeaderAndRequest);
            }

            if (!isActive.get()) {
                responseFuture = handleInactive(kafkaHeaderAndRequest);
            } else {
                switch (kafkaHeaderAndRequest.getHeader().apiKey()) {
                    case API_VERSIONS:
                        responseFuture = handleApiVersionsRequest(kafkaHeaderAndRequest);
                        break;
                    case METADATA:
                        responseFuture = handleTopicMetadataRequest(kafkaHeaderAndRequest);
                        // this is special, wait Metadata command return, before execute other command?
                        // responseFuture.get();
                        break;
                    case PRODUCE:
                        responseFuture = handleProduceRequest(kafkaHeaderAndRequest);
                        break;
                    case FIND_COORDINATOR:
                        responseFuture = handleFindCoordinatorRequest(kafkaHeaderAndRequest);
                        break;
                    case LIST_OFFSETS:
                        responseFuture = handleListOffsetRequest(kafkaHeaderAndRequest);
                        break;
                    case OFFSET_FETCH:
                        responseFuture = handleOffsetFetchRequest(kafkaHeaderAndRequest);
                        break;
                    case OFFSET_COMMIT:
                        responseFuture = handleOffsetCommitRequest(kafkaHeaderAndRequest);
                        break;
                    case FETCH:
                        responseFuture = handleFetchRequest(kafkaHeaderAndRequest);
                        break;
                    case JOIN_GROUP:
                        responseFuture = handleJoinGroupRequest(kafkaHeaderAndRequest);
                        break;
                    case SYNC_GROUP:
                        responseFuture = handleSyncGroupRequest(kafkaHeaderAndRequest);
                        break;
                    case HEARTBEAT:
                        responseFuture = handleHeartbeatRequest(kafkaHeaderAndRequest);
                        break;
                    case LEAVE_GROUP:
                        responseFuture = handleLeaveGroupRequest(kafkaHeaderAndRequest);
                        break;
                    case DESCRIBE_GROUPS:
                        responseFuture = handleDescribeGroupRequest(kafkaHeaderAndRequest);
                        break;
                    case LIST_GROUPS:
                        responseFuture = handleListGroupsRequest(kafkaHeaderAndRequest);
                        break;
                    case DELETE_GROUPS:
                        responseFuture = handleDeleteGroupsRequest(kafkaHeaderAndRequest);
                        break;
                    case SASL_HANDSHAKE:
                        responseFuture = handleSaslHandshake(kafkaHeaderAndRequest);
                        break;
                    case SASL_AUTHENTICATE:
                        responseFuture = handleSaslAuthenticate(kafkaHeaderAndRequest);
                        break;
                    default:
                        responseFuture = handleError(kafkaHeaderAndRequest);
                }
            }

            responsesQueue.compute(channel, (key, queue) -> {
                if (queue == null) {
                    Queue<ResponseAndRequest> newQueue = Queues.newConcurrentLinkedQueue();
                    newQueue.add(ResponseAndRequest.of(responseFuture, kafkaHeaderAndRequest));
                    return newQueue;
                } else {
                    queue.add(ResponseAndRequest.of(responseFuture, kafkaHeaderAndRequest));
                    return queue;
                }
            });

            responseFuture.whenComplete((response, e) -> {
                writeAndFlushResponseToClient(channel);
            });
        } catch (Exception e) {
            log.error("error while handle command:", e);
            close();
        }
    }

    // Write and flush continuously completed request back through channel.
    // This is to make sure request get responseFuture in the same order.
    protected void writeAndFlushResponseToClient(Channel channel) {
        Queue<ResponseAndRequest> responseQueue =
            responsesQueue.get(channel);

        // loop from first responseFuture.
        while (responseQueue != null && responseQueue.peek() != null
            && responseQueue.peek().getResponseFuture().isDone()) {
            ResponseAndRequest response = responseQueue.remove();
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
        Queue<ResponseAndRequest> responseQueue =
            responsesQueue.get(channel);

        // loop from first responseFuture, and return them all
        while (responseQueue != null && responseQueue.peek() != null) {
            try {
                ResponseAndRequest pair = responseQueue.remove();

                if (log.isDebugEnabled()) {
                    log.debug("Write kafka cmd responseFuture back to client. request: {}",
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

    protected abstract CompletableFuture<AbstractResponse>
    handleError(KafkaHeaderAndRequest kafkaHeaderAndRequest);

    protected abstract CompletableFuture<AbstractResponse>
    handleInactive(KafkaHeaderAndRequest kafkaHeaderAndRequest);

    protected abstract CompletableFuture<AbstractResponse>
    handleApiVersionsRequest(KafkaHeaderAndRequest apiVersion);

    protected abstract CompletableFuture<AbstractResponse>
    handleTopicMetadataRequest(KafkaHeaderAndRequest metadata);

    protected abstract CompletableFuture<AbstractResponse>
    handleProduceRequest(KafkaHeaderAndRequest produce);

    protected abstract CompletableFuture<AbstractResponse>
    handleFindCoordinatorRequest(KafkaHeaderAndRequest findCoordinator);

    protected abstract CompletableFuture<AbstractResponse>
    handleListOffsetRequest(KafkaHeaderAndRequest listOffset);

    protected abstract CompletableFuture<AbstractResponse>
    handleOffsetFetchRequest(KafkaHeaderAndRequest offsetFetch);

    protected abstract CompletableFuture<AbstractResponse>
    handleOffsetCommitRequest(KafkaHeaderAndRequest offsetCommit);

    protected abstract CompletableFuture<AbstractResponse>
    handleFetchRequest(KafkaHeaderAndRequest fetch);

    protected abstract CompletableFuture<AbstractResponse>
    handleJoinGroupRequest(KafkaHeaderAndRequest joinGroup);

    protected abstract CompletableFuture<AbstractResponse>
    handleSyncGroupRequest(KafkaHeaderAndRequest syncGroup);

    protected abstract CompletableFuture<AbstractResponse>
    handleHeartbeatRequest(KafkaHeaderAndRequest heartbeat);

    protected abstract CompletableFuture<AbstractResponse>
    handleLeaveGroupRequest(KafkaHeaderAndRequest leaveGroup);

    protected abstract CompletableFuture<AbstractResponse>
    handleDescribeGroupRequest(KafkaHeaderAndRequest describeGroup);

    protected abstract CompletableFuture<AbstractResponse>
    handleListGroupsRequest(KafkaHeaderAndRequest listGroups);

    protected abstract CompletableFuture<AbstractResponse>
    handleDeleteGroupsRequest(KafkaHeaderAndRequest deleteGroups);

    protected abstract CompletableFuture<AbstractResponse>
    handleSaslAuthenticate(KafkaHeaderAndRequest kafkaHeaderAndRequest);

    protected abstract CompletableFuture<AbstractResponse>
    handleSaslHandshake(KafkaHeaderAndRequest kafkaHeaderAndRequest);

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
        private final ByteBuf buffer;

        private KafkaHeaderAndResponse(short apiVersion,
                                       ResponseHeader header,
                                       AbstractResponse response,
                                       ByteBuf buffer) {
            this.apiVersion = apiVersion;
            this.header = header;
            this.response = response;
            this.buffer = buffer.retain();
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
                response,
                request.getBuffer());
        }

        public String toString() {
            return String.format("KafkaHeaderAndResponse(header=%s,responseFuture=%s)",
                this.header.toStruct().toString(), this.response.toString(this.getApiVersion()));
        }

        @Override
        public void close() {
            this.buffer.release();
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
