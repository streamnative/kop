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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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

    // Queue to make request get response in order.
    protected final ConcurrentHashMap<Channel, Queue<CompletableFuture<ResponseAndRequest>>> responsesQueue =
        new ConcurrentHashMap();

    // TODO: do we need keep alive? if need, messageReceived() before every command is need?

    public KafkaCommandDecoder() {
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.remoteAddress = ctx.channel().remoteAddress();
        this.ctx = ctx;

        if (log.isDebugEnabled()) {
            log.debug("[{}] channel active {}", ctx.channel());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[{}] Got exception: {}", remoteAddress, cause.getMessage(), cause);
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

        CompletableFuture<ResponseAndRequest> responseFuture;


        try (KafkaHeaderAndRequest kafkaHeaderAndRequest = byteBufToRequest(buffer, remoteAddress)){
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received kafka cmd {}",
                    ctx.channel() != null ? ctx.channel().remoteAddress() : "Null channel",
                    kafkaHeaderAndRequest.getHeader());
            }

            switch (kafkaHeaderAndRequest.getHeader().apiKey()) {
                case API_VERSIONS:
                    responseFuture = handleApiVersionsRequest(kafkaHeaderAndRequest);
                    break;
                case METADATA:
                    responseFuture = handleTopicMetadataRequest(kafkaHeaderAndRequest);
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

            responsesQueue.compute(channel, (key, queue) -> {
                if (queue == null) {
                    Queue<CompletableFuture<ResponseAndRequest>> newQueue = Queues.newConcurrentLinkedQueue();
                    newQueue.add(responseFuture);
                    return newQueue;
                } else {
                    queue.add(responseFuture);
                    return queue;
                }
            });

            responseFuture.whenComplete((response, e) -> {
                writeAndFlushResponseToClient(channel);
            });
        }
    }

    // Write and flush continuously completed request back through channel.
    // This is to make sure request get response in the same order.
    protected void writeAndFlushResponseToClient(Channel channel) {
        Queue<CompletableFuture<ResponseAndRequest>> responseQueue =
            responsesQueue.get(channel);

        // loop from first response.
        while (responseQueue != null && responseQueue.peek() != null && responseQueue.peek().isDone()) {
            CompletableFuture<ResponseAndRequest> response = responseQueue.remove();
            try {
                ResponseAndRequest pair = response.join();
                if (log.isDebugEnabled()) {
                    log.debug("Write kafka cmd response back to client. request: {}",
                        pair.getRequest().getHeader());
                }

                ByteBuf result = responseToByteBuf(pair.getResponse(), pair.getRequest());
                channel.writeAndFlush(result);
            } catch (Exception e) {
                // should not comes here.
                log.error("error to get Response ByteBuf:", e);
                throw e;
            }
        }
    }

    protected abstract CompletableFuture<ResponseAndRequest>
    handleError(KafkaHeaderAndRequest kafkaHeaderAndRequest);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleApiVersionsRequest(KafkaHeaderAndRequest apiVersion);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleTopicMetadataRequest(KafkaHeaderAndRequest metadata);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleProduceRequest(KafkaHeaderAndRequest produce);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleFindCoordinatorRequest(KafkaHeaderAndRequest findCoordinator);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleListOffsetRequest(KafkaHeaderAndRequest listOffset);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleOffsetFetchRequest(KafkaHeaderAndRequest offsetFetch);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleOffsetCommitRequest(KafkaHeaderAndRequest offsetCommit);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleFetchRequest(KafkaHeaderAndRequest fetch);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleJoinGroupRequest(KafkaHeaderAndRequest joinGroup);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleSyncGroupRequest(KafkaHeaderAndRequest syncGroup);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleHeartbeatRequest(KafkaHeaderAndRequest heartbeat);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleLeaveGroupRequest(KafkaHeaderAndRequest leaveGroup);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleDescribeGroupRequest(KafkaHeaderAndRequest describeGroup);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleListGroupsRequest(KafkaHeaderAndRequest listGroups);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleDeleteGroupsRequest(KafkaHeaderAndRequest deleteGroups);

    protected abstract CompletableFuture<ResponseAndRequest>
    handleSaslAuthenticate(KafkaHeaderAndRequest kafkaHeaderAndRequest);

    protected abstract CompletableFuture<ResponseAndRequest>
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
            return String.format("KafkaHeaderAndResponse(header=%s,response=%s)",
                this.header.toStruct().toString(), this.response.toString(this.getApiVersion()));
        }

        @Override
        public void close() {
            this.buffer.release();
        }
    }

    /**
     * A class that stores Kafka request and its related response.
     */
    static class ResponseAndRequest {
        @Getter
        private AbstractResponse response;
        @Getter
        private KafkaHeaderAndRequest request;

        public static ResponseAndRequest of(AbstractResponse response, KafkaHeaderAndRequest request) {
            return new ResponseAndRequest(response, request);
        }

        ResponseAndRequest(AbstractResponse response, KafkaHeaderAndRequest request) {
            this.response = response;
            this.request = request;
        }
    }
}
