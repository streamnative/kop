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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.io.Closeable;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
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

            return Unpooled.wrappedBuffer(serialized);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Get a buffer that contains the full frame
        ByteBuf buffer = (ByteBuf) msg;

        SocketAddress remoteAddress = ctx.channel().remoteAddress();
        try (KafkaHeaderAndRequest kafkaHeaderAndRequest = byteBufToRequest(buffer, remoteAddress)){
            if (log.isDebugEnabled()) {
                log.debug("[{}] Received kafka cmd {}",
                    ctx.channel() != null ? ctx.channel().remoteAddress() : "Null channel",
                    kafkaHeaderAndRequest.getHeader().apiKey().name);
            }

            switch (kafkaHeaderAndRequest.getHeader().apiKey()) {
                case API_VERSIONS:
                    handleApiVersionsRequest(kafkaHeaderAndRequest);
                    break;
                case METADATA:
                    handleTopicMetadataRequest(kafkaHeaderAndRequest);
                    break;
                case PRODUCE:
                    handleProduceRequest(kafkaHeaderAndRequest);
                    break;
                case FIND_COORDINATOR:
                    handleFindCoordinatorRequest(kafkaHeaderAndRequest);
                    break;
                case LIST_OFFSETS:
                    handleListOffsetRequest(kafkaHeaderAndRequest);
                    break;
                case OFFSET_FETCH:
                    handleOffsetFetchRequest(kafkaHeaderAndRequest);
                    break;
                case OFFSET_COMMIT:
                    handleOffsetCommitRequest(kafkaHeaderAndRequest);
                    break;
                case FETCH:
                    handleFetchRequest(kafkaHeaderAndRequest);
                    break;
                case JOIN_GROUP:
                    handleJoinGroupRequest(kafkaHeaderAndRequest);
                    break;
                case SYNC_GROUP:
                    handleSyncGroupRequest(kafkaHeaderAndRequest);
                    break;
                case HEARTBEAT:
                    handleHeartbeatRequest(kafkaHeaderAndRequest);
                    break;
                case LEAVE_GROUP:
                    handleLeaveGroupRequest(kafkaHeaderAndRequest);
                    break;
                case DESCRIBE_GROUPS:
                    handleDescribeGroupRequest(kafkaHeaderAndRequest);
                    break;
                case LIST_GROUPS:
                    handleListGroupsRequest(kafkaHeaderAndRequest);
                    break;
                case DELETE_GROUPS:
                    handleDeleteGroupsRequest(kafkaHeaderAndRequest);
                    break;
                default:
                    String err = String.format("Kafka API (%s) Not supported by kop server.",
                        kafkaHeaderAndRequest.getHeader().apiKey());
                    log.error(err);
                    handleError(err);
            }
        }
    }

    protected abstract void handleError(String error);

    protected abstract void handleApiVersionsRequest(KafkaHeaderAndRequest apiVersion);

    protected abstract void handleTopicMetadataRequest(KafkaHeaderAndRequest metadata);

    protected abstract void handleProduceRequest(KafkaHeaderAndRequest produce);

    protected abstract void handleFindCoordinatorRequest(KafkaHeaderAndRequest findCoordinator);

    protected abstract void handleListOffsetRequest(KafkaHeaderAndRequest listOffset);

    protected abstract void handleOffsetFetchRequest(KafkaHeaderAndRequest offsetFetch);

    protected abstract void handleOffsetCommitRequest(KafkaHeaderAndRequest offsetCommit);

    protected abstract void handleFetchRequest(KafkaHeaderAndRequest fetch);

    protected abstract void handleJoinGroupRequest(KafkaHeaderAndRequest joinGroup);

    protected abstract void handleSyncGroupRequest(KafkaHeaderAndRequest syncGroup);

    protected abstract void handleHeartbeatRequest(KafkaHeaderAndRequest heartbeat);

    protected abstract void handleLeaveGroupRequest(KafkaHeaderAndRequest leaveGroup);

    protected abstract void handleDescribeGroupRequest(KafkaHeaderAndRequest describeGroup);

    protected abstract void handleListGroupsRequest(KafkaHeaderAndRequest listGroups);

    protected abstract void handleDeleteGroupsRequest(KafkaHeaderAndRequest deleteGroups);

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
}
