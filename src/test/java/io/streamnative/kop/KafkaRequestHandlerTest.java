package io.streamnative.kop;


import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.streamnative.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.kop.KafkaCommandDecoder.KafkaHeaderAndResponse;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class KafkaRequestHandlerTest {

    private KafkaService kafkaService;

    private KafkaRequestHandler handler;

    @BeforeMethod
    public void setup() {
        kafkaService = new KafkaService(new KafkaServiceConfiguration());
        handler = new KafkaRequestHandler(kafkaService);
    }

    @Test
    public void testByteBufToRequest() {
        int correlationId = 7777;
        String clientId = "KopClientId";

        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest.Builder().build();
        RequestHeader header = new RequestHeader(
            ApiKeys.API_VERSIONS,
            ApiKeys.API_VERSIONS.latestVersion(),
            clientId,
            correlationId);

        KafkaHeaderAndRequest kopRequest = new KafkaHeaderAndRequest(
            header,
            apiVersionsRequest,
            Unpooled.buffer(20));

        // 1. serialize request into ByteBuf
        ByteBuffer serializedRequest = apiVersionsRequest.serialize(header);
        int size = serializedRequest.remaining();
        ByteBuf inputBuf = Unpooled.buffer(size);
        inputBuf.writeBytes(serializedRequest);

        // 2. turn Bytebuf into KafkaHeaderAndRequest.
        KafkaHeaderAndRequest request = handler.byteBufToRequest(inputBuf);

        // 3. verify byteBufToRequest works well.
        assertEquals(request.getHeader().toStruct(), header.toStruct());
        assertTrue(request.getRequest() instanceof ApiVersionsRequest);
    }


    @Test
    public void testResponseToByteBuf() throws Exception {
        int correlationId = 7777;
        String clientId = "KopClientId";

        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest.Builder().build();
        RequestHeader requestHeader = new RequestHeader(
            ApiKeys.API_VERSIONS,
            ApiKeys.API_VERSIONS.latestVersion(),
            clientId,
            correlationId);

        KafkaHeaderAndRequest kopRequest = new KafkaHeaderAndRequest(
            requestHeader,
            apiVersionsRequest,
            Unpooled.buffer(20));

        ApiVersionsResponse apiVersionsResponse = ApiVersionsResponse.defaultApiVersionsResponse();
        KafkaHeaderAndResponse kopResponse = KafkaHeaderAndResponse.responseForRequest(
            kopRequest, apiVersionsResponse);

        // 1. serialize response into ByteBuf
        ByteBuf serializedResponse = handler.responseToByteBuf(kopResponse.getResponse(), kopRequest);

        // 2. verify responseToByteBuf works well.
        ByteBuffer byteBuffer = serializedResponse.nioBuffer();
        ResponseHeader responseHeader = ResponseHeader.parse(byteBuffer);
        assertEquals(responseHeader.correlationId(), correlationId);

        ApiVersionsResponse parsedResponse = ApiVersionsResponse.parse(
            byteBuffer, kopResponse.getApiVersion());

        assertEquals(parsedResponse.apiVersions().size(), apiVersionsResponse.apiVersions().size());
    }

    @Test
    public void testChannelRead() throws Exception {
        int correlationId = 7777;
        String clientId = "KopClientId";

        ApiVersionsRequest apiVersionsRequest = new ApiVersionsRequest.Builder().build();
        RequestHeader header = new RequestHeader(
            ApiKeys.API_VERSIONS,
            ApiKeys.API_VERSIONS.latestVersion(),
            clientId,
            correlationId);

        ByteBuffer serializedRequest = apiVersionsRequest.serialize(header);
        int size = serializedRequest.remaining();
        ByteBuf inputBuf = Unpooled.buffer(size);
        inputBuf.writeBytes(serializedRequest);

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        SocketAddress address = mock(SocketAddress.class);

        when(ctx.channel()).thenReturn(channel);
        when(channel.remoteAddress()).thenReturn(address);

        handler = mock(KafkaRequestHandler.class, CALLS_REAL_METHODS);
        handler.channelActive(ctx);
        handler.channelRead(mock(ChannelHandlerContext.class), inputBuf);

        verify(handler, times(1)).handleApiVersionsRequest(anyObject());
    }

}
