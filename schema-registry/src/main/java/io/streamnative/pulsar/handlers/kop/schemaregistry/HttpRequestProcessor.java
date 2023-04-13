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
package io.streamnative.pulsar.handlers.kop.schemaregistry;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl.SchemaStorageException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class HttpRequestProcessor implements AutoCloseable {

    protected static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(SerializationFeature.INDENT_OUTPUT, true);

    public static FullHttpResponse buildStringResponse(String body, String contentType) {
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, OK,
                Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length());
        addCORSHeaders(httpResponse);
        return httpResponse;
    }

    public static FullHttpResponse buildEmptyResponseNoContentResponse() {
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, NO_CONTENT, Unpooled.EMPTY_BUFFER);
        httpResponse.headers().set(HttpHeaderNames.ALLOW, "GET, POST, PUT, DELETE");
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
        addCORSHeaders(httpResponse);
        return httpResponse;
    }

    public static FullHttpResponse buildErrorResponse(HttpResponseStatus error, String message) {
        byte[] bytes = null;
        try {
            bytes = MAPPER.writeValueAsBytes(new ErrorMessage(error.code(), message));
        } catch (JsonProcessingException e) {
            log.error("Failed to write ErrorMessage({}, {})", error.code(), message, e);
        }
        final ByteBuf buf = Unpooled.wrappedBuffer((bytes != null) ? bytes : "{}".getBytes(StandardCharsets.UTF_8));

        FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, error, buf);
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/schemaregistry.v1+json");
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, buf.readableBytes());
        addCORSHeaders(httpResponse);
        return httpResponse;
    }

    public static FullHttpResponse buildJsonErrorResponse(Throwable throwable) {
        Throwable err = throwable;
        while (err instanceof CompletionException) {
            err = err.getCause();
        }
        HttpResponseStatus httpStatusCode = err instanceof SchemaStorageException
                ? ((SchemaStorageException) err).getHttpStatusCode()
                : INTERNAL_SERVER_ERROR;
        return buildErrorResponse(httpStatusCode, err.getMessage());
    }

    public static void addCORSHeaders(FullHttpResponse httpResponse) {
        httpResponse.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS, true);
        httpResponse.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        httpResponse.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "PUT, POST, GET, DELETE");
        httpResponse.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "content-type");
    }

    protected abstract boolean acceptRequest(FullHttpRequest request);

    protected abstract CompletableFuture<FullHttpResponse> processRequest(FullHttpRequest request);

    protected FullHttpResponse buildJsonResponse(Object content, String contentType) {
        try {
            String body = MAPPER.writeValueAsString(content);
            return buildStringResponse(body, contentType);
        } catch (JsonProcessingException err) {
            return buildErrorResponse(INTERNAL_SERVER_ERROR,
                    "Build JSON response failed: " + err.getMessage());
        }
    }

    @Override
    public void close() {
        // nothing
    }
}
