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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl.SchemaStorageException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.AllArgsConstructor;

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

    public static FullHttpResponse buildErrorResponse(HttpResponseStatus error, String body, String contentType) {
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, error,
                Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length());
        addCORSHeaders(httpResponse);
        return httpResponse;
    }

    public static FullHttpResponse buildJsonErrorResponse(Throwable err) {
        while (err instanceof CompletionException) {
            err = err.getCause();
        }
        int httpStatusCode = err instanceof SchemaStorageException
                ? ((SchemaStorageException) err).getHttpStatusCode()
                : INTERNAL_SERVER_ERROR.code();
        HttpResponseStatus error = HttpResponseStatus.valueOf(httpStatusCode);

        FullHttpResponse httpResponse = null;
        try {
            String body = MAPPER.writeValueAsString(new ErrorModel(httpStatusCode, err.getMessage()));
            httpResponse = new DefaultFullHttpResponse(HTTP_1_1, error,
                    Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
            httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/vnd.schemaregistry.v1+json");
            httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length());
            addCORSHeaders(httpResponse);
        } catch (JsonProcessingException impossible) {
            String body = "Error " + err;
            httpResponse = new DefaultFullHttpResponse(HTTP_1_1, error,
                    Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
            httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
            httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length());
            addCORSHeaders(httpResponse);
        }
        return httpResponse;
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
                    "Internal server error - JSON Processing", "text/plain");
        }
    }

    @Override
    public void close() {
        // nothing
    }

    @AllArgsConstructor
    private static final class ErrorModel {
        // https://docs.confluent.io/platform/current/schema-registry/develop/api.html#schemas

        final int errorCode;
        final String message;

        @JsonProperty("error_code")
        public int getErrorCode() {
            return errorCode;
        }

        public String getMessage() {
            return message;
        }
    }
}
