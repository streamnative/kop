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
package io.streamnative.pulsar.handlers.kop.http;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import lombok.AllArgsConstructor;

/**
 * Abstract request processor for HTTP requests.
 */
public abstract class HttpRequestProcessor implements AutoCloseable {

    protected static final ObjectMapper MAPPER = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);

    /**
     * Build a FullHttpResponse using the given parameters.
     *
     * @param body        the response body
     * @param contentType the CONTENT_TYPE header
     * @return the response built
     */
    public static FullHttpResponse buildStringResponse(String body, String contentType) {
        FullHttpResponse httpResponse =
                new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length());
        addCORSHeaders(httpResponse);
        return httpResponse;
    }

    /**
     * Build an empty FullHttpResponse.
     *
     * @return the response built
     */
    public static FullHttpResponse buildEmptyResponseNoContentResponse() {
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, NO_CONTENT, Unpooled.EMPTY_BUFFER);
        httpResponse.headers().set(HttpHeaderNames.ALLOW, "GET, POST, PUT, DELETE");
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
        addCORSHeaders(httpResponse);
        return httpResponse;
    }

    /**
     * Build a FullHttpResponse with an error using the given parameters.
     *
     * @param error       the error
     * @param body        the response body
     * @param contentType the CONTENT_TYPE header
     * @return the response built
     */
    public static FullHttpResponse buildErrorResponse(HttpResponseStatus error, String body, String contentType) {
        FullHttpResponse httpResponse =
                new DefaultFullHttpResponse(HTTP_1_1, error, Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length());
        addCORSHeaders(httpResponse);
        return httpResponse;
    }

    protected abstract int statusCodeFromThrowable(Throwable err);

    protected abstract String getErrorResponseContentType();

    /**
     * Build a FullHttpResponse from an error. The response body is a serialized ErrorModel.
     *
     * @param throwable the error
     * @return the response built
     */
    public FullHttpResponse buildJsonErrorResponse(Throwable throwable) {
        Throwable err = throwable;
        while (err instanceof CompletionException) {
            err = err.getCause();
        }
        int httpStatusCode = statusCodeFromThrowable(err);
        HttpResponseStatus error = HttpResponseStatus.valueOf(httpStatusCode);

        FullHttpResponse httpResponse;
        String body;
        String contentType;
        try {
            body = MAPPER.writeValueAsString(new ErrorModel(httpStatusCode, err.getMessage()));
            contentType = getErrorResponseContentType();
        } catch (JsonProcessingException impossible) {
            body = "Error " + err;
            contentType = "text/plain";
        }
        httpResponse = new DefaultFullHttpResponse(HTTP_1_1, error, Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length());
        addCORSHeaders(httpResponse);
        return httpResponse;
    }

    /**
     * Add CORS headers to a given FullHttpResponse.
     *
     * @param httpResponse the FullHttpResponse
     */
    public static void addCORSHeaders(FullHttpResponse httpResponse) {
        httpResponse.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS, true);
        httpResponse.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
        httpResponse.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "PUT, POST, GET, DELETE");
        httpResponse.headers().set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "content-type");
    }

    protected abstract boolean acceptRequest(FullHttpRequest request);

    protected abstract CompletableFuture<FullHttpResponse> processRequest(FullHttpRequest request);

    @VisibleForTesting
    static FullHttpResponse buildJsonResponse(Object content, String contentType) {
        try {
            String body = MAPPER.writeValueAsString(content);
            return buildStringResponse(body, contentType);
        } catch (JsonProcessingException err) {
            return buildErrorResponse(INTERNAL_SERVER_ERROR, "Internal server error - JSON Processing", "text/plain");
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
