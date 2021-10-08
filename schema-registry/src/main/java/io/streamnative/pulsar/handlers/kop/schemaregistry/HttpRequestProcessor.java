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
import lombok.AllArgsConstructor;

public abstract class HttpRequestProcessor {

    protected static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(SerializationFeature.INDENT_OUTPUT, true);

    abstract FullHttpResponse processRequest(FullHttpRequest request);

    protected FullHttpResponse buildStringResponse(String body, String contentType) {
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1,  OK,
                Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length());
        return httpResponse;
    }

    protected FullHttpResponse buildErrorResponse(HttpResponseStatus error, String body, String contentType) {
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1,  error,
                Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
        httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length());
        return httpResponse;
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

    protected FullHttpResponse buildJsonErrorResponse(SchemaStorageException err) {
        HttpResponseStatus error = HttpResponseStatus.valueOf(err.getHttpStatusCode());

        FullHttpResponse httpResponse = null;
        try {
            String body = MAPPER.writeValueAsString(new ErrorModel(err.getHttpStatusCode(), err.getMessage()));
            httpResponse = new DefaultFullHttpResponse(HTTP_1_1,  error,
                    Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
            httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/vnd.schemaregistry.v1+json");
            httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length());
        } catch (JsonProcessingException impossible) {
            String body = "Error " + err;
            httpResponse = new DefaultFullHttpResponse(HTTP_1_1,  error,
                    Unpooled.copiedBuffer(body, CharsetUtil.UTF_8));
            httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
            httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, body.length());
        }
        return httpResponse;
    }

    protected FullHttpResponse buildJsonResponse(Object content, String contentType) {
        try {
            String body = MAPPER.writeValueAsString(content);
            return buildStringResponse(body, contentType);
        } catch (JsonProcessingException err) {
            return buildErrorResponse(INTERNAL_SERVER_ERROR,
                    "Internal server error - JSON Processing", "text/plain");
        }
    }


}
