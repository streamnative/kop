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

import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.testng.Assert.assertEquals;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

public class HttpRequestProcessorTest {
    private static class DummyHttpRequestProcessor extends HttpRequestProcessor {
        @Override
        protected int statusCodeFromThrowable(Throwable err) {
            return 404;
        }

        @Override
        protected String getErrorResponseContentType() {
            return "error";
        }

        @Override
        protected boolean acceptRequest(FullHttpRequest request) {
            return true;
        }

        @Override
        protected CompletableFuture<FullHttpResponse> processRequest(FullHttpRequest request) {
            return CompletableFuture.completedFuture(
                    new DefaultFullHttpResponse(HTTP_1_1, NO_CONTENT, Unpooled.EMPTY_BUFFER));
        }
    }

    @Test
    public void testBuildStringResponse() {
        String body = "body";
        String contentType = "type";
        FullHttpResponse response = HttpRequestProcessor.buildStringResponse(body, contentType);
        assertEquals(response.headers().get(HttpHeaderNames.CONTENT_TYPE), contentType);
        assertEquals(response.headers().get(HttpHeaderNames.CONTENT_LENGTH), String.valueOf(contentType.length()));
        assertEquals(response.content().toString(StandardCharsets.UTF_8), body);
    }

    @Test
    public void testBuildEmptyResponseNoContentResponse() {
        FullHttpResponse response = HttpRequestProcessor.buildEmptyResponseNoContentResponse();
        assertEquals(response.headers().get(HttpHeaderNames.CONTENT_LENGTH), "0");
        assertEquals(response.content().array().length, 0);
    }

    @Test
    public void testBuildErrorResponse() {
        HttpResponseStatus error = HttpResponseStatus.BAD_REQUEST;
        String body = "body";
        String contentType = "type";
        FullHttpResponse response = HttpRequestProcessor.buildErrorResponse(error, body, contentType);
        assertEquals(response.status(), error);
        assertEquals(response.headers().get(HttpHeaderNames.CONTENT_TYPE), contentType);
        assertEquals(response.headers().get(HttpHeaderNames.CONTENT_LENGTH), String.valueOf(contentType.length()));
        assertEquals(response.content().toString(StandardCharsets.UTF_8), body);
    }

    @Test
    public void testBuildJsonErrorResponse() {
        Throwable error = new RuntimeException("error");
        DummyHttpRequestProcessor processor = new DummyHttpRequestProcessor();
        FullHttpResponse response = processor.buildJsonErrorResponse(error);
        assertEquals(response.headers().get(HttpHeaderNames.CONTENT_TYPE), "error");
        assertEquals(response.content().toString(StandardCharsets.UTF_8), String.join("\n", """
                {
                  "message" : "error",
                  "error_code" : 404
                }"""));
    }

    @Test
    public void testAddCORSHeaders() {
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, NO_CONTENT, Unpooled.EMPTY_BUFFER);
        HttpRequestProcessor.addCORSHeaders(response);
        assertEquals(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS), "true");
        assertEquals(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN), "*");
        assertEquals(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS), "PUT, POST, GET, DELETE");
        assertEquals(response.headers().get(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS), "content-type");
    }

    @Test
    public void testBuildJsonResponse() {
        Pair<Integer, String> pair = new ImmutablePair<>(1, "a");
        String contentType = "application/json";
        FullHttpResponse response = HttpRequestProcessor.buildJsonResponse(pair, contentType);
        assertEquals(response.content().toString(StandardCharsets.UTF_8),
                String.join("\n", """
                        {
                          "1" : "a"
                        }"""));
        assertEquals(response.headers().get(HttpHeaderNames.CONTENT_TYPE), contentType);
    }
}
