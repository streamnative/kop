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

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Abstract HttpRequestProcessor for JSON requests and responses handling.
 * @param <K> request type
 * @param <R> response type
 */
@Slf4j
public abstract class HttpJsonRequestProcessor<K, R> extends HttpRequestProcessor {

    protected static final String RESPONSE_CONTENT_TYPE = "application/json";

    private final Class<K> requestModel;
    private final Pattern pattern;
    private final String method;

    public HttpJsonRequestProcessor(Class<K> requestModel, String uriPattern, String method) {
        this.requestModel = requestModel;
        this.pattern = Pattern.compile(uriPattern);
        this.method = method;
    }

    protected static int getInt(int position, List<String> queryStringGroups) {
        return Integer.parseInt(queryStringGroups.get(position));
    }

    protected static String getString(int position, List<String> queryStringGroups) {
        return queryStringGroups.get(position);
    }

    @Override
    public boolean acceptRequest(FullHttpRequest request) {
        if (!request.method().name().equals(method)) {
            return false;
        }
        return detectGroups(request) != null;
    }

    @Override
    public CompletableFuture<FullHttpResponse> processRequest(FullHttpRequest request) {
        List<String> groups = detectGroups(request);
        K decodeRequest;
        try (ByteBufInputStream inputStream = new ByteBufInputStream(request.content())) {
            if (requestModel == Void.class) {
                decodeRequest = null;
            } else {
                decodeRequest = MAPPER.readValue((DataInput) inputStream, requestModel);
            }
        } catch (IOException err) {
            log.error("Cannot decode request", err);
            return CompletableFuture.completedFuture(buildErrorResponse(HttpResponseStatus.BAD_REQUEST,
                    "Cannot decode request: " + err.getMessage(), "text/plain"));
        }

        CompletableFuture<FullHttpResponse> result;
        try {
            result = processRequest(decodeRequest, groups, request).thenApply(resp -> {
                if (resp == null) {
                    return buildErrorResponse(NOT_FOUND, "Not found", "text/plain");
                }
                if (resp.getClass() == String.class) {
                    return buildStringResponse(((String) resp), RESPONSE_CONTENT_TYPE);
                } else {
                    return buildJsonResponse(resp, RESPONSE_CONTENT_TYPE);
                }
            });
        } catch (Exception err) {
            result = FutureUtil.failedFuture(err);
        }
        return result.exceptionally(err -> {
            log.error("Error while processing request", err);
            return buildJsonErrorResponse(err);
        });
    }

    private List<String> detectGroups(FullHttpRequest request) {
        String uri = request.uri();
        // TODO: here we are discarding the query string part
        // in the future we will probably have to implement
        // query string parameters
        int questionMark = uri.lastIndexOf('?');
        if (questionMark > 0) {
            uri = uri.substring(0, questionMark);
        }
        if (uri.endsWith("/")) {
            // compatibility with Confluent Schema registry
            uri = uri.substring(0, uri.length() - 1);
        }
        Matcher matcher = pattern.matcher(uri);
        if (!matcher.matches()) {
            return null;
        }
        List<String> groups = new ArrayList<>(matcher.groupCount());
        for (int i = 0; i < matcher.groupCount(); i++) {
            groups.add(matcher.group(i + 1));
        }
        return groups;
    }

    protected abstract CompletableFuture<R> processRequest(K payload, List<String> patternGroups,
                                                           FullHttpRequest request) throws Exception;
}
