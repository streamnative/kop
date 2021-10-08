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
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl.SchemaStorageException;
import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class HttpJsonRequestProcessor <K, R> extends HttpRequestProcessor {

    protected static final String RESPONSE_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";

    private final Class<K> requestModel;
    private final Pattern pattern;
    private final String method;

    public HttpJsonRequestProcessor(Class<K> requestModel, String uriPattern, String method) {
        this.requestModel = requestModel;
        this.pattern = Pattern.compile(uriPattern);
        this.method = method;
    }

    @Override
    FullHttpResponse processRequest(FullHttpRequest request) {
        if (!request.method().name().equals(method)) {
            return null;
        }
        List<String> groups = detectGroups(request);
        if (groups == null) {
            return null;
        }

        try (ByteBufInputStream  inputStream = new ByteBufInputStream(request.content());) {
            K decodeRequest;
            if (requestModel == Void.class) {
                decodeRequest = null;
            } else {
                decodeRequest = MAPPER.readValue((DataInput) inputStream, requestModel);
            }

            R result = null;
            try {
                result = processRequest(decodeRequest, groups, request);
            } catch (SchemaStorageException err) {
                log.error("Error while processing request", err);
                return buildJsonErrorResponse(err);
            } catch (Exception err) {
                log.error("Error while processing request", err);
                return buildErrorResponse(INTERNAL_SERVER_ERROR,
                        "Error while processing request", "text/plain");
            }
            if (result == null) {
                return buildErrorResponse(NOT_FOUND, "Not found", "text/plain");
            }
            if (result.getClass() == String.class) {
                return buildStringResponse(((String) result), RESPONSE_CONTENT_TYPE);
            } else {
                return buildJsonResponse(result, RESPONSE_CONTENT_TYPE);
            }
        } catch (IOException err) {
            log.error("Error while processing request", err);
            return buildErrorResponse(HttpResponseStatus.BAD_REQUEST,
                    "Cannot decode request: " + err.getMessage(), "text/plain");
        }
    }

    private List<String> detectGroups(FullHttpRequest request) {
        Matcher matcher = pattern.matcher(request.uri());
        if (!matcher.matches()) {
            return null;
        }
        List<String> groups = new ArrayList<>(matcher.groupCount());
        for (int i = 0; i < matcher.groupCount(); i++) {
            groups.add(matcher.group(i + 1));
        }
        return groups;
    }

    protected abstract R processRequest(K payload, List<String> patternGroups,
                                        FullHttpRequest request) throws Exception;

    protected static int getInt(int position, List<String> queryStringGroups) {
        return Integer.parseInt(queryStringGroups.get(position));
    }

    protected static String getString(int position, List<String> queryStringGroups) {
        return queryStringGroups.get(position);
    }
}
