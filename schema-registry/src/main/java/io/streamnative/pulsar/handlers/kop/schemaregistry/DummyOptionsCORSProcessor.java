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

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import java.util.concurrent.CompletableFuture;

public class DummyOptionsCORSProcessor extends HttpRequestProcessor {
    @Override
    protected boolean acceptRequest(FullHttpRequest request) {
        return request.method().name().equals("OPTIONS");
    }

    @Override
    protected CompletableFuture<FullHttpResponse> processRequest(FullHttpRequest request) {
        return CompletableFuture.completedFuture(buildEmptyResponseNoContentResponse());
    }
}
