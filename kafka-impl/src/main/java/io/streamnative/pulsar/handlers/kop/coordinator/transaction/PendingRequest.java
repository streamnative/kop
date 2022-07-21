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
package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.Getter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;

public class PendingRequest {

    private static final String CLIENT_ID = "kop-internal-txn-client";
    private final RequestHeader requestHeader;
    private final AbstractRequest request;
    private final Consumer<ResponseContext> responseConsumerHandler;
    @Getter
    private final CompletableFuture<AbstractResponse> sendFuture = new CompletableFuture<>();

    public PendingRequest(final ApiKeys apiKeys,
                          final int correlationId,
                          final AbstractRequest request,
                          final Consumer<ResponseContext> responseConsumerHandler) {
        this.requestHeader = new RequestHeader(apiKeys, apiKeys.latestVersion(), CLIENT_ID, correlationId);
        this.request = request;
        this.responseConsumerHandler = responseConsumerHandler;
    }

    public ByteBuffer serialize() {
        return request.serialize(requestHeader);
    }

    public AbstractResponse parseResponse(final ByteBuffer buffer) {
        return AbstractResponse.parseResponse(requestHeader.apiKey(),
                requestHeader.apiKey().parseResponse(requestHeader.apiVersion(), buffer));
    }

    public short getApiVersion() {
        return requestHeader.apiVersion();
    }

    public int getCorrelationId() {
        return requestHeader.correlationId();
    }

    public void complete(final ResponseContext responseContext) {
        responseConsumerHandler.accept(responseContext);
        sendFuture.complete(responseContext.getResponse());
    }

    public void completeExceptionally(final Throwable throwable) {
        sendFuture.completeExceptionally(throwable);
    }

    @Override
    public String toString() {
        return requestHeader.toString();
    }
}
