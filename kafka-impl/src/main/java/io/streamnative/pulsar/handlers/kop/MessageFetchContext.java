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
package io.streamnative.pulsar.handlers.kop;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.concurrent.EventExecutor;
import io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder.KafkaHeaderAndRequest;
import io.streamnative.pulsar.handlers.kop.coordinator.transaction.TransactionCoordinator;
import io.streamnative.pulsar.handlers.kop.utils.GroupIdUtils;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.pulsar.metadata.api.GetResult;

/**
 * MessageFetchContext handling FetchRequest.
 */
@Slf4j
@Getter
public final class MessageFetchContext {

    private static final Recycler<MessageFetchContext> RECYCLER = new Recycler<MessageFetchContext>() {
        protected MessageFetchContext newObject(Handle<MessageFetchContext> handle) {
            return new MessageFetchContext(handle);
        }
    };

    private final Handle<MessageFetchContext> recyclerHandle;

    private volatile KafkaRequestHandler requestHandler;
    private volatile KafkaTopicManager topicManager;
    private volatile RequestStats statsLogger;
    private volatile TransactionCoordinator tc;
    private volatile EventExecutor eventExecutor;
    private volatile String clientHost;
    private volatile String namespacePrefix;
    private volatile int maxReadEntriesNum;

    private volatile RequestHeader header;

    private volatile KafkaTopicManagerSharedState sharedState;
    private volatile ScheduledExecutorService decodeExecutor;

    // recycler and get for this object
    public static MessageFetchContext get(KafkaRequestHandler requestHandler,
                                          TransactionCoordinator tc,
                                          final int maxReadEntriesNum,
                                          final String namespacePrefix,
                                          KafkaTopicManagerSharedState sharedState,
                                          ScheduledExecutorService decodeExecutor,
                                          KafkaHeaderAndRequest kafkaHeaderAndRequest) {
        MessageFetchContext context = RECYCLER.get();
        context.requestHandler = requestHandler;
        context.eventExecutor = requestHandler.ctx.executor();
        context.sharedState = sharedState;
        context.decodeExecutor = decodeExecutor;
        context.topicManager = requestHandler.getTopicManager();
        context.statsLogger = requestHandler.requestStats;
        context.tc = tc;
        context.clientHost = kafkaHeaderAndRequest.getClientHost();
        context.header = kafkaHeaderAndRequest.getHeader();
        context.namespacePrefix = namespacePrefix;
        context.maxReadEntriesNum = maxReadEntriesNum;
        return context;
    }

    private MessageFetchContext(Handle<MessageFetchContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public CompletableFuture<String> getCurrentConnectedGroupNameAsync() {
        return this.requestHandler.getCurrentConnectedGroup()
                .computeIfAbsent(this.clientHost, clientHost -> {
                    CompletableFuture<String> storeGroupIdFuture = new CompletableFuture<>();
                    String groupIdPath = GroupIdUtils.groupIdPathFormat(clientHost, header.clientId());
                    this.requestHandler.getMetadataStore()
                            .get(this.requestHandler.getGroupIdStoredPath() + groupIdPath)
                            .thenAccept(getResultOpt -> {
                                if (getResultOpt.isPresent()) {
                                    GetResult getResult = getResultOpt.get();
                                    storeGroupIdFuture.complete(new String(getResult.getValue() == null
                                            ? new byte[0] : getResult.getValue(), StandardCharsets.UTF_8));
                                } else {
                                    storeGroupIdFuture.complete("");
                                }
                            }).exceptionally(ex -> {
                                storeGroupIdFuture.completeExceptionally(ex);
                                return null;
                            });
                    return storeGroupIdFuture;
                });
    }

    public void recycle() {
        requestHandler = null;
        sharedState = null;
        decodeExecutor = null;
        topicManager = null;
        statsLogger = null;
        tc = null;
        clientHost = null;
        header = null;
        maxReadEntriesNum = -1;
        namespacePrefix = null;
        recyclerHandle.recycle(this);
    }
}
