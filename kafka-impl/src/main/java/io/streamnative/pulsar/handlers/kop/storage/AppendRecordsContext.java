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
package io.streamnative.pulsar.handlers.kop.storage;

import io.netty.util.Recycler;
import io.streamnative.pulsar.handlers.kop.KafkaTopicManager;
import io.streamnative.pulsar.handlers.kop.PendingTopicFutures;
import java.util.Map;
import java.util.function.Consumer;
import lombok.Getter;
import org.apache.kafka.common.TopicPartition;

/**
 * AppendRecordsContext is use for pass parameters to ReplicaManager, to avoid long parameter lists.
 */
@Getter
public class AppendRecordsContext {
    private static final Recycler<AppendRecordsContext> RECYCLER = new Recycler<AppendRecordsContext>() {
        protected AppendRecordsContext newObject(Handle<AppendRecordsContext> handle) {
            return new AppendRecordsContext(handle);
        }
    };

    private final Recycler.Handle<AppendRecordsContext> recyclerHandle;
    private KafkaTopicManager topicManager;
    private Consumer<Integer> startSendOperationForThrottling;
    private Consumer<Integer> completeSendOperationForThrottling;
    private Map<TopicPartition, PendingTopicFutures> pendingTopicFuturesMap;

    private AppendRecordsContext(Recycler.Handle<AppendRecordsContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    // recycler and get for this object
    public static AppendRecordsContext get(final KafkaTopicManager topicManager,
                                           final Consumer<Integer> startSendOperationForThrottling,
                                           final Consumer<Integer> completeSendOperationForThrottling,
                                           final Map<TopicPartition, PendingTopicFutures> pendingTopicFuturesMap) {
        AppendRecordsContext context = RECYCLER.get();
        context.topicManager = topicManager;
        context.startSendOperationForThrottling = startSendOperationForThrottling;
        context.completeSendOperationForThrottling = completeSendOperationForThrottling;
        context.pendingTopicFuturesMap = pendingTopicFuturesMap;

        return context;
    }

    public void recycle() {
        topicManager = null;
        startSendOperationForThrottling = null;
        completeSendOperationForThrottling = null;
        pendingTopicFuturesMap = null;
        recyclerHandle.recycle(this);
    }

}
