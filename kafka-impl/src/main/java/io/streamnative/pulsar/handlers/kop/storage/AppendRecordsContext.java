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

import io.netty.channel.ChannelHandlerContext;
import io.streamnative.pulsar.handlers.kop.KafkaTopicManager;
import io.streamnative.pulsar.handlers.kop.PendingTopicFutures;
import java.util.Map;
import java.util.function.Consumer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

/**
 * AppendRecordsContext is use for pass parameters to ReplicaManager, to avoid long parameter lists.
 */
@Slf4j
@AllArgsConstructor
@Getter
public class AppendRecordsContext {
    private KafkaTopicManager topicManager;
    private Consumer<Integer> startSendOperationForThrottling;
    private Consumer<Integer> completeSendOperationForThrottling;
    private Map<TopicPartition, PendingTopicFutures> pendingTopicFuturesMap;
    private ChannelHandlerContext ctx;

    // recycler and get for this object
    public static AppendRecordsContext get(final KafkaTopicManager topicManager,
                                           final Consumer<Integer> startSendOperationForThrottling,
                                           final Consumer<Integer> completeSendOperationForThrottling,
                                           final Map<TopicPartition, PendingTopicFutures> pendingTopicFuturesMap,
                                           final ChannelHandlerContext ctx) {
        return new AppendRecordsContext(topicManager,
                startSendOperationForThrottling,
                completeSendOperationForThrottling,
                pendingTopicFuturesMap,
                ctx);
    }

}
