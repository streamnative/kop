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

import static io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationKey.TopicKey;
import static org.apache.kafka.common.requests.CreateTopicsRequest.TopicDetails;

import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperation;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationPurgatory;
import io.streamnative.pulsar.handlers.kop.utils.timer.SystemTimer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.pulsar.client.admin.PulsarAdmin;

@Slf4j
class AdminManager {

    private final DelayedOperationPurgatory<DelayedOperation> topicPurgatory =
            DelayedOperationPurgatory.<DelayedOperation>builder()
                    .purgatoryName("topic")
                    .timeoutTimer(SystemTimer.builder().executorName("topic").build())
                    .build();

    private final PulsarAdmin admin;

    AdminManager(PulsarAdmin admin) {
        this.admin = admin;
    }

    CompletableFuture<Map<String, ApiError>> createTopicsAsync(Map<String, TopicDetails> createInfo, int timeoutMs) {
        final Map<String, CompletableFuture<ApiError>> futureMap = new ConcurrentHashMap<>();
        final AtomicInteger numTopics = new AtomicInteger(createInfo.size());
        final CompletableFuture<Map<String, ApiError>> resultFuture = new CompletableFuture<>();

        Runnable complete = () -> {
            // prevent `futureMap` from being modified by createPartitionedTopicAsync()'s callback
            numTopics.set(0);
            // complete the pending futures with timeout error
            futureMap.values().forEach(future -> {
                if (!future.isDone()) {
                    future.complete(new ApiError(Errors.REQUEST_TIMED_OUT, null));
                }
            });
            resultFuture.complete(futureMap.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().getNow(ApiError.NONE)
            )));
        };

        createInfo.forEach((topic, detail) -> {
            final CompletableFuture<ApiError> errorFuture = new CompletableFuture<>();
            futureMap.put(topic, errorFuture);

            KopTopic kopTopic;
            try {
                kopTopic = new KopTopic(topic);
            } catch (RuntimeException e) {
                errorFuture.complete(ApiError.fromThrowable(e));
                return;
            }
            admin.topics().createPartitionedTopicAsync(kopTopic.getFullName(), detail.numPartitions)
                    .whenComplete((ignored, e) -> {
                        if (e == null) {
                            if (log.isDebugEnabled()) {
                                log.debug("Successfully create topic '{}'", topic);
                            }
                        } else {
                            log.error("Failed to create topic '{}': {}", topic, e);
                        }

                        int restNumTopics = numTopics.decrementAndGet();
                        if (restNumTopics < 0) {
                            return;
                        }
                        errorFuture.complete((e == null) ? ApiError.NONE : ApiError.fromThrowable(e));
                        if (restNumTopics == 0) {
                            complete.run();
                        }
                    });
        });

        if (timeoutMs <= 0) {
            complete.run();
        } else {
            List<Object> delayedCreateKeys =
                    createInfo.keySet().stream().map(TopicKey::new).collect(Collectors.toList());
            DelayedCreateTopics delayedCreate = new DelayedCreateTopics(timeoutMs, numTopics, complete);
            topicPurgatory.tryCompleteElseWatch(delayedCreate, delayedCreateKeys);
        }

        return resultFuture;
    }
}
