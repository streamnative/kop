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

import io.streamnative.pulsar.handlers.kop.exceptions.KoPTopicException;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperation;
import io.streamnative.pulsar.handlers.kop.utils.delayed.DelayedOperationPurgatory;
import io.streamnative.pulsar.handlers.kop.utils.timer.SystemTimer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

@Slf4j
class AdminManager {

    private final DelayedOperationPurgatory<DelayedOperation> topicPurgatory =
            DelayedOperationPurgatory.<DelayedOperation>builder()
                    .purgatoryName("topic")
                    .timeoutTimer(SystemTimer.builder().executorName("topic").build())
                    .build();

    private final PulsarAdmin admin;
    private final int defaultNumPartitions;
    private volatile Set<Node> brokersCache = new HashSet<>();
    private final ReentrantReadWriteLock brokersCacheLock = new ReentrantReadWriteLock();


    public AdminManager(PulsarAdmin admin, KafkaServiceConfiguration conf) {
        this.admin = admin;
        this.defaultNumPartitions = conf.getDefaultNumPartitions();
    }

    public void shutdown() {
        topicPurgatory.shutdown();
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
            } catch (KoPTopicException e) {
                errorFuture.complete(ApiError.fromThrowable(e));
                if (numTopics.decrementAndGet() == 0) {
                    complete.run();
                }
                return;
            }
            int numPartitions = detail.numPartitions;
            if (numPartitions == CreateTopicsRequest.NO_NUM_PARTITIONS) {
                numPartitions = defaultNumPartitions;
            }
            if (numPartitions < 0) {
                errorFuture.complete(ApiError.fromThrowable(
                        new InvalidRequestException("The partition '" + numPartitions + "' is negative")));
                if (numTopics.decrementAndGet() == 0) {
                    complete.run();
                }
                return;
            }
            admin.topics().createPartitionedTopicAsync(kopTopic.getFullName(), numPartitions)
                    .whenComplete((ignored, e) -> {
                        if (e == null) {
                            if (log.isDebugEnabled()) {
                                log.debug("Successfully create topic '{}'", topic);
                            }
                        } else {
                            log.error("Failed to create topic '{}': {}", topic, e);
                        }
                        if (e == null) {
                            errorFuture.complete(ApiError.NONE);
                        } else if (e instanceof PulsarAdminException.ConflictException) {
                            errorFuture.complete(ApiError.fromThrowable(
                                    new TopicExistsException("Topic '" + topic + "' already exists.")));
                        } else {
                            errorFuture.complete(ApiError.fromThrowable(e));
                        }
                        if (numTopics.decrementAndGet() == 0) {
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

    CompletableFuture<Map<ConfigResource, DescribeConfigsResponse.Config>> describeConfigsAsync(
            Map<ConfigResource, Optional<Set<String>>> resourceToConfigNames) {
        // Since Kafka's storage and policies are much different from Pulsar, here we just return a default config to
        // avoid some Kafka based systems need to send DescribeConfigs request, like confluent schema registry.
        final DescribeConfigsResponse.Config defaultTopicConfig = new DescribeConfigsResponse.Config(ApiError.NONE,
                KafkaLogConfig.getEntries().entrySet().stream().map(entry ->
                        new DescribeConfigsResponse.ConfigEntry(entry.getKey(), entry.getValue(),
                                DescribeConfigsResponse.ConfigSource.DEFAULT_CONFIG, false, false,
                                Collections.emptyList())
                ).collect(Collectors.toList()));

        Map<ConfigResource, CompletableFuture<DescribeConfigsResponse.Config>> futureMap =
                resourceToConfigNames.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    ConfigResource resource = entry.getKey();
                    try {
                        CompletableFuture<DescribeConfigsResponse.Config> future = new CompletableFuture<>();
                        switch (resource.type()) {
                            case TOPIC:
                                KopTopic kopTopic = new KopTopic(resource.name());
                                admin.topics().getPartitionedTopicMetadataAsync(kopTopic.getFullName())
                                        .whenComplete((metadata, e) -> {
                                            if (e != null) {
                                                if (e instanceof PulsarAdminException.NotFoundException) {
                                                    final ApiError error = new ApiError(
                                                            Errors.UNKNOWN_TOPIC_OR_PARTITION,
                                                            "Topic " + kopTopic.getOriginalName() + " doesn't exist");
                                                    future.complete(new DescribeConfigsResponse.Config(
                                                            error, Collections.emptyList()));
                                                } else {
                                                    future.complete(new DescribeConfigsResponse.Config(
                                                            ApiError.fromThrowable(e), Collections.emptyList()));
                                                }
                                            } else if (metadata.partitions > 0) {
                                                future.complete(defaultTopicConfig);
                                            } else {
                                                final ApiError error = new ApiError(Errors.INVALID_TOPIC_EXCEPTION,
                                                        "Topic " + kopTopic.getOriginalName()
                                                                + " is non-partitioned");
                                                future.complete(new DescribeConfigsResponse.Config(
                                                        error, Collections.emptyList()));
                                            }
                                        });
                                break;
                            case BROKER:
                                throw new RuntimeException("KoP doesn't support resource type: " + resource.type());
                            default:
                                throw new InvalidRequestException("Unsupported resource type: " + resource.type());
                        }
                        return future;
                    } catch (Exception e) {
                        return CompletableFuture.completedFuture(
                                new DescribeConfigsResponse.Config(ApiError.fromThrowable(e), Collections.emptyList()));
                    }
                }));
        CompletableFuture<Map<ConfigResource, DescribeConfigsResponse.Config>> resultFuture = new CompletableFuture<>();
        CompletableFuture.allOf(futureMap.values().toArray(new CompletableFuture[0])).whenComplete((ignored, e) -> {
            resultFuture.complete(futureMap.entrySet().stream().collect(
                    Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getNow(null))
            ));
        });
        return resultFuture;
    }

    public Map<String, Errors> deleteTopics(Set<String> topicsToDelete) {
        Map<String, Errors> result = new ConcurrentHashMap<>();
        topicsToDelete.forEach(topic -> {
            try {
                String topicFullName = new KopTopic(topic).getFullName();
                admin.topics().deletePartitionedTopic(topicFullName);
                result.put(topic, Errors.NONE);
                log.info("delete topic {} successfully.", topicFullName);
            } catch (PulsarAdminException e) {
                log.error("delete topic {} failed, exception: ", topic, e);
                result.put(topic, Errors.UNKNOWN_TOPIC_OR_PARTITION);
            }
        });
        return result;
    }

    public Collection<? extends Node> getBrokers() {
        return brokersCache;
    }

    public void setBrokers(Set<Node> newBrokers) {
        brokersCacheLock.writeLock().lock();
        try {
            this.brokersCache = newBrokers;
        } finally {
            brokersCacheLock.writeLock().unlock();
        }
    }
}
