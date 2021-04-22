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
package io.streamnative.pulsar.handlers.kop.coordinator.group;

import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.OffsetSearchPredicate;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;

/**
 * This class used to track all the partition offset commit position.
 */
@Slf4j
public class OffsetAcker implements Closeable {

    private static final Map<String, CompletableFuture<Consumer<byte[]>>> EMPTY_CONSUMERS = new HashMap<>();

    private final ConsumerBuilder<byte[]> consumerBuilder;
    private final BrokerService brokerService;

    // A map whose
    //   key is group id,
    //   value is a map whose
    //     key is the partition,
    //     value is the created future of consumer.
    // The consumer, whose subscription is the group id, is used for acknowledging message id cumulatively.
    // This behavior is equivalent to committing offsets in Kafka.
    public static final Map<String, Map<String, CompletableFuture<Consumer<byte[]>>>>
            CONSUMERS = new ConcurrentHashMap<>();

    public OffsetAcker(PulsarClientImpl pulsarClient) {
        this.consumerBuilder = pulsarClient.newConsumer()
                .receiverQueueSize(0)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
        brokerService = null;
    }

    public OffsetAcker(PulsarClientImpl pulsarClient, BrokerService brokerService) {
        this.consumerBuilder = pulsarClient.newConsumer()
                .receiverQueueSize(0)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
        this.brokerService = brokerService;
    }

    public void addOffsetsTracker(String groupId, byte[] assignment) {
        ByteBuffer assignBuffer = ByteBuffer.wrap(assignment);
        Assignment assign = ConsumerProtocol.deserializeAssignment(assignBuffer);
        if (log.isDebugEnabled()) {
            log.debug(" Add offsets after sync group: {}", assign.toString());
        }
        assign.partitions().forEach(topicPartition -> getOrCreateConsumer(groupId, topicPartition));
    }

    public void ackOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsetMetadata) {
        if (log.isDebugEnabled()) {
            log.debug(" ack offsets after commit offset for group: {}", groupId);
            offsetMetadata.forEach((partition, metadata) ->
                log.debug("\t partition: {}",
                    partition));
        }
        offsetMetadata.forEach(((topicPartition, offsetAndMetadata) -> {
            // 1. get consumer, then do ackCumulative
            CompletableFuture<Consumer<byte[]>> consumerFuture = getOrCreateConsumer(groupId, topicPartition);

            consumerFuture.whenComplete((consumer, throwable) -> {
                if (throwable != null) {
                    log.warn("Failed to create offset consumer for [group={}] [topic={}]: {}",
                            groupId, topicPartition, throwable.getMessage());
                    removeConsumer(groupId, topicPartition);
                    return;
                }
                KopTopic kopTopic = new KopTopic(topicPartition.topic());
                String partitionTopicName = kopTopic.getPartitionName(topicPartition.partition());
                brokerService.getTopic(partitionTopicName, false).whenComplete((topic, error) -> {
                    if (error != null) {
                        log.error("[{}] get topic failed when ack for {}.", partitionTopicName, groupId, error);
                        return;
                    }
                    if (topic.isPresent()) {
                        PersistentTopic persistentTopic = (PersistentTopic) topic.get();
                        PositionImpl position = null;
                        try {
                            position = (PositionImpl) persistentTopic.getManagedLedger()
                                    .asyncFindPosition(new OffsetSearchPredicate(offsetAndMetadata.offset())).get();
                            if (position.compareTo(
                                    (PositionImpl) persistentTopic.getManagedLedger().getLastConfirmedEntry()) > 0) {
                                position = (PositionImpl) persistentTopic.getManagedLedger().getLastConfirmedEntry();
                            }
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] find position {} for offset {}.",
                                        partitionTopicName, position, offsetAndMetadata.offset());
                            }
                        } catch (InterruptedException | ExecutionException e) {
                            log.error("[{}] Failed to find position for offset {} when processing offset commit.",
                                    partitionTopicName, offsetAndMetadata.offset());
                        }
                        consumer.acknowledgeCumulativeAsync(
                                new MessageIdImpl(position.getLedgerId(), position.getEntryId(), -1));
                    } else {
                        log.error("[{}] Topic not exist when ack for {}.", partitionTopicName, groupId);
                    }
                });
            });
        }));
    }

    public void close(Set<String> groupIds) {
        for (String groupId : groupIds) {
            final Map<String, CompletableFuture<Consumer<byte[]>>>
                    consumersToRemove = CONSUMERS.remove(groupId);
            if (consumersToRemove == null) {
                continue;
            }
            consumersToRemove.forEach((topicPartition, consumerFuture) -> {
                if (!consumerFuture.isDone()) {
                    log.warn("Consumer of [group={}] [topic={}] is not done while being closed",
                            groupId, topicPartition);
                    consumerFuture.complete(null);
                }
                final Consumer<byte[]> consumer = consumerFuture.getNow(null);
                if (consumer != null) {
                    log.debug("Try to close consumer of [group={}] [topic={}]", groupId, topicPartition);
                    consumer.closeAsync();
                }
            });
        }
    }

    @Override
    public void close() {
        log.info("close OffsetAcker with {} groupIds", CONSUMERS.size());
        close(CONSUMERS.keySet());
    }

    @NonNull
    public CompletableFuture<Consumer<byte[]>> getOrCreateConsumer(String groupId, TopicPartition topicPartition) {
        Map<String, CompletableFuture<Consumer<byte[]>>> group = CONSUMERS
            .computeIfAbsent(groupId, gid -> new ConcurrentHashMap<>());
        KopTopic kopTopic = new KopTopic(topicPartition.topic());
        String topicName = kopTopic.getPartitionName((topicPartition.partition()));
        return group.computeIfAbsent(
            topicName,
            name -> createConsumer(groupId, name));
    }

    @NonNull
    private CompletableFuture<Consumer<byte[]>> createConsumer(String groupId, String topicName) {
        return consumerBuilder.clone()
                .topic(topicName)
                .subscriptionName(groupId)
                .subscribeAsync();
    }

    public CompletableFuture<Consumer<byte[]>> getConsumer(String groupId, TopicPartition topicPartition) {
        KopTopic kopTopic = new KopTopic(topicPartition.topic());
        String topicName = kopTopic.getPartitionName((topicPartition.partition()));
        return CONSUMERS.getOrDefault(groupId, EMPTY_CONSUMERS).get(topicName);
    }

    public void removeConsumer(String groupId, TopicPartition topicPartition) {
        KopTopic kopTopic = new KopTopic(topicPartition.topic());
        String topicName = kopTopic.getPartitionName((topicPartition.partition()));

        final CompletableFuture<Consumer<byte[]>> consumerFuture =
                CONSUMERS.getOrDefault(groupId, EMPTY_CONSUMERS).remove(topicName);
        if (consumerFuture != null) {
            consumerFuture.whenComplete((consumer, e) -> {
                if (e == null) {
                    consumer.closeAsync();
                } else {
                    log.error("Failed to create consumer for [group={}] [topic={}]: {}",
                            groupId, topicPartition, e.getMessage());
                }
            });
        }
    }

    public static void removeOffsetAcker(String topicName) {
        CONSUMERS.forEach((groupId, group) -> {
            CompletableFuture<Consumer<byte[]> > consumerCompletableFuture = group.remove(topicName);
            if (consumerCompletableFuture != null) {
                consumerCompletableFuture.thenApply(Consumer::closeAsync).whenCompleteAsync((ignore, t) -> {
                    if (t != null) {
                        log.error("Failed to close offsetAcker consumer when remove partition {}.",
                            topicName);
                    }
                });
            }
        });
    }
}
