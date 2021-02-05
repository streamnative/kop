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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

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

    private final ConsumerBuilder<byte[]> consumerBuilder;
    private final BrokerService brokerService;

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

    // map off consumser: <groupId, consumers>
    Map<String, Map<TopicPartition, CompletableFuture<Consumer<byte[]>>>> consumers = new ConcurrentHashMap<>();

    public void addOffsetsTracker(String groupId, byte[] assignment) {
        ByteBuffer assignBuffer = ByteBuffer.wrap(assignment);
        Assignment assign = ConsumerProtocol.deserializeAssignment(assignBuffer);
        if (log.isDebugEnabled()) {
            log.debug(" Add offsets after sync group: {}", assign.toString());
        }
        assign.partitions().forEach(topicPartition -> getConsumer(groupId, topicPartition));
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
            CompletableFuture<Consumer<byte[]>> consumerFuture = getConsumer(groupId, topicPartition);

            consumerFuture.whenComplete((consumer, throwable) -> {
                if (throwable != null) {
                    log.warn("Error when get consumer for offset ack:", throwable);
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
        groupIds.forEach(groupId -> {
            // consumers cache is empty if the broker restart.
            if (!consumers.containsKey(groupId)) {
                return;
            }
            consumers.get(groupId).values().forEach(consumerFuture -> {
                consumerFuture.whenComplete((consumer, throwable) -> {
                    if (throwable != null) {
                        log.warn("Error when get consumer for consumer group close:", throwable);
                        return;
                    }
                    try {
                        consumer.close();
                    } catch (Exception e) {
                        log.warn("Error when close consumer topic: {}, sub: {}.",
                                consumer.getTopic(), consumer.getSubscription(), e);
                    }
                });
            });
        });
    }

    @Override
    public void close() {
        log.info("close OffsetAcker with {} groupIds", consumers.size());
        close(consumers.keySet());
    }

    public CompletableFuture<Consumer<byte[]>> getConsumer(String groupId, TopicPartition topicPartition) {
        Map<TopicPartition, CompletableFuture<Consumer<byte[]>>> group = consumers
            .computeIfAbsent(groupId, gid -> new ConcurrentHashMap<>());
        return group.computeIfAbsent(
            topicPartition,
            partition -> createConsumer(groupId, partition));
    }

    private CompletableFuture<Consumer<byte[]>> createConsumer(String groupId, TopicPartition topicPartition) {
        KopTopic kopTopic = new KopTopic(topicPartition.topic());
        return consumerBuilder.clone()
                .topic(kopTopic.getPartitionName(topicPartition.partition()))
                .subscriptionName(groupId)
                .subscribeAsync();
    }

}
