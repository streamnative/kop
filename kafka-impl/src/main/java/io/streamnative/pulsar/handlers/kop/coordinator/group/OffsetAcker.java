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
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import io.streamnative.pulsar.handlers.kop.utils.OffsetSearchPredicate;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.CommandSubscribe;

/**
 * This class used to track all the partition offset commit position.
 */
@AllArgsConstructor
@Slf4j
public class OffsetAcker {

    private final BrokerService brokerService;

    public CompletableFuture<Map<TopicPartition, Errors>> ackOffsets(
            final String groupId, final Map<TopicPartition, OffsetAndMetadata> partitionAndOffsets) {
        if (groupId == null || groupId.isEmpty()) {
            log.warn("Try to ackOffsets for group {}", groupId);
            return CompletableFuture.completedFuture(
                    CoreUtils.mapValue(partitionAndOffsets, offsetAndMetadata -> Errors.INVALID_GROUP_ID));
        }

        final CompletableFuture<Map<TopicPartition, Errors>> future = new CompletableFuture<>();
        final Map<TopicPartition, Errors> ackResultMap = new ConcurrentHashMap<>();
        final int numPartitions = partitionAndOffsets.size();

        final BiConsumer<TopicPartition, Errors> addAckResult = (topicPartition, errors) -> {
            ackResultMap.put(topicPartition, errors);
            if (ackResultMap.size() == numPartitions) {
                future.complete(ackResultMap);
            }
        };

        partitionAndOffsets.forEach((topicPartition, offsetAndMetadata) -> {
            final String partitionTopicName = new KopTopic(topicPartition.topic())
                    .getPartitionName(topicPartition.partition());
            // 1. find PersistentTopic
            brokerService.getTopicIfExists(partitionTopicName).whenComplete((optTopic, e) -> {
                if (e != null) {
                    log.warn("OffsetAcker failed to get topic {}: {}", topicPartition, e.getMessage());
                    addAckResult.accept(topicPartition, Errors.NOT_LEADER_FOR_PARTITION);
                    return;
                }
                if (!optTopic.isPresent()) {
                    log.warn("OffsetAcker got empty topic {}", topicPartition);
                    addAckResult.accept(topicPartition, Errors.NOT_LEADER_FOR_PARTITION);
                    return;
                }

                final PersistentTopic persistentTopic = (PersistentTopic) optTopic.get();
                final ManagedLedger managedLedger = persistentTopic.getManagedLedger();
                final long offset = offsetAndMetadata.offset();
                // 2. find Position of the offset
                managedLedger.asyncFindPosition(new OffsetSearchPredicate(offset)).whenComplete((position, e1) -> {
                    if (e1 != null) {
                        log.warn("[{}] Failed to find position of offset {}: {}",
                                topicPartition, offset, e1.getMessage());
                        addAckResult.accept(topicPartition, Errors.forException(e1));
                        return;
                    }
                    if (position == null) {
                        log.warn("[{}] Failed to find position of offset {}: null", topicPartition, offset);
                        addAckResult.accept(topicPartition, Errors.forException(new ApiException("null position")));
                        return;
                    }

                    final PositionImpl lastConfirmedPosition = (PositionImpl) managedLedger.getLastConfirmedEntry();
                    final Position positionImpl = ((PositionImpl) position).compareTo(lastConfirmedPosition) > 0
                            ? lastConfirmedPosition
                            : (PositionImpl) position;
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] find position {} for offset {}", topicPartition, position, offset);
                    }

                    // 3. get or create the durable subscription
                    getOrCreateDurableSubscription(persistentTopic, groupId).whenComplete((subscription, e2) -> {
                        if (e2 != null) {
                            log.warn("[{}] Failed to create subscription {}: {}",
                                    topicPartition, groupId, e2.getMessage());
                            addAckResult.accept(topicPartition, Errors.forException(e2));
                            return;
                        }

                        // 4. reset cursor of the subscription to the position
                        subscription.resetCursor(positionImpl).whenComplete((ignored, e3) -> {
                            if (e3 == null) {
                                addAckResult.accept(topicPartition, Errors.NONE);
                            } else {
                                addAckResult.accept(topicPartition, Errors.forException(e3));
                            }
                        });
                    });
                });
            });
        });

        return future;
    }

    private CompletableFuture<Subscription> getOrCreateDurableSubscription(
            @NonNull final PersistentTopic persistentTopic,
            @NonNull final String groupId) {
        final Subscription subscription = persistentTopic.getSubscription(groupId);
        if (subscription != null) {
            return CompletableFuture.completedFuture(subscription);
        } else {
            // replicateSubscriptionState is false
            if (log.isDebugEnabled()) {
                log.debug("Create subscription {} for topic {}", groupId, persistentTopic.getName());
            }
            return persistentTopic.createSubscription(groupId, CommandSubscribe.InitialPosition.Earliest, false);
        }
    }
}
