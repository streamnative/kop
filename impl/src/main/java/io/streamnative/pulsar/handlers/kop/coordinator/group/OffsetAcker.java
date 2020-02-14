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
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
import io.streamnative.pulsar.handlers.kop.utils.TopicNameUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.ReaderBuilderImpl;
import org.apache.pulsar.client.impl.ReaderImpl;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.naming.TopicName;

/**
 * This will
 */
@Slf4j
public class OffsetAcker implements Closeable {

    private final ReaderBuilder<byte[]> readerBuilder;

    public OffsetAcker(PulsarClientImpl pulsarClient) {
        this.readerBuilder = new ReaderBuilderImpl<>(pulsarClient, Schema.BYTES)
            .receiverQueueSize(0)
            .startMessageId(MessageId.earliest);
    }

    // <groupId, consumers>
    Map<String, Map<TopicPartition, CompletableFuture<Consumer<byte[]>>>> consumers = new ConcurrentHashMap<>();

    public void ackOffsets(String groupId, Map<TopicPartition, OffsetAndMetadata> offsetMetadata) {
        offsetMetadata.forEach(((topicPartition, offsetAndMetadata) -> {
            // 1. get consumer, then do ackCumulative
            CompletableFuture<Consumer<byte[]>> consumerFuture = getConsumer(groupId, topicPartition);

            consumerFuture.whenComplete((consumer, throwable) -> {
                MessageId messageId = MessageIdUtils.getMessageId(offsetAndMetadata.offset());
                consumer.acknowledgeCumulativeAsync(messageId);
            });
        }));
    }

    public CompletableFuture<Consumer<byte[]>> getConsumer(String groupId, TopicPartition topicPartition) {
        Map<TopicPartition, CompletableFuture<Consumer<byte[]>>> group = consumers
            .computeIfAbsent(groupId, gid -> new ConcurrentHashMap<>());
        return group.computeIfAbsent(
                topicPartition,
                partition -> createConsumer(groupId, partition));
    }

    // todo: need close consumer when deleteGroup.
    @Override
    public void close() throws IOException {

    }

    // TODO: need to handle group join/leave/sync, get the topics for each memoryId.
    // consumer for partition created when SyncGroup response send.
    // consumer for partition delete when leave?



    private CompletableFuture<Consumer<byte[]>> createConsumer(String groupId, TopicPartition topicPartition) {
        TopicName pulsarTopicName = TopicNameUtils.pulsarTopicName(topicPartition);
        return readerBuilder.clone()
            .topic(pulsarTopicName.toString())
            .subscriptionRolePrefix(groupId)
            .createAsync()
            .thenApply(reader -> ((ReaderImpl<byte[]>) reader).getConsumer());
    }

}
