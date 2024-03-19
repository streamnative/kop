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

import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.CURRENT_GROUP_VALUE_SCHEMA_VERSION;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.groupMetadataKey;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.groupMetadataValue;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.offsetCommitKey;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.offsetCommitValue;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.readGroupMessageValue;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.readMessageKey;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.readOffsetMessageValue;
import static io.streamnative.pulsar.handlers.kop.utils.CoreUtils.groupBy;
import static io.streamnative.pulsar.handlers.kop.utils.CoreUtils.inLock;
import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.kop.SystemTopicClient;
import io.streamnative.pulsar.handlers.kop.coordinator.CompactedPartitionedTopic;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata.CommitRecordMetadataAndOffset;
import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import io.streamnative.pulsar.handlers.kop.utils.KafkaResponseUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Manager to manage a coordination group.
 */
@Slf4j
public class GroupMetadataManager {

    private final byte magicValue = RecordBatch.CURRENT_MAGIC_VALUE;
    private final CompressionType compressionType;
    @Getter
    private final OffsetConfig offsetConfig;
    private final String namespacePrefix;
    private final ConcurrentMap<String, GroupMetadata> groupMetadataCache = new ConcurrentHashMap<>();
    /* lock protecting access to loading and owned partition sets */
    private final ReentrantLock partitionLock = new ReentrantLock();
    /**
     * partitions of consumer groups that are being loaded, its lock should
     * be always called BEFORE the group lock if needed.
     */
    private final Set<Integer> loadingPartitions = new HashSet<>();
    /* partitions of consumer groups that are assigned, using the same loading partition lock */
    private final Set<Integer> ownedPartitions = new HashSet<>();
    /* shutting down flag */
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    /* single-thread scheduler to handle offset/group metadata cache loading and unloading */
    private final ScheduledExecutorService scheduler;
    /**
     * The groups with open transactional offsets commits per producer. We need this because when the commit or abort
     * marker comes in for a transaction, it is for a particular partition on the offsets topic and a particular
     * producerId. We use this structure to quickly find the groups which need to be updated by the commit/abort
     * marker.
     */
    private final Map<Long, Set<String>> openGroupsForProducer = new HashMap<>();

    @Getter
    @VisibleForTesting
    private final CompactedPartitionedTopic<ByteBuffer> offsetTopic;
    private final Time time;

    /**
     * The key interface.
     */
    public interface BaseKey {
        short version();
        Object key();
    }

    /**
     * Key to index group metadata.
     */
    @Data
    @Accessors(fluent = true)
    static class GroupMetadataKey implements BaseKey {

        private final short version;
        private final String key;

        @Override
        public String toString() {
            return key;
        }

    }

    /**
     * Key to index offset.
     */
    @Data
    @Accessors(fluent = true)
    public static class OffsetKey implements BaseKey {

        private final short version;
        private final GroupTopicPartition key;

        @Override
        public String toString() {
            return key.toString();
        }

    }

    @AllArgsConstructor
    public static class UnknownKey implements BaseKey {

        private final short version;

        @Override
        public short version() {
            return version;
        }

        @Override
        public Object key() {
            return null;
        }
    }

    /**
     * The group on a topic partition.
     */
    @Data
    @Accessors(fluent = true)
    static class GroupTopicPartition {

        private final String group;
        private final TopicPartition topicPartition;

        GroupTopicPartition(String group,
                            String topic,
                            int partition) {
            this.group = group;
            this.topicPartition = new TopicPartition(topic, partition);
        }

        @Override
        public String toString() {
            return String.format(
                "[%s, %s, %d]",
                group, topicPartition.topic(), topicPartition.partition()
            );
        }

    }

    public GroupMetadataManager(OffsetConfig offsetConfig,
                                SystemTopicClient client,
                                ScheduledExecutorService scheduler,
                                String namespacePrefixForMetadata,
                                Time time) {
        this.offsetConfig = offsetConfig;
        this.compressionType = offsetConfig.offsetsTopicCompressionType();
        this.offsetTopic = new CompactedPartitionedTopic<>(client.getPulsarClient(), Schema.BYTEBUFFER,
                client.getMaxPendingMessages(), offsetConfig, scheduler,
                buffer -> buffer.limit() == 0);
        this.scheduler = scheduler;
        this.namespacePrefix = namespacePrefixForMetadata;
        this.time = time;
    }

    public static int getPartitionId(String groupId, int offsetsTopicNumPartitions) {
        return MathUtils.signSafeMod(groupId.hashCode(), offsetsTopicNumPartitions);
    }

    public void startup(boolean enableMetadataExpiration) {
        if (enableMetadataExpiration) {
            scheduler.scheduleAtFixedRate(
                this::cleanupGroupMetadata,
                offsetConfig.offsetsRetentionCheckIntervalMs(),
                offsetConfig.offsetsRetentionCheckIntervalMs(),
                TimeUnit.MILLISECONDS
            );
        }
    }

    public void shutdown() {
        shuttingDown.set(true);
        offsetTopic.close();
        scheduler.shutdown();
    }

    public Iterable<GroupMetadata> currentGroups() {
        return groupMetadataCache.values();
    }

    public Stream<GroupMetadata> currentGroupsStream() {
        return groupMetadataCache.values().stream();
    }

    public boolean isPartitionOwned(int partition) {
        return inLock(
            partitionLock,
            () -> ownedPartitions.contains(partition));
    }

    public boolean isPartitionLoading(int partition) {
        return inLock(
            partitionLock,
            () -> loadingPartitions.contains(partition)
        );
    }

    public int partitionFor(String groupId) {
        return getPartitionId(groupId, offsetConfig().offsetsTopicNumPartitions());
    }

    public String getTopicPartitionName() {
        return offsetConfig.offsetsTopicName();
    }

    public String getTopicPartitionName(int partitionId) {
        return offsetConfig.offsetsTopicName() + PARTITIONED_TOPIC_SUFFIX + partitionId;
    }

    public boolean isGroupLocal(String groupId) {
        return isPartitionOwned(partitionFor(groupId));
    }

    public boolean isGroupLoading(String groupId) {
        return isPartitionLoading(partitionFor(groupId));
    }

    public boolean isLoading() {
        return inLock(
            partitionLock,
            () -> !loadingPartitions.isEmpty()
        );
    }

    public OffsetConfig offsetConfig() {
        return offsetConfig;
    }

    // return true iff group is owned and the group doesn't exist
    public boolean groupNotExists(String groupId) {
        return inLock(
            partitionLock,
            () -> isGroupLocal(groupId)
                && getGroup(groupId)
                .map(group -> group.inLock(() -> group.is(GroupState.Dead)))
                .orElse(true)
        );
    }

    public Optional<GroupMetadata> getGroup(String groupId) {
        return Optional.ofNullable(groupMetadataCache.getOrDefault(groupId, null));
    }

    public GroupMetadata addGroup(GroupMetadata group) {
        GroupMetadata oldGroup = groupMetadataCache.putIfAbsent(group.groupId(), group);
        if (null != oldGroup) {
            return oldGroup;
        } else {
            return group;
        }
    }

    public CompletableFuture<Errors> storeGroup(GroupMetadata group,
                                                Map<String, byte[]> groupAssignment) {

        TimestampType timestampType = TimestampType.CREATE_TIME;
        long timestamp = time.milliseconds();
        byte[] key = groupMetadataKey(group.groupId());
        byte[] value = groupMetadataValue(
            group, groupAssignment, CURRENT_GROUP_VALUE_SCHEMA_VERSION);

        // construct the record
        ByteBuffer buffer = ByteBuffer.allocate(AbstractRecords.estimateSizeInBytes(
            magicValue,
            compressionType,
            Lists.newArrayList(new SimpleRecord(timestamp, key, value))
        ));
        MemoryRecordsBuilder recordsBuilder = MemoryRecords.builder(
            buffer,
            magicValue,
            compressionType,
            timestampType,
            0L
        );
        recordsBuilder.append(timestamp, key, value);
        MemoryRecords records = recordsBuilder.build();

        String groupId = group.groupId();
        int partition = partitionFor(groupId);
        return storeOffsetMessageAsync(partition, key, records.buffer(), timestamp).thenApply(__ -> {
            if (!isGroupLocal(groupId)) {
                log.warn("add partition ownership for group {}", groupId);
                addPartitionOwnership(partition);
            }
            return Errors.NONE;
        }).exceptionally(e -> {
            Throwable cause = e.getCause();
            log.error("Coordinator failed to store group {}: {}", groupId, cause.getMessage());
            if (cause instanceof PulsarClientException.AlreadyClosedException) {
                return Errors.NOT_COORDINATOR;
            } else if (cause instanceof PulsarClientException.TimeoutException) {
                return Errors.REBALANCE_IN_PROGRESS;
            } else {
                return Errors.UNKNOWN_SERVER_ERROR;
            }
        });
    }

    // visible for mock
    CompletableFuture<MessageId> storeOffsetMessageAsync(
            int partition, byte[] key, ByteBuffer value, long timestamp) {
        return offsetTopic.sendAsync(partition, key, value, timestamp);
    }

    @VisibleForTesting
    void storeOffsetMessage(int partition, byte[] key, ByteBuffer value) {
        storeOffsetMessageAsync(partition, key, value, System.currentTimeMillis()).join();
    }

    public CompletableFuture<Map<TopicPartition, Errors>> storeOffsets(
        GroupMetadata group,
        String consumerId,
        Map<TopicPartition, OffsetAndMetadata> offsetMetadata
    ) {
        return storeOffsets(
            group,
            consumerId,
            offsetMetadata,
            RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH
        );
    }

    public CompletableFuture<Map<TopicPartition, Errors>> storeOffsets(
        GroupMetadata group,
        String consumerId,
        Map<TopicPartition, OffsetAndMetadata> offsetMetadata,
        long producerId,
        short producerEpoch
    ) {
        // first filter out partitions with offset metadata size exceeding limit
        Map<TopicPartition, OffsetAndMetadata> filteredOffsetMetadata =
            offsetMetadata.entrySet().stream()
                .filter(entry -> validateOffsetMetadataLength(entry.getValue().metadata()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));

        group.inLock(() -> {
            if (!group.hasReceivedConsistentOffsetCommits()) {
                log.warn("group: {} with leader: {} has received offset commits from consumers as well "
                        + "as transactional offsetsProducers. Mixing both types of offset commits will generally"
                        + " result in surprises and should be avoided.",
                    group.groupId(), group.leaderOrNull());
            }
            return null;
        });

        boolean isTxnOffsetCommit = producerId != RecordBatch.NO_PRODUCER_ID;
        // construct the message set to append
        if (filteredOffsetMetadata.isEmpty()) {
            // compute the final error codes for the commit response
            Map<TopicPartition, Errors> commitStatus = offsetMetadata.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                    e -> Errors.OFFSET_METADATA_TOO_LARGE
                ));
            return CompletableFuture.completedFuture(commitStatus);
        }

        TimestampType timestampType = TimestampType.CREATE_TIME;
        long timestamp = time.milliseconds();
        List<SimpleRecord> records = filteredOffsetMetadata.entrySet().stream()
            .map(e -> {
                byte[] key = offsetCommitKey(group.groupId(), e.getKey(), namespacePrefix);
                byte[] value = offsetCommitValue(e.getValue());
                return new SimpleRecord(timestamp, key, value);
            })
            .collect(Collectors.toList());

        ByteBuffer buffer = ByteBuffer.allocate(
            AbstractRecords.estimateSizeInBytes(
                magicValue, compressionType, records
            )
        );

        MemoryRecordsBuilder builder = MemoryRecords.builder(
            buffer, magicValue, compressionType,
            timestampType, 0L, timestamp,
            producerId,
            producerEpoch,
            0,
            isTxnOffsetCommit,
            RecordBatch.NO_PARTITION_LEADER_EPOCH
        );
        records.forEach(builder::append);

        MemoryRecords entries = builder.build();

        if (isTxnOffsetCommit) {
            group.inLock(() -> {
                addProducerGroup(producerId, group.groupId());
                group.prepareTxnOffsetCommit(producerId, offsetMetadata);
                return null;
            });
        } else {
            group.inLock(() -> {
                group.prepareOffsetCommit(offsetMetadata);
                return null;
            });
        }

        // dummy offset commit key
        String groupId = group.groupId();
        int partition = partitionFor(groupId);
        byte[] key = offsetCommitKey(groupId, new TopicPartition("", -1), namespacePrefix);
        return storeOffsetMessageAsync(partition, key, entries.buffer(), timestamp)
            .thenApply(messageId -> {
                if (!group.is(GroupState.Dead)) {
                    MessageIdImpl lastMessageId = (MessageIdImpl) messageId;
                    filteredOffsetMetadata.forEach((tp, offsetAndMetadata) -> {
                        CommitRecordMetadataAndOffset commitRecordMetadataAndOffset =
                            new CommitRecordMetadataAndOffset(
                                Optional.of(new PositionImpl(lastMessageId.getLedgerId(), lastMessageId.getEntryId())),
                                offsetAndMetadata
                            );
                        if (isTxnOffsetCommit) {
                            group.onTxnOffsetCommitAppend(producerId, tp, commitRecordMetadataAndOffset);
                        } else {
                            group.onOffsetCommitAppend(tp, commitRecordMetadataAndOffset);
                        }
                    });
                }
                return Errors.NONE;
            })
            .exceptionally(e -> {
                if (!group.is(GroupState.Dead)) {
                    if (!group.hasPendingOffsetCommitsFromProducer(producerId)) {
                        removeProducerGroup(producerId, group.groupId());
                    }
                    filteredOffsetMetadata.forEach((tp, offsetAndMetadata) -> {
                        if (isTxnOffsetCommit) {
                            group.failPendingTxnOffsetCommit(producerId, tp);
                        } else {
                            group.failPendingOffsetWrite(tp, offsetAndMetadata);
                        }
                    });
                }

                Throwable cause = e.getCause();
                log.error("Offset commit {} from group {}, consumer {} with generation {} failed"
                                + " when appending to log due to {}",
                        filteredOffsetMetadata, group.groupId(), consumerId, group.generationId(), cause.getMessage());
                if (cause instanceof PulsarClientException.AlreadyClosedException) {
                    return Errors.NOT_COORDINATOR;
                } else if (cause instanceof PulsarClientException.TimeoutException) {
                    return Errors.REQUEST_TIMED_OUT;
                } else {
                    return Errors.UNKNOWN_SERVER_ERROR;
                }
            })
            .thenApply(errors -> offsetMetadata.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                    e -> {
                        if (validateOffsetMetadataLength(e.getValue().metadata())) {
                            return errors;
                        } else {
                            return Errors.OFFSET_METADATA_TOO_LARGE;
                        }
                    }
                )));
    }

    /**
     * The most important guarantee that this API provides is that it should never return a stale offset.
     * i.e., it either returns the current offset or it begins to sync the cache from the log
     * (and returns an error code).
     */
    public Map<TopicPartition, PartitionData> getOffsets(
        String groupId, Optional<List<TopicPartition>> topicPartitionsOpt
    ) {
        if (log.isTraceEnabled()) {
            log.trace("Getting offsets of {} for group {}.",
                topicPartitionsOpt.map(List::toString).orElse("all partitions"),
                groupId);
        }

        GroupMetadata group = groupMetadataCache.get(groupId);
        if (null == group) {
            return topicPartitionsOpt.orElse(Collections.emptyList())
                .stream()
                .collect(Collectors.toMap(
                    tp -> tp,
                    __ -> KafkaResponseUtils.newOffsetFetchPartition()
                ));
        }

        return group.inLock(() -> {
            if (group.is(GroupState.Dead)) {
                return topicPartitionsOpt.orElse(Collections.emptyList())
                    .stream()
                    .collect(Collectors.toMap(
                        tp -> tp,
                        tp -> KafkaResponseUtils.newOffsetFetchPartition()
                    ));
            }

            return topicPartitionsOpt.map(topicPartitions ->
                topicPartitions.stream()
                    .collect(Collectors.toMap(
                        tp -> tp,
                        topicPartition ->
                            group.offset(topicPartition, namespacePrefix)
                                .map(offsetAndMetadata -> KafkaResponseUtils.newOffsetFetchPartition(
                                    offsetAndMetadata.offset(),
                                    offsetAndMetadata.metadata())
                                )
                                .orElseGet(KafkaResponseUtils::newOffsetFetchPartition)
                    ))
            ).orElseGet(() ->
                group.allOffsets().entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                        e -> {
                            OffsetAndMetadata oam = e.getValue();
                            return KafkaResponseUtils.newOffsetFetchPartition(
                                oam.offset(),
                                oam.metadata()
                            );
                        }
                    ))
            );
        });
    }


    private void addProducerGroup(long producerId,
                                  String groupId) {
        synchronized (openGroupsForProducer) {
            openGroupsForProducer.computeIfAbsent(producerId, (pid) -> new HashSet<>())
                .add(groupId);
        }
    }

    private void removeProducerGroup(long producerId,
                                     String groupId) {
        synchronized (openGroupsForProducer) {
            Set<String> groups = openGroupsForProducer.get(producerId);
            if (null != groups) {
                groups.remove(groupId);
                if (groups.isEmpty()) {
                    openGroupsForProducer.remove(producerId);
                }
            }
            openGroupsForProducer.computeIfAbsent(producerId, (pid) -> new HashSet<>())
                .remove(groupId);
        }
    }

    private Set<String> groupsBelongingToPartitions(long producerId,
                                                    Set<Integer> partitions) {
        synchronized (openGroupsForProducer) {
            return openGroupsForProducer.computeIfAbsent(producerId, (pid) -> new HashSet<>())
                .stream()
                .filter(group -> partitions.contains(partitionFor(group)))
                .collect(Collectors.toSet());
        }
    }

    private void removeGroupFromAllProducers(String groupId) {
        synchronized (openGroupsForProducer) {
            openGroupsForProducer.forEach((pid, groups) -> groups.remove(groupId));
        }
    }

    /*
     * Check if the offset metadata length is valid
     */
    private boolean validateOffsetMetadataLength(String metadata) {
        return metadata == null || metadata.length() <= offsetConfig.maxMetadataSize();
    }

    public CompletableFuture<Void> scheduleLoadGroupAndOffsets(int offsetsPartition,
                                                               Consumer<GroupMetadata> onGroupLoaded) {
        final String topicPartition = getTopicPartitionName(offsetsPartition);
        if (addLoadingPartition(offsetsPartition)) {
            final var future = new CompletableFuture<Void>();
            log.info("Scheduling loading of offsets and group metadata from {}", topicPartition);
            final var loadedOffsets = new HashMap<GroupTopicPartition, CommitRecordMetadataAndOffset>();
            final var pendingOffsets = new HashMap<Long, Map<GroupTopicPartition, CommitRecordMetadataAndOffset>>();
            final var loadedGroups = new HashMap<String, GroupMetadata>();
            final var removedGroups = new HashSet<String>();
            offsetTopic.readToLatest(offsetsPartition, msg -> {
                if (!shuttingDown.get()) {
                    processOffsetMessage(msg, loadedOffsets, pendingOffsets, loadedGroups, removedGroups);
                }
            }).thenApplyAsync(result -> {
                processLoadedAndRemovedGroups(topicPartition, onGroupLoaded, loadedOffsets, pendingOffsets,
                        loadedGroups, removedGroups);
                return result;
            }, scheduler).whenCompleteAsync((result, cause) -> {
                inLock(partitionLock, () -> {
                    ownedPartitions.add(offsetsPartition);
                    loadingPartitions.remove(offsetsPartition);
                    return null;
                });
                if (null != cause) {
                    log.error("Error loading offsets from {}", topicPartition, cause);
                    future.completeExceptionally(cause);
                    return;
                }
                log.info("Finished loading {} offsets and group metadata from {} in {} milliseconds",
                        topicPartition, result.numMessages(), result.timeMs());
                future.complete(null);
            }, scheduler);
            return future;
        } else {
            log.info("Already loading offsets and group metadata from {}", topicPartition);
            return CompletableFuture.completedFuture(null);
        }
    }

    private void processOffsetMessage(Message<ByteBuffer> msg,
                                      Map<GroupTopicPartition, CommitRecordMetadataAndOffset> loadedOffsets,
                                      Map<Long, Map<GroupTopicPartition, CommitRecordMetadataAndOffset>> pendingOffsets,
                                      Map<String, GroupMetadata> loadedGroups,
                                      Set<String> removedGroups) {
        if (log.isTraceEnabled()) {
            log.trace("Reading the next metadata message from topic {}", msg.getTopicName());
        }
        if (!msg.hasKey()) {
            // the messages without key are placeholders
            return;
        }
        MemoryRecords.readableRecords(msg.getValue()).batches().forEach(batch -> {
            boolean isTxnOffsetCommit = batch.isTransactional();
            if (batch.isControlBatch()) {
                Iterator<Record> recordIterator = batch.iterator();
                if (recordIterator.hasNext()) {
                    Record record = recordIterator.next();
                    ControlRecordType controlRecord = ControlRecordType.parse(record.key());
                    if (controlRecord == ControlRecordType.COMMIT) {
                        pendingOffsets.getOrDefault(batch.producerId(), Collections.emptyMap())
                                .forEach(((groupTopicPartition, commitRecordMetadataAndOffset) -> {
                                    if (!loadedOffsets.containsKey(groupTopicPartition)
                                            || loadedOffsets.get(groupTopicPartition)
                                            .olderThan(commitRecordMetadataAndOffset)) {
                                        loadedOffsets.put(groupTopicPartition, commitRecordMetadataAndOffset);
                                    }
                                }));
                    }
                    pendingOffsets.remove(batch.producerId());
                }
            } else {
                Optional<PositionImpl> batchBaseOffset = Optional.empty();
                for (Record record : batch) {
                    if (!record.hasKey()) {
                        // It throws an exception here in Kafka. However, the exception will be caught and processed
                        // later in Kafka, while we cannot catch the exception in KoP. So here just skip it.
                        log.warn("[{}] Group metadata/offset entry key should not be null", msg.getMessageId());
                        continue;
                    }
                    if (batchBaseOffset.isEmpty()) {
                        batchBaseOffset = Optional.of(new PositionImpl(0, record.offset()));
                    }
                    BaseKey bk = readMessageKey(record.key());
                    if (log.isTraceEnabled()) {
                        log.trace("Applying metadata record {} received from {}", bk, msg.getTopicName());
                    }
                    if (bk instanceof OffsetKey) {
                        OffsetKey offsetKey = (OffsetKey) bk;
                        if (isTxnOffsetCommit && !pendingOffsets.containsKey(batch.producerId())) {
                            pendingOffsets.put(batch.producerId(), new HashMap<>());
                        }
                        // load offset
                        GroupTopicPartition groupTopicPartition = offsetKey.key();
                        if (!record.hasValue()) { // TODO: when should we send null messages?
                            if (isTxnOffsetCommit) {
                                pendingOffsets.get(batch.producerId()).remove(groupTopicPartition);
                            } else {
                                loadedOffsets.remove(groupTopicPartition);
                            }
                        } else {
                            OffsetAndMetadata offsetAndMetadata = readOffsetMessageValue(record.value());
                            CommitRecordMetadataAndOffset commitRecordMetadataAndOffset =
                                    new CommitRecordMetadataAndOffset(batchBaseOffset, offsetAndMetadata);
                            if (isTxnOffsetCommit) {
                                pendingOffsets.get(batch.producerId())
                                        .put(groupTopicPartition, commitRecordMetadataAndOffset);
                            } else {
                                loadedOffsets.put(groupTopicPartition, commitRecordMetadataAndOffset);
                            }
                        }
                    } else if (bk instanceof GroupMetadataKey) {
                        GroupMetadataKey groupMetadataKey = (GroupMetadataKey) bk;
                        String groupId = groupMetadataKey.key();
                        GroupMetadata groupMetadata = readGroupMessageValue(groupId, record.value());
                        if (groupMetadata != null) {
                            removedGroups.remove(groupId);
                            loadedGroups.put(groupId, groupMetadata);
                        } else {
                            loadedGroups.remove(groupId);
                            removedGroups.add(groupId);
                        }
                    } else {
                        log.warn("Unknown message key with version {}"
                                + " while loading offsets and group metadata from {}."
                                + "Ignoring it. It could be a left over from an aborted upgrade.",
                                bk.version(), msg.getTopicName());
                    }
                }
            }
        });
    }

    private void processLoadedAndRemovedGroups(
            String topicPartition,
            Consumer<GroupMetadata> onGroupLoaded,
            Map<GroupTopicPartition, CommitRecordMetadataAndOffset> loadedOffsets,
            Map<Long, Map<GroupTopicPartition, CommitRecordMetadataAndOffset>> pendingOffsets,
            Map<String, GroupMetadata> loadedGroups,
            Set<String> removedGroups) {
        if (log.isTraceEnabled()) {
            log.trace("Completing loading : {} loaded groups, {} removed groups, {} loaded offsets, {} pending offsets",
                    loadedGroups.size(), removedGroups.size(), loadedOffsets.size(), pendingOffsets.size());
        }

        final var groupOffsetsPair = CoreUtils.partition(
                CoreUtils.groupBy(loadedOffsets, GroupTopicPartition::group, GroupTopicPartition::topicPartition),
                loadedGroups::containsKey);
        final var groupOffsets = groupOffsetsPair.get(true);
        final var emptyGroupOffsets = groupOffsetsPair.get(false);

        final var pendingOffsetsByGroup =
                new HashMap<String, Map<Long, Map<TopicPartition, CommitRecordMetadataAndOffset>>>();
        pendingOffsets.forEach((producerId, producerOffsets) -> {
            producerOffsets.keySet().stream().map(GroupTopicPartition::group)
                    .forEach(group -> addProducerGroup(producerId, group));
            groupBy(producerOffsets, GroupTopicPartition::group).forEach((group, offsets) -> {
                final var groupPendingOffsets = pendingOffsetsByGroup.computeIfAbsent(group, __ -> new HashMap<>());
                final var groupProducerOffsets = groupPendingOffsets.computeIfAbsent(producerId, __ -> new HashMap<>());
                offsets.forEach((groupTopicPartition, offset) -> {
                    groupProducerOffsets.put(groupTopicPartition.topicPartition(), offset);
                });
            });
        });

        final var pendingGroupOffsetsPair = CoreUtils.partition(pendingOffsetsByGroup, loadedGroups::containsKey);
        final var pendingGroupOffsets = pendingGroupOffsetsPair.get(true);
        final var pendingEmptyGroupOffsets = pendingGroupOffsetsPair.get(false);

        // load groups which store offsets in kafka, but which have no active members and thus no group
        // metadata stored in the log
        Stream.concat(
                emptyGroupOffsets.keySet().stream(), pendingEmptyGroupOffsets.keySet().stream()).forEach(groupId -> {
            // TODO: add the time field to GroupMetadata, see https://github.com/apache/kafka/pull/4896
            final var group = new GroupMetadata(groupId, GroupState.Empty);
            final var offsets = emptyGroupOffsets.getOrDefault(groupId, Collections.emptyMap());
            final var pendingOffsetsOfTheGroup = pendingEmptyGroupOffsets.getOrDefault(groupId, Collections.emptyMap());
            if (log.isDebugEnabled()) {
                log.debug("Loaded group metadata {} with offsets {} and pending offsets {}",
                        group, offsets, pendingOffsetsOfTheGroup);
            }
            loadGroup(group, offsets, pendingOffsetsOfTheGroup);
            onGroupLoaded.accept(group);
        });

        loadedGroups.values().forEach(group -> {
            final var offsets = groupOffsets.getOrDefault(group.groupId(), Collections.emptyMap());
            final var pendingOffsetsOfTheGroup =
                    pendingGroupOffsets.getOrDefault(group.groupId(), Collections.emptyMap());
            if (log.isDebugEnabled()) {
                log.debug("Loaded group metadata {} with offsets {} and pending offsets {}",
                        group, offsets, pendingOffsetsOfTheGroup);
            }
            loadGroup(group, offsets, pendingOffsetsOfTheGroup);
            onGroupLoaded.accept(group);
        });

        removedGroups.forEach(groupId -> {
            // if the cache already contains a group which should be removed, raise an error. Note that it
            // is possible (however unlikely) for a consumer group to be removed, and then to be used only for
            // offset storage (i.e. by "simple" consumers)
            // TODO: Should we throw an exception here?
            if (groupMetadataCache.containsKey(groupId) && !emptyGroupOffsets.containsKey(groupId)) {
                log.warn("Unexpected unload of active group {} while loading partition {}",
                        groupId, topicPartition);
            }
        });
    }

    private void loadGroup(GroupMetadata group,
                           Map<TopicPartition, CommitRecordMetadataAndOffset> offsets,
                           Map<Long, Map<TopicPartition, CommitRecordMetadataAndOffset>> pendingTransactionalOffsets) {
        // offsets are initialized prior to loading the group into the cache to ensure that clients see a consistent
        // view of the group's offsets
        Map<TopicPartition, CommitRecordMetadataAndOffset> loadedOffsets = CoreUtils.mapValue(
            offsets,
            commitRecordMetadataAndOffset -> {
                OffsetAndMetadata offsetAndMetadata = commitRecordMetadataAndOffset.offsetAndMetadata();
                OffsetAndMetadata updatedOffsetAndMetadata;
                if (offsetAndMetadata.expireTimestamp() == OffsetCommitRequest.DEFAULT_TIMESTAMP) {
                    long expireTimestamp = offsetAndMetadata.commitTimestamp() + offsetConfig.offsetsRetentionMs();
                    updatedOffsetAndMetadata = OffsetAndMetadata.apply(
                        offsetAndMetadata.offset(),
                        offsetAndMetadata.metadata(),
                        offsetAndMetadata.commitTimestamp(),
                        expireTimestamp
                    );
                } else {
                    updatedOffsetAndMetadata = offsetAndMetadata;
                }
                return new CommitRecordMetadataAndOffset(
                    commitRecordMetadataAndOffset.appendedPosition(),
                    updatedOffsetAndMetadata
                );
            }
        );
        if (log.isTraceEnabled()) {
            log.trace("Initialized offsets {} from group {}",
                loadedOffsets, group.groupId());
        }
        group.initializeOffsets(
            loadedOffsets,
            pendingTransactionalOffsets
        );

        GroupMetadata currentGroup = addGroup(group);
        if (group != currentGroup) {
            log.debug("Attempt to load group {} from log with generation {} failed "
                    + "because there is already a cached group with generation {}",
                group.groupId(), group.generationId(), currentGroup.generationId());
        }
    }


    /**
     * When this broker becomes a follower for an offsets topic partition clear out the cache for groups that belong to
     * that partition.
     *
     * @param offsetsPartition Groups belonging to this partition of the offsets topic will be deleted from the cache.
     */
    public void removeGroupsForPartition(int offsetsPartition,
                                         Consumer<GroupMetadata> onGroupUnloaded) {
        TopicPartition topicPartition = new TopicPartition(
            GROUP_METADATA_TOPIC_NAME, offsetsPartition
        );

        if (scheduler.isShutdown()) {
            log.info("Broker is shutting down, skip unloading of offsets and group metadata from {}", topicPartition);
        } else {
            log.info("Scheduling unloading of offsets and group metadata from {}", topicPartition);
            scheduler.submit(() -> removeGroupsAndOffsets(offsetsPartition, onGroupUnloaded));
        }
    }


    @VisibleForTesting
    void removeGroupsAndOffsets(int partition, Consumer<GroupMetadata> onGroupUnloaded) {
        int numOffsetsRemoved = 0;
        int numGroupsRemoved = 0;

        if (log.isDebugEnabled()) {
            log.debug("Started unloading offsets and group metadata for {}", getTopicPartitionName(partition));
        }
        partitionLock.lock();
        try {
            // we need to guard the group removal in cache in the loading partition lock
            // to prevent coordinator's check-and-get-group race condition
            ownedPartitions.remove(partition);
            loadingPartitions.remove(partition);

            for (GroupMetadata group : groupMetadataCache.values()) {
                if (partitionFor(group.groupId()) == partition) {
                    onGroupUnloaded.accept(group);
                    groupMetadataCache.remove(group.groupId(), group);
                    removeGroupFromAllProducers(group.groupId());
                    numGroupsRemoved++;
                    numOffsetsRemoved += group.numOffsets();
                }
            }
        } finally {
            partitionLock.unlock();
        }

        offsetTopic.remove(partition);
        log.info("Finished unloading {}. Removed {} cached offsets and {} cached groups.",
                getTopicPartitionName(partition), numOffsetsRemoved, numGroupsRemoved);
    }

    CompletableFuture<Void> cleanupGroupMetadata() {
        final long startMs = time.milliseconds();
        return cleanGroupMetadata(groupMetadataCache.values().stream(),
            group -> group.removeExpiredOffsets(time.milliseconds())
        ).thenAcceptAsync(offsetsRemoved ->
                log.info("Removed {} expired offsets in {} milliseconds.",
                    offsetsRemoved, time.milliseconds() - startMs)
            , scheduler);
    }

    CompletableFuture<Integer> cleanGroupMetadata(Stream<GroupMetadata> groups,
                                                  Function<GroupMetadata, Map<TopicPartition, OffsetAndMetadata>>
                                                      selector) {
        List<CompletableFuture<Integer>> cleanFutures = groups.map(group -> {
            String groupId = group.groupId();
            Triple<Map<TopicPartition, OffsetAndMetadata>, Boolean, Integer> result = group.inLock(() -> {
                Map<TopicPartition, OffsetAndMetadata> removedOffsets =
                    Collections.synchronizedMap(selector.apply(group));
                if (group.is(GroupState.Empty) && !group.hasOffsets()) {
                    log.info("Group {} transitioned to Dead in generation {}",
                        groupId, group.generationId());
                    group.transitionTo(GroupState.Dead);
                }

                return Triple.of(
                    removedOffsets,
                    group.is(GroupState.Dead),
                    group.generationId()
                );
            });
            Map<TopicPartition, OffsetAndMetadata> removedOffsets = result.getLeft();
            boolean groupIsDead = result.getMiddle();
            int generation = result.getRight();

            TimestampType timestampType = TimestampType.CREATE_TIME;
            long timestamp = time.milliseconds();
            List<SimpleRecord> tombstones = new ArrayList<>();
            removedOffsets.forEach((topicPartition, offsetAndMetadata) -> {
                byte[] commitKey = offsetCommitKey(
                    groupId, topicPartition, namespacePrefix
                );
                tombstones.add(new SimpleRecord(timestamp, commitKey, null));
            });

            // We avoid writing the tombstone when the generationId is 0, since this group is only using
            // Kafka for offset storage.
            if (groupIsDead && groupMetadataCache.remove(groupId, group) && generation > 0) {
                // Append the tombstone messages to the partition. It is okay if the replicas don't receive these (say,
                // if we crash or leaders move) since the new leaders will still expire the consumers with heartbeat and
                // retry removing this group.
                byte[] groupMetadataKey = groupMetadataKey(group.groupId());
                tombstones.add(new SimpleRecord(timestamp, groupMetadataKey, null));
            }

            final CompletableFuture<Integer> future = new CompletableFuture<>();
            if (!tombstones.isEmpty()) {
                MemoryRecords records = MemoryRecords.withRecords(
                    magicValue, 0L, compressionType,
                    timestampType,
                    tombstones.toArray(new SimpleRecord[0])
                );
                byte[] groupKey = groupMetadataKey(
                    group.groupId()
                );
                int partition = partitionFor(groupId);
                storeOffsetMessageAsync(partition, groupKey, records.buffer(), timestamp).whenComplete((msgId, e) -> {
                    if (e == null) {
                        future.complete(removedOffsets.size());
                    } else {
                        log.warn("Failed to append {} tombstones to topic {} for expired/deleted "
                                        + "offsets and/or metadata for group {}: {}",
                                tombstones.size(), getTopicPartitionName(partition), groupId, e.getMessage());
                        future.complete(0);
                    }
                });
            } else {
                future.complete(0);
            }
            return future;
        }).collect(Collectors.toList());
        return FutureUtils.collect(cleanFutures)
            .thenApplyAsync(removedList -> removedList.stream().mapToInt(Integer::intValue).sum(), scheduler);
    }

    /**
     * Complete pending transactional offset commits of the groups of `producerId` from the provided
     * `completedPartitions`. This method is invoked when a commit or abort marker is fully written
     * to the log. It may be invoked when a group lock is held by the caller, for instance when delayed
     * operations are completed while appending offsets for a group. Since we need to acquire one or
     * more group metadata locks to handle transaction completion, this operation is scheduled on
     * the scheduler thread to avoid deadlocks.
     */
    public CompletableFuture<Void> scheduleHandleTxnCompletion(long producerId,
                                                 Set<Integer> completedPartitions,
                                                 boolean isCommit) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        scheduler.submit(() -> handleTxnCompletion(producerId, completedPartitions, isCommit, completableFuture));
        return completableFuture;
    }

    protected void handleTxnCompletion(long producerId, Set<Integer> completedPartitions,
                                       boolean isCommit, CompletableFuture<Void> completableFuture) {
        List<CompletableFuture<Void>> groupFutureList = new ArrayList<>();
        Set<String> pendingGroups = groupsBelongingToPartitions(producerId, completedPartitions);
        pendingGroups.forEach(groupId -> {
            CompletableFuture<Void> groupFuture = new CompletableFuture<>();
            groupFutureList.add(groupFuture);
            getGroup(groupId).map(group -> group.inLock(() -> {
                if (!group.is(GroupState.Dead)) {
                    group.completePendingTxnOffsetCommit(producerId, isCommit);
                    removeProducerGroup(producerId, groupId);
                }
                groupFuture.complete(null);
                return null;
            })).orElseGet(() -> {
                log.info("Group {} has moved away from this coordinator after transaction marker was written"
                        + " but before the cache was updated. The cache on the new group owner will be updated"
                        + " instead.",
                    groupId);
                groupFuture.complete(null);
                return null;
            });
        });
        FutureUtil.waitForAll(groupFutureList).whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                log.error("Failed to handle txn completion.");
                completableFuture.completeExceptionally(throwable);
            } else {
                completableFuture.complete(null);
            }
        });
    }

    /**
     * Add the partition into the owned list.
     *
     * <p>NOTE: this is for test only
     */
    void addPartitionOwnership(int partition) {
        inLock(partitionLock, () -> {
            ownedPartitions.add(partition);
            return null;
        });
    }

    /**
     * Add a partition to the loading partitions set. Return true if the partition was not
     * already loading.
     *
     * <p>Visible for testing
     */
    boolean addLoadingPartition(int partition) {
        return inLock(partitionLock, () -> {
            if (ownedPartitions.contains(partition)) {
                return false;
            } else {
                return loadingPartitions.add(partition);
            }
        });
    }
}
