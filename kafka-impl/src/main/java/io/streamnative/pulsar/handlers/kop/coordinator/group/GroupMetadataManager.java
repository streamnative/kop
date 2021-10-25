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

import static com.google.common.base.Preconditions.checkArgument;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.CURRENT_GROUP_VALUE_SCHEMA_VERSION;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.groupMetadataKey;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.groupMetadataValue;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.offsetCommitKey;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.offsetCommitValue;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.readGroupMessageValue;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.readMessageKey;
import static io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataConstants.readOffsetMessageValue;
import static io.streamnative.pulsar.handlers.kop.utils.CoreUtils.inLock;
import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;
import static org.apache.pulsar.common.naming.TopicName.PARTITIONED_TOPIC_SUFFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadata.CommitRecordMetadataAndOffset;
import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import io.streamnative.pulsar.handlers.kop.utils.CoreUtils;
import io.streamnative.pulsar.handlers.kop.utils.MessageIdUtils;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.MathUtils;
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
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse.PartitionData;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Manager to manage a coordination group.
 */
@Slf4j
public class GroupMetadataManager {

    /**
     * The key interface.
     */
    interface BaseKey {
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
    static class OffsetKey implements BaseKey {

        private final short version;
        private final GroupTopicPartition key;

        @Override
        public String toString() {
            return key.toString();
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

    private final byte magicValue = RecordBatch.CURRENT_MAGIC_VALUE;
    private final CompressionType compressionType;
    @Getter
    private final OffsetConfig offsetConfig;
    private final String tenant;
    private final ConcurrentMap<String, GroupMetadata> groupMetadataCache;
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
    private final int groupMetadataTopicPartitionCount;

    // Map of <PartitionId, Producer>
    private final ConcurrentMap<Integer, CompletableFuture<Producer<ByteBuffer>>> offsetsProducers =
        new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, CompletableFuture<Reader<ByteBuffer>>> offsetsReaders =
        new ConcurrentHashMap<>();

    /* single-thread scheduler to handle offset/group metadata cache loading and unloading */
    private final ScheduledExecutorService scheduler;
    /**
     * The groups with open transactional offsets commits per producer. We need this because when the commit or abort
     * marker comes in for a transaction, it is for a particular partition on the offsets topic and a particular
     * producerId. We use this structure to quickly find the groups which need to be updated by the commit/abort
     * marker.
     */
    private final Map<Long, Set<String>> openGroupsForProducer = new HashMap<>();

    private final ProducerBuilder<ByteBuffer> metadataTopicProducerBuilder;
    private final ReaderBuilder<ByteBuffer> metadataTopicReaderBuilder;
    private final Time time;
    private final Function<String, Integer> partitioner;

    public GroupMetadataManager(String tenant,
                                OffsetConfig offsetConfig,
                                ProducerBuilder<ByteBuffer> metadataTopicProducerBuilder,
                                ReaderBuilder<ByteBuffer> metadataTopicReaderBuilder,
                                ScheduledExecutorService scheduler,
                                Time time) {
        this(tenant,
            offsetConfig,
            metadataTopicProducerBuilder,
            metadataTopicReaderBuilder,
            scheduler,
            time,
            // Be same with kafka: abs(groupId.hashCode) % groupMetadataTopicPartitionCount
            // return a partitionId
            groupId -> getPartitionId(groupId, offsetConfig.offsetsTopicNumPartitions())
        );
    }

    public static int getPartitionId(String groupId, int offsetsTopicNumPartitions) {
        return MathUtils.signSafeMod(groupId.hashCode(), offsetsTopicNumPartitions);
    }

    GroupMetadataManager(String tenant,
                         OffsetConfig offsetConfig,
                         ProducerBuilder<ByteBuffer> metadataTopicProducerBuilder,
                         ReaderBuilder<ByteBuffer> metadataTopicConsumerBuilder,
                         ScheduledExecutorService scheduler,
                         Time time,
                         Function<String, Integer> partitioner) {
        this.tenant = tenant;
        this.offsetConfig = offsetConfig;
        this.compressionType = offsetConfig.offsetsTopicCompressionType();
        this.groupMetadataCache = new ConcurrentHashMap<>();
        this.groupMetadataTopicPartitionCount = offsetConfig.offsetsTopicNumPartitions();
        this.metadataTopicProducerBuilder = metadataTopicProducerBuilder;
        this.metadataTopicReaderBuilder = metadataTopicConsumerBuilder;
        this.scheduler = scheduler;
        this.time = time;
        this.partitioner = partitioner;
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
        List<CompletableFuture<Void>> producerCloses = offsetsProducers.values().stream()
            .map(producerCompletableFuture -> producerCompletableFuture
                    .thenComposeAsync(Producer::closeAsync, scheduler))
            .collect(Collectors.toList());
        offsetsProducers.clear();
        List<CompletableFuture<Void>> readerCloses = offsetsReaders.values().stream()
            .map(readerCompletableFuture -> readerCompletableFuture
                    .thenComposeAsync(Reader::closeAsync, scheduler))
            .collect(Collectors.toList());
        offsetsReaders.clear();

        FutureUtil.waitForAll(producerCloses).whenCompleteAsync((ignore, t) -> {
            if (t != null) {
                log.error("Error when close all the {} offsetsProducers in GroupMetadataManager",
                    producerCloses.size(), t);
            }
            if (log.isDebugEnabled()) {
                log.debug("Closed all the {} offsetsProducers in GroupMetadataManager", producerCloses.size());
            }
        }, scheduler);

        FutureUtil.waitForAll(readerCloses).whenCompleteAsync((ignore, t) -> {
            if (t != null) {
                log.error("Error when close all the {} offsetsReaders in GroupMetadataManager",
                    readerCloses.size(), t);
            }
            if (log.isDebugEnabled()) {
                log.debug("Closed all the {} offsetsReaders in GroupMetadataManager.", readerCloses.size());
            }
        }, scheduler);
        scheduler.shutdown();
    }

    public ConcurrentMap<Integer, CompletableFuture<Producer<ByteBuffer>>> getOffsetsProducers() {
        return offsetsProducers;
    }
    public ConcurrentMap<Integer, CompletableFuture<Reader<ByteBuffer>>> getOffsetsReaders() {
        return offsetsReaders;
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
        return partitioner.apply(groupId);
    }

    public String getTopicPartitionName() {
        return offsetConfig.getCurrentOffsetsTopicName(tenant);
    }

    public String getTopicPartitionName(int partitionId) {
        return getTopicPartitionName(offsetConfig.getCurrentOffsetsTopicName(tenant), partitionId);
    }

    public static String getTopicPartitionName(String offsetsTopicName, int partitionId) {
        return offsetsTopicName + PARTITIONED_TOPIC_SUFFIX + partitionId;
    }

    public int getGroupMetadataTopicPartitionCount() {
        return groupMetadataTopicPartitionCount;
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

    boolean isGroupOpenForProducer(long producerId,
                                   String groupId) {
        return openGroupsForProducer.getOrDefault(
            producerId, Collections.emptySet()
        ).contains(groupId);
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

        return getOffsetsTopicProducer(group.groupId())
            .thenComposeAsync(f -> f.newMessage()
                    .keyBytes(key)
                    .value(records.buffer())
                    .eventTime(timestamp).sendAsync()
                , scheduler)
            .thenApplyAsync(msgId -> {
                if (!isGroupLocal(group.groupId())) {
                    if (log.isDebugEnabled()) {
                        log.warn("add partition ownership for group {}",
                            group.groupId());
                    }
                    addPartitionOwnership(partitionFor(group.groupId()));
                }
                return Errors.NONE;
            }, scheduler)
            .exceptionally(cause -> Errors.COORDINATOR_NOT_AVAILABLE);
    }

    // visible for mock
    CompletableFuture<MessageId> storeOffsetMessage(String groupId,
                                                    byte[] key,
                                                    ByteBuffer buffer,
                                                    long timestamp) {
        return getOffsetsTopicProducer(groupId)
            .thenComposeAsync(f -> f.newMessage()
                    .keyBytes(key)
                    .value(buffer)
                    .eventTime(timestamp).sendAsync()
                , scheduler);
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
                    e -> e.getKey(),
                    e -> e.getValue()
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
                    e -> e.getKey(),
                    e -> Errors.OFFSET_METADATA_TOO_LARGE
                ));
            return CompletableFuture.completedFuture(commitStatus);
        }

        TimestampType timestampType = TimestampType.CREATE_TIME;
        long timestamp = time.milliseconds();
        List<SimpleRecord> records = filteredOffsetMetadata.entrySet().stream()
            .map(e -> {
                byte[] key = offsetCommitKey(group.groupId(), e.getKey());
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
        byte[] key = offsetCommitKey(group.groupId(), new TopicPartition("", -1));
        return storeOffsetMessage(group.groupId(), key, entries.buffer(), timestamp)
            .thenApplyAsync(messageId -> {
                if (!group.is(GroupState.Dead)) {
                    MessageIdImpl lastMessageId = (MessageIdImpl) messageId;
                    long baseOffset = MessageIdUtils.getMockOffset(
                        lastMessageId.getLedgerId(),
                        lastMessageId.getEntryId()
                    );
                    filteredOffsetMetadata.forEach((tp, offsetAndMetadata) -> {
                        CommitRecordMetadataAndOffset commitRecordMetadataAndOffset =
                            new CommitRecordMetadataAndOffset(
                                Optional.of(baseOffset),
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
            }, scheduler)
            .exceptionally(cause -> {
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

                if (log.isDebugEnabled()) {
                    log.debug("Offset commit {} from group {}, consumer {} with generation {} failed"
                            + " when appending to log due to ",
                        filteredOffsetMetadata, group.groupId(), consumerId, group.generationId(), cause);
                }

                return Errors.UNKNOWN_SERVER_ERROR;
            })
            .thenApplyAsync(errors -> offsetMetadata.entrySet()
                .stream()
                .collect(Collectors.toMap(
                    e -> e.getKey(),
                    e -> {
                        if (validateOffsetMetadataLength(e.getValue().metadata())) {
                            return errors;
                        } else {
                            return Errors.OFFSET_METADATA_TOO_LARGE;
                        }
                    }
                )), scheduler);
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
                    tp -> new PartitionData(
                        OffsetFetchResponse.INVALID_OFFSET,
                        "",
                        Errors.NONE
                    )
                ));
        }

        return group.inLock(() -> {
            if (group.is(GroupState.Dead)) {
                return topicPartitionsOpt.orElse(Collections.emptyList())
                    .stream()
                    .collect(Collectors.toMap(
                        tp -> tp,
                        tp -> new PartitionData(
                            OffsetFetchResponse.INVALID_OFFSET,
                            "",
                            Errors.NONE
                        )
                    ));
            }

            return topicPartitionsOpt.map(topicPartitions ->
                topicPartitions.stream()
                    .collect(Collectors.toMap(
                        tp -> tp,
                        topicPartition ->
                            group.offset(topicPartition)
                                .map(offsetAndMetadata -> new PartitionData(
                                    offsetAndMetadata.offset(),
                                    offsetAndMetadata.metadata(),
                                    Errors.NONE
                                ))
                                .orElseGet(() -> new PartitionData(
                                    OffsetFetchResponse.INVALID_OFFSET,
                                    "",
                                    Errors.NONE
                                ))))
            ).orElseGet(() ->
                group.allOffsets().entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                        e -> e.getKey(),
                        e -> {
                            OffsetAndMetadata oam = e.getValue();
                            return new PartitionData(
                                oam.offset(),
                                oam.metadata(),
                                Errors.NONE
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
                .remove(producerId);
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
        String topicPartition = offsetConfig.getCurrentOffsetsTopicName(tenant) + PARTITIONED_TOPIC_SUFFIX + offsetsPartition;
        if (addLoadingPartition(offsetsPartition)) {
            log.info("Scheduling loading of offsets and group metadata from {}", topicPartition);
            long startMs = time.milliseconds();
            return getOffsetsTopicProducer(offsetsPartition)
                .thenComposeAsync(f -> f.newMessage()
                        .value(ByteBuffer.allocate(0))
                        .eventTime(time.milliseconds()).sendAsync()
                    , scheduler)
                .thenComposeAsync(lastMessageId -> {
                    if (log.isTraceEnabled()) {
                        log.trace("Successfully write a placeholder record into {} @ {}",
                            topicPartition, lastMessageId);
                    }
                    return doLoadGroupsAndOffsets(getOffsetsTopicReader(offsetsPartition),
                        lastMessageId, onGroupLoaded);
                }, scheduler)
                .whenCompleteAsync((ignored, cause) -> {
                    if (null != cause) {
                        log.error("Error loading offsets from {}", topicPartition, cause);
                        removeLoadingPartition(offsetsPartition);
                        return;
                    }
                    log.info("Finished loading offsets and group metadata from {} in {} milliseconds",
                        topicPartition, time.milliseconds() - startMs);
                    inLock(partitionLock, () -> {
                        ownedPartitions.add(offsetsPartition);
                        loadingPartitions.remove(offsetsPartition);
                        return null;
                    });
                }, scheduler);
        } else {
            log.info("Already loading offsets and group metadata from {}", topicPartition);
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> doLoadGroupsAndOffsets(
        CompletableFuture<Reader<ByteBuffer>> metadataConsumer,
        MessageId endMessageId,
        Consumer<GroupMetadata> onGroupLoaded
    ) {
        final Map<GroupTopicPartition, CommitRecordMetadataAndOffset> loadedOffsets = new HashMap<>();
        final Map<Long, Map<GroupTopicPartition, CommitRecordMetadataAndOffset>> pendingOffsets = new HashMap<>();
        final Map<String, GroupMetadata> loadedGroups = new HashMap<>();
        final Set<String> removedGroups = new HashSet<>();
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        loadNextMetadataMessage(
            metadataConsumer,
            endMessageId,
            resultFuture,
            onGroupLoaded,
            loadedOffsets,
            pendingOffsets,
            loadedGroups,
            removedGroups);

        return resultFuture;
    }

    private void loadNextMetadataMessage(CompletableFuture<Reader<ByteBuffer>> metadataConsumer,
                                         MessageId endMessageId,
                                         CompletableFuture<Void> resultFuture,
                                         Consumer<GroupMetadata> onGroupLoaded,
                                         Map<GroupTopicPartition, CommitRecordMetadataAndOffset> loadedOffsets,
                                         Map<Long, Map<GroupTopicPartition, CommitRecordMetadataAndOffset>>
                                             pendingOffsets,
                                         Map<String, GroupMetadata> loadedGroups,
                                         Set<String> removedGroups) {
        try {
            unsafeLoadNextMetadataMessage(
                metadataConsumer,
                endMessageId,
                resultFuture,
                onGroupLoaded,
                loadedOffsets,
                pendingOffsets,
                loadedGroups,
                removedGroups
            );
        } catch (Throwable cause) {
            log.error("Unknown exception caught when loading group and offsets from topic",
                cause);
            resultFuture.completeExceptionally(cause);
        }
    }

    private void unsafeLoadNextMetadataMessage(CompletableFuture<Reader<ByteBuffer>> metadataConsumer,
                                               MessageId endMessageId,
                                               CompletableFuture<Void> resultFuture,
                                               Consumer<GroupMetadata> onGroupLoaded,
                                               Map<GroupTopicPartition, CommitRecordMetadataAndOffset> loadedOffsets,
                                               Map<Long, Map<GroupTopicPartition, CommitRecordMetadataAndOffset>>
                                                   pendingOffsets,
                                               Map<String, GroupMetadata> loadedGroups,
                                               Set<String> removedGroups) {
        if (shuttingDown.get()) {
            resultFuture.completeExceptionally(
                new Exception("Group metadata manager is shutting down"));
            return;
        }

        if (log.isTraceEnabled()) {
            log.trace("Reading the next metadata message from topic {}",
                metadataConsumer.join().getTopic());
        }

        BiConsumer<Message<ByteBuffer>, Throwable> readNextComplete = (message, cause) -> {
            if (log.isTraceEnabled()) {
                log.trace("Metadata consumer received a metadata message from {} @ {}",
                    metadataConsumer.join().getTopic(), message.getMessageId());
            }

            if (null != cause) {
                resultFuture.completeExceptionally(cause);
                return;
            }

            if (message.getMessageId().compareTo(endMessageId) >= 0) {
                // reach the end of partition
                processLoadedAndRemovedGroups(
                    resultFuture,
                    onGroupLoaded,
                    loadedOffsets,
                    pendingOffsets,
                    loadedGroups,
                    removedGroups
                );
                return;
            }

            if (!message.hasKey()) {
                // the messages without key are placeholders
                loadNextMetadataMessage(
                    metadataConsumer,
                    endMessageId,
                    resultFuture,
                    onGroupLoaded,
                    loadedOffsets,
                    pendingOffsets,
                    loadedGroups,
                    removedGroups
                );
                return;
            }

            ByteBuffer buffer = message.getValue();
            MemoryRecords memRecords = MemoryRecords.readableRecords(buffer);

            memRecords.batches().forEach(batch -> {
                boolean isTxnOffsetCommit = batch.isTransactional();
                if (batch.isControlBatch()) {
                    Iterator<Record> recordIterator = batch.iterator();
                    if (recordIterator.hasNext()) {
                        Record record = recordIterator.next();
                        ControlRecordType controlRecord = ControlRecordType.parse(record.key());
                        if (controlRecord == ControlRecordType.COMMIT) {
                            pendingOffsets.getOrDefault(batch.producerId(), Collections.emptyMap())
                                .forEach((groupTopicPartition, commitRecordMetadataAndOffset) -> {
                                    if (!loadedOffsets.containsKey(groupTopicPartition)
                                        || loadedOffsets.get(groupTopicPartition)
                                        .olderThan(commitRecordMetadataAndOffset)) {
                                        loadedOffsets.put(groupTopicPartition, commitRecordMetadataAndOffset);
                                    }
                                });
                        }
                        pendingOffsets.remove(batch.producerId());
                    }
                } else {
                    Optional<Long> batchBaseOffset = Optional.empty();
                    for (Record record : batch) {
                        checkArgument(record.hasKey(), "Group metadata/offset entry key should not be null");
                        if (!batchBaseOffset.isPresent()) {
                            batchBaseOffset = Optional.of(record.offset());
                        }
                        BaseKey bk = readMessageKey(record.key());

                        if (log.isTraceEnabled()) {
                            log.trace("Applying metadata record {} received from {}",
                                bk, metadataConsumer.join().getTopic());
                        }

                        if (bk instanceof OffsetKey) {
                            OffsetKey offsetKey = (OffsetKey) bk;
                            if (isTxnOffsetCommit && !pendingOffsets.containsKey(batch.producerId())) {
                                pendingOffsets.put(
                                    batch.producerId(),
                                    new HashMap<>()
                                );
                            }
                            // load offset
                            GroupTopicPartition groupTopicPartition = offsetKey.key();
                            if (!record.hasValue()) {
                                if (isTxnOffsetCommit) {
                                    pendingOffsets.get(batch.producerId()).remove(groupTopicPartition);
                                } else {
                                    loadedOffsets.remove(groupTopicPartition);
                                }
                            } else {
                                OffsetAndMetadata offsetAndMetadata = readOffsetMessageValue(record.value());
                                CommitRecordMetadataAndOffset commitRecordMetadataAndOffset =
                                    new CommitRecordMetadataAndOffset(
                                        batchBaseOffset,
                                        offsetAndMetadata
                                    );
                                if (isTxnOffsetCommit) {
                                    pendingOffsets.get(batch.producerId()).put(
                                        groupTopicPartition,
                                        commitRecordMetadataAndOffset);
                                } else {
                                    loadedOffsets.put(
                                        groupTopicPartition,
                                        commitRecordMetadataAndOffset
                                    );
                                }
                            }
                        } else if (bk instanceof GroupMetadataKey) {
                            GroupMetadataKey groupMetadataKey = (GroupMetadataKey) bk;
                            String gid = groupMetadataKey.key();
                            GroupMetadata gm = readGroupMessageValue(gid, record.value());
                            if (gm != null) {
                                removedGroups.remove(gid);
                                loadedGroups.put(gid, gm);
                            } else {
                                loadedGroups.remove(gid);
                                removedGroups.add(gid);
                            }
                        } else {
                            resultFuture.completeExceptionally(
                                new IllegalStateException(
                                    "Unexpected message key " + bk + " while loading offsets and group metadata"));
                            return;
                        }
                    }
                }

            });

            loadNextMetadataMessage(
                metadataConsumer,
                endMessageId,
                resultFuture,
                onGroupLoaded,
                loadedOffsets,
                pendingOffsets,
                loadedGroups,
                removedGroups
            );
        };

        metadataConsumer.thenComposeAsync(r -> r.readNextAsync()).whenCompleteAsync((message, cause) -> {
            try {
                readNextComplete.accept(message, cause);
            } catch (Throwable completeCause) {
                log.error("Unknown exception caught when processing the received metadata message from topic {}",
                    metadataConsumer.join().getTopic(), completeCause);
                resultFuture.completeExceptionally(completeCause);
            }
        }, scheduler);
    }

    private void processLoadedAndRemovedGroups(CompletableFuture<Void> resultFuture,
                                               Consumer<GroupMetadata> onGroupLoaded,
                                               Map<GroupTopicPartition, CommitRecordMetadataAndOffset> loadedOffsets,
                                               Map<Long, Map<GroupTopicPartition, CommitRecordMetadataAndOffset>>
                                                   pendingOffsets,
                                               Map<String, GroupMetadata> loadedGroups,
                                               Set<String> removedGroups) {
        if (log.isTraceEnabled()) {
            log.trace("Completing loading : {} loaded groups, {} removed groups, {} loaded offsets, {} pending offsets",
                loadedGroups.size(), removedGroups.size(), loadedOffsets.size(), pendingOffsets.size());
        }
        try {
            Map<String, Map<TopicPartition, CommitRecordMetadataAndOffset>> groupLoadedOffsets =
                loadedOffsets.entrySet().stream()
                    .collect(Collectors.groupingBy(
                        e -> e.getKey().group(),
                        Collectors.toMap(
                            f -> f.getKey().topicPartition(),
                            f -> f.getValue()
                        ))
                    );
            Map<Boolean, Map<String, Map<TopicPartition, CommitRecordMetadataAndOffset>>> partitionedLoadedOffsets =
                CoreUtils.partition(groupLoadedOffsets, group -> loadedGroups.containsKey(group));
            Map<String, Map<TopicPartition, CommitRecordMetadataAndOffset>> groupOffsets =
                partitionedLoadedOffsets.get(true);
            Map<String, Map<TopicPartition, CommitRecordMetadataAndOffset>> emptyGroupOffsets =
                partitionedLoadedOffsets.get(false);

            Map<String, Map<Long, Map<TopicPartition, CommitRecordMetadataAndOffset>>> pendingOffsetsByGroup =
                new HashMap<>();
            pendingOffsets.forEach((producerId, producerOffsets) -> {
                producerOffsets.keySet().stream()
                    .map(GroupTopicPartition::group)
                    .forEach(group -> addProducerGroup(producerId, group));
                producerOffsets
                    .entrySet()
                    .stream()
                    .collect(Collectors.groupingBy(
                        e -> e.getKey().group,
                        Collectors.toMap(
                            f -> f.getKey().topicPartition(),
                            f -> f.getValue()
                        )
                    ))
                    .forEach((group, offsets) -> {
                        Map<Long, Map<TopicPartition, CommitRecordMetadataAndOffset>> groupPendingOffsets =
                            pendingOffsetsByGroup.computeIfAbsent(
                                group,
                                g -> new HashMap<>());
                        Map<TopicPartition, CommitRecordMetadataAndOffset> groupProducerOffsets =
                            groupPendingOffsets.computeIfAbsent(
                                producerId,
                                p -> new HashMap<>());
                        groupProducerOffsets.putAll(offsets);
                    });
            });

            Map<Boolean, Map<String, Map<Long, Map<TopicPartition, CommitRecordMetadataAndOffset>>>>
                partitionedPendingOffsetsByGroup = CoreUtils.partition(
                pendingOffsetsByGroup,
                group -> loadedGroups.containsKey(group)
            );
            Map<String, Map<Long, Map<TopicPartition, CommitRecordMetadataAndOffset>>> pendingGroupOffsets =
                partitionedPendingOffsetsByGroup.get(true);
            Map<String, Map<Long, Map<TopicPartition, CommitRecordMetadataAndOffset>>> pendingEmptyGroupOffsets =
                partitionedPendingOffsetsByGroup.get(false);

            loadedGroups.values().forEach(group -> {
                Map<TopicPartition, CommitRecordMetadataAndOffset> offsets =
                    groupOffsets.getOrDefault(group.groupId(), Collections.emptyMap());
                Map<Long, Map<TopicPartition, CommitRecordMetadataAndOffset>> pOffsets =
                    pendingGroupOffsets.getOrDefault(group.groupId(), Collections.emptyMap());

                if (log.isDebugEnabled()) {
                    log.debug("Loaded group metadata {} with offsets {} and pending offsets {}",
                        group, offsets, pOffsets);
                }

                loadGroup(group, offsets, pOffsets);
                onGroupLoaded.accept(group);
            });

            Sets.union(
                emptyGroupOffsets.keySet(),
                pendingEmptyGroupOffsets.keySet()
            ).forEach(groupId -> {
                GroupMetadata group = new GroupMetadata(groupId, GroupState.Empty);
                Map<TopicPartition, CommitRecordMetadataAndOffset> offsets =
                    emptyGroupOffsets.getOrDefault(groupId, Collections.emptyMap());
                Map<Long, Map<TopicPartition, CommitRecordMetadataAndOffset>> pOffsets =
                    pendingEmptyGroupOffsets.getOrDefault(groupId, Collections.emptyMap());
                if (log.isDebugEnabled()) {
                    log.debug("Loaded group metadata {} with offsets {} and pending offsets {}",
                        group, offsets, pOffsets);
                }
                loadGroup(group, offsets, pOffsets);
                onGroupLoaded.accept(group);
            });

            removedGroups.forEach(groupId -> {
                // if the cache already contains a group which should be removed, raise an error. Note that it
                // is possible (however unlikely) for a consumer group to be removed, and then to be used only for
                // offset storage (i.e. by "simple" consumers)
                if (groupMetadataCache.containsKey(groupId)
                    && !emptyGroupOffsets.containsKey(groupId)) {
                    throw new IllegalStateException("Unexpected unload of active group " + groupId
                        + " while loading partition");
                }
            });
            resultFuture.complete(null);
        } catch (RuntimeException re) {
            resultFuture.completeExceptionally(re);
        }
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
                    commitRecordMetadataAndOffset.appendedBatchOffset(),
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
        log.info("removeGroupsForPartition {}", topicPartition);
        log.info("Scheduling unloading of offsets and group metadata from {}", topicPartition);
        scheduler.submit(() -> {
            AtomicInteger numOffsetsRemoved = new AtomicInteger();
            AtomicInteger numGroupsRemoved = new AtomicInteger();
            inLock(partitionLock, () -> {
                // we need to guard the group removal in cache in the loading partition lock
                // to prevent coordinator's check-and-get-group race condition
                ownedPartitions.remove(offsetsPartition);

                for (GroupMetadata group : groupMetadataCache.values()) {
                    if (partitionFor(group.groupId()) == offsetsPartition) {
                        onGroupUnloaded.accept(group);
                        groupMetadataCache.remove(group.groupId(), group);
                        removeGroupFromAllProducers(group.groupId());
                        numGroupsRemoved.incrementAndGet();
                        numOffsetsRemoved.addAndGet(group.numOffsets());
                    }
                }

                // remove related producers and readers
                CompletableFuture<Producer<ByteBuffer>> producer = offsetsProducers.remove(offsetsPartition);
                CompletableFuture<Reader<ByteBuffer>> reader = offsetsReaders.remove(offsetsPartition);
                if (producer != null) {
                    producer.thenApplyAsync(p -> p.closeAsync()).whenCompleteAsync((ignore, t) -> {
                        if (t != null) {
                            log.error("Failed to close producer when remove partition {}.",
                                producer.join().getTopic());
                        }
                    }, scheduler);
                }
                if (reader != null) {
                    reader.thenApplyAsync(p -> p.closeAsync()).whenCompleteAsync((ignore, t) -> {
                        if (t != null) {
                            log.error("Failed to close reader when remove partition {}.",
                                reader.join().getTopic());
                        }
                    }, scheduler);
                }

                return null;
            });

            log.info("Finished unloading {}. Removed {} cached offsets and {} cached groups.",
                topicPartition, numOffsetsRemoved, numGroupsRemoved);
        });
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
                    groupId, topicPartition
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

            if (!tombstones.isEmpty()) {
                MemoryRecords records = MemoryRecords.withRecords(
                    magicValue, 0L, compressionType,
                    timestampType,
                    tombstones.toArray(new SimpleRecord[tombstones.size()])
                );
                byte[] groupKey = groupMetadataKey(
                    group.groupId()
                );
                return getOffsetsTopicProducer(group.groupId())
                    .thenComposeAsync(f -> f.newMessage()
                        .keyBytes(groupKey)
                        .value(records.buffer())
                        .eventTime(timestamp).sendAsync(), scheduler)
                    .thenApplyAsync(ignored -> removedOffsets.size(), scheduler)
                    .exceptionally(cause -> {
                        log.error("Failed to append {} tombstones to topic {} for expired/deleted "
                                + "offsets and/or metadata for group {}",
                            tombstones.size(),
                                offsetConfig.getCurrentOffsetsTopicName(tenant) + '-' + partitioner.apply(group.groupId()),
                            group.groupId(), cause);
                        // ignore and continue
                        return 0;
                    });
            } else {
                return CompletableFuture.completedFuture(0);
            }
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
            getGroup(groupId).map(group -> {
                return group.inLock(() -> {
                    if (!group.is(GroupState.Dead)) {
                        group.completePendingTxnOffsetCommit(producerId, isCommit);
                        removeProducerGroup(producerId, groupId);
                    }
                    groupFuture.complete(null);
                    return null;
                });
            }).orElseGet(() -> {
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
        return inLock(partitionLock, () -> loadingPartitions.add(partition));
    }

    /**
     * Remove a partition to the loading partitions set. Return true if the partition is removed.
     *
     * <p>Visible for testing
     */
    @VisibleForTesting
    public boolean removeLoadingPartition(int partition) {
        return inLock(partitionLock, () -> loadingPartitions.remove(partition));
    }

    CompletableFuture<Producer<ByteBuffer>> getOffsetsTopicProducer(String groupId) {
        return offsetsProducers.computeIfAbsent(partitionFor(groupId),
            partitionId -> {
                if (log.isDebugEnabled()) {
                    log.debug("Created Partitioned producer: {} for consumer group: {}",
                            offsetConfig.getCurrentOffsetsTopicName(tenant) + PARTITIONED_TOPIC_SUFFIX + partitionId,
                        groupId);
                }
                return metadataTopicProducerBuilder.clone()
                    .topic(offsetConfig.getCurrentOffsetsTopicName(tenant) + PARTITIONED_TOPIC_SUFFIX + partitionId)
                    .createAsync();
            });
    }

    CompletableFuture<Producer<ByteBuffer>> getOffsetsTopicProducer(int partitionId) {
        return offsetsProducers.computeIfAbsent(partitionId,
            id -> {
                if (log.isDebugEnabled()) {
                    log.debug("Will create Partitioned producer: {}",
                            offsetConfig.getCurrentOffsetsTopicName(tenant) + PARTITIONED_TOPIC_SUFFIX + id);
                }
                return metadataTopicProducerBuilder.clone()
                    .topic(offsetConfig.getCurrentOffsetsTopicName(tenant) + PARTITIONED_TOPIC_SUFFIX + id)
                    .createAsync();
            });
    }

    CompletableFuture<Reader<ByteBuffer>> getOffsetsTopicReader(int partitionId) {
        return offsetsReaders.computeIfAbsent(partitionId,
            id -> {
                if (log.isDebugEnabled()) {
                    log.debug("Will create Partitioned reader: {}",
                            offsetConfig.getCurrentOffsetsTopicName(tenant) + PARTITIONED_TOPIC_SUFFIX + id);
                }
                return metadataTopicReaderBuilder.clone()
                    .topic(offsetConfig.getCurrentOffsetsTopicName(tenant) + PARTITIONED_TOPIC_SUFFIX + partitionId)
                    .readCompacted(true)
                    .createAsync();
            });
    }
}
