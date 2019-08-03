package io.streamnative.kop.coordinator.group;

import static io.streamnative.kop.coordinator.group.GroupMetadataConstants.CURRENT_GROUP_VALUE_SCHEMA_VERSION;
import static io.streamnative.kop.coordinator.group.GroupMetadataConstants.groupMetadataKey;
import static io.streamnative.kop.coordinator.group.GroupMetadataConstants.groupMetadataValue;
import static io.streamnative.kop.coordinator.group.GroupMetadataConstants.readGroupMessageValue;
import static io.streamnative.kop.coordinator.group.GroupMetadataConstants.readMessageKey;
import static io.streamnative.kop.utils.CoreUtils.inLock;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.hash.Murmur3;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.ApiVersionsResponse.ApiVersion;
import org.apache.kafka.common.utils.Time;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;

/**
 * Manager to manage a coordination group.
 */
@Slf4j
class GroupMetadataManager {

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

    private final int brokerId;
    private final ApiVersion interBrokerProtocolVersion;
    private final OffsetConfig config;
    private final CompressionType compressionType;
    private final ConcurrentMap<String, GroupMetadata> groupMetadataCache;
    /* lock protecting access to loading and owned partition sets */
    private final ReentrantLock partitionLock = new ReentrantLock();
    /**
     * partitions of consumer groups that are being loaded, its lock should
     * be always called BEFORE the group lock if needed
     */
    private final Set<Integer> loadingPartitions = new HashSet<>();
    /* partitions of consumer groups that are assigned, using the same loading partition lock */
    private final Set<Integer> ownedPartitions = new HashSet<>();
    /* shutting down flag */
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final String logIdent;
    private final int groupMetadataTopicPartitionCount;
    private final Producer<byte[]> metadataTopicProducer;
    private final Reader<byte[]> metadataTopicReader;
    private final Time time;

    GroupMetadataManager(int brokerId,
                         ApiVersion interBrokerProtocolVersion,
                         OffsetConfig config,
                         int groupMetadataTopicPartitionCount,
                         Producer<byte[]> metadataTopicProducer,
                         Reader<byte[]> metadataTopicConsumer,
                         Time time) {
        this.brokerId = brokerId;
        this.interBrokerProtocolVersion = interBrokerProtocolVersion;
        this.config = config;
        this.compressionType = config.offsetsTopicCompressionType();
        this.groupMetadataCache = new ConcurrentHashMap<>();
        this.logIdent = String.format("[GroupMetadataManager brokerId=%d]", brokerId);
        this.groupMetadataTopicPartitionCount = groupMetadataTopicPartitionCount;
        this.metadataTopicProducer = metadataTopicProducer;
        this.metadataTopicReader = metadataTopicConsumer;
        this.time = time;
    }

    public Iterable<GroupMetadata> currentGroups() {
        return groupMetadataCache.values();
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
        return MathUtils.signSafeMod(
            Murmur3.hash32(groupId.getBytes(UTF_8)),
            groupMetadataTopicPartitionCount
        );
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

    // return true iff group is owned and the group doesn't exist
    public boolean groupNotExists(String groupId) {
        return inLock(
            partitionLock,
            () -> {
                if (isGroupLocal(groupId)) {
                    return true;
                } else {
                    return getGroup(groupId)
                        .map(metadata -> metadata.inLock(() -> metadata.is(GroupState.Dead)))
                        .orElse(false);
                }
            }
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
        long timestamp = time.milliseconds();
        byte[] key = groupMetadataKey(group.groupId());
        byte[] value = groupMetadataValue(
            group, groupAssignment, CURRENT_GROUP_VALUE_SCHEMA_VERSION);

        return metadataTopicProducer.newMessage()
            .keyBytes(key)
            .value(value)
            .eventTime(timestamp)
            .sendAsync()
            .thenApply(msgId -> Errors.NONE)
            .exceptionally(cause -> Errors.COORDINATOR_NOT_AVAILABLE);
    }

    public CompletableFuture<Void> scheduleLoadGroupAndOffsets(int offsetsPartition,
                                                               Consumer<GroupMetadata> onGroupLoaded) {
        TopicPartition topicPartition = new TopicPartition(
            GROUP_METADATA_TOPIC_NAME, offsetsPartition
        );
        if (addLoadingPartition(offsetsPartition)) {
            log.info("Scheduling loading of offsets and group metadata from {}", topicPartition);
            long startMs = time.milliseconds();
            return metadataTopicProducer.newMessage()
                .value(new byte[0])
                .eventTime(time.milliseconds())
                .sendAsync()
                .thenCompose(lastMessageId ->
                    doLoadGroupsAndOffsets(metadataTopicReader, lastMessageId, onGroupLoaded))
                .whenComplete((ignored, cause) -> {
                    if (null == cause) {
                        log.info("Finished loading offsets and group metadata from {} in {} milliseconds",
                            topicPartition, time.milliseconds() - startMs);
                    } else {
                        log.error("Error loading offsets from {}", topicPartition, cause);
                    }
                    inLock(partitionLock, () -> {
                        ownedPartitions.add(topicPartition.partition());
                        loadingPartitions.remove(topicPartition.partition());
                        return null;
                    });
                });
        } else {
            log.info("Already loading offsets and group metadata from {}", topicPartition);
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> doLoadGroupsAndOffsets(
        Reader<byte[]> metadataConsumer,
        MessageId endMessageId,
        Consumer<GroupMetadata> onGroupLoaded
    ) {
        final Map<String, GroupMetadata> loadedGroups = new HashMap<>();
        final Set<String> removedGroups = new HashSet<>();
        final CompletableFuture<Void> resultFuture = new CompletableFuture<>();

        loadNextMetadataMessage(
            metadataConsumer,
            endMessageId,
            resultFuture,
            onGroupLoaded,
            loadedGroups,
            removedGroups);

        return resultFuture;
    }

    private void loadNextMetadataMessage(Reader<byte[]> metadataConsumer,
                                         MessageId endMessageId,
                                         CompletableFuture<Void> resultFuture,
                                         Consumer<GroupMetadata> onGroupLoaded,
                                         Map<String, GroupMetadata> loadedGroups,
                                         Set<String> removedGroups) {
        metadataConsumer.readNextAsync().whenComplete((message, cause) -> {
            if (null != cause) {
                resultFuture.completeExceptionally(cause);
                return;
            }

            if (message.getMessageId().compareTo(endMessageId) >= 0) {
                // reach the end of partition
                processLoadedAndRemovedGroups(
                    resultFuture,
                    onGroupLoaded,
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
                    loadedGroups,
                    removedGroups
                );
                return;
            }

            BaseKey baseKey = readMessageKey(ByteBuffer.wrap(message.getKeyBytes()));
            if (baseKey instanceof GroupMetadataKey) {
                // load group metadata
                GroupMetadataKey gmKey = (GroupMetadataKey) baseKey;
                String groupId = gmKey.key();
                byte[] data = message.getValue();
                if (null == data || data.length == 0) {
                    // null value
                    loadedGroups.remove(groupId);
                    removedGroups.add(groupId);
                } else {
                    GroupMetadata groupMetadata = readGroupMessageValue(
                        groupId,
                        ByteBuffer.wrap(message.getValue())
                    );
                    if (null != groupMetadata) {
                        removedGroups.remove(groupId);
                        loadedGroups.put(groupId, groupMetadata);
                    } else {
                        loadedGroups.remove(groupId);
                        removedGroups.add(groupId);
                    }
                }

                loadNextMetadataMessage(
                    metadataConsumer,
                    endMessageId,
                    resultFuture,
                    onGroupLoaded,
                    loadedGroups,
                    removedGroups
                );
            } else {
                resultFuture.completeExceptionally(
                    new IllegalStateException("Unexpected message key "
                        + baseKey + " while loading offsets and group metadata"));
            }

        });
    }

    private void processLoadedAndRemovedGroups(CompletableFuture<Void> resultFuture,
                                               Consumer<GroupMetadata> onGroupLoaded,
                                               Map<String, GroupMetadata> loadedGroups,
                                               Set<String> removedGroups) {
        try {
            loadedGroups.values().forEach(group -> {
                loadGroup(group);
                onGroupLoaded.accept(group);
            });

            removedGroups.forEach(groupId -> {
                // TODO: add offsets later
            });
            resultFuture.complete(null);
        } catch (RuntimeException re) {
            resultFuture.completeExceptionally(re);
        }
    }

    private void loadGroup(GroupMetadata group) {
        GroupMetadata currentGroup = addGroup(group);
        if (group != currentGroup) {
            log.debug("Attempt to load group {} from log with generation {} failed "
                + "because there is already a cached group with generation {}",
                group.groupId(), group.generationId(), currentGroup.generationId());
        }
    }

    /**
     * Add the partition into the owned list
     *
     * NOTE: this is for test only
     */
    private void addPartitionOwnership(int partition) {
        inLock(partitionLock, () -> {
            ownedPartitions.add(partition);
            return null;
        });
    }

    /**
     * Add a partition to the loading partitions set. Return true if the partition was not
     * already loading.
     *
     * Visible for testing
     */
    boolean addLoadingPartition(int partition) {
        return inLock(partitionLock, () -> loadingPartitions.add(partition));
    }




}
