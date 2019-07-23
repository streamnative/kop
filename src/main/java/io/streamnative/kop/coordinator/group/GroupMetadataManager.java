package io.streamnative.kop.coordinator.group;

import static io.streamnative.kop.coordinator.group.GroupMetadataConstants.CURRENT_GROUP_KEY_SCHEMA;
import static io.streamnative.kop.coordinator.group.GroupMetadataConstants.CURRENT_GROUP_KEY_SCHEMA_VERSION;
import static io.streamnative.kop.coordinator.group.GroupMetadataConstants.GROUP_KEY_GROUP_FIELD;
import static io.streamnative.kop.utils.CoreUtils.inLock;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
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
import org.apache.bookkeeper.common.hash.Murmur3;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.ApiVersionsResponse.ApiVersion;

/**
 * Manager to manage a coordination group.
 */
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
    static class GroupMetadataKey {

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
    /* single-thread scheduler to handle offset/group metadata cache loading and unloading */
    private OrderedScheduler scheduler;
    private final String logIdent;
    private final int groupMetadataTopicPartitionCount;

    GroupMetadataManager(int brokerId,
                         ApiVersion interBrokerProtocolVersion,
                         OffsetConfig config,
                         int groupMetadataTopicPartitionCount) {
        this.brokerId = brokerId;
        this.interBrokerProtocolVersion = interBrokerProtocolVersion;
        this.config = config;
        this.compressionType = config.offsetsTopicCompressionType();
        this.groupMetadataCache = new ConcurrentHashMap<>();
        this.logIdent = String.format("[GroupMetadataManager brokerId=%d]", brokerId);
        this.groupMetadataTopicPartitionCount = groupMetadataTopicPartitionCount;
    }

    public void startup() {
        this.scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("group-metadata-manager")
            .numThreads(1)
            .build();
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

    }




}
