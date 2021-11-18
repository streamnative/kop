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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.kafka.common.protocol.types.Type.BYTES;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.INT64;
import static org.apache.kafka.common.protocol.types.Type.NULLABLE_STRING;
import static org.apache.kafka.common.protocol.types.Type.STRING;

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataManager.BaseKey;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataManager.GroupMetadataKey;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataManager.GroupTopicPartition;
import io.streamnative.pulsar.handlers.kop.coordinator.group.GroupMetadataManager.OffsetKey;
import io.streamnative.pulsar.handlers.kop.exceptions.KoPTopicException;
import io.streamnative.pulsar.handlers.kop.offset.OffsetAndMetadata;
import io.streamnative.pulsar.handlers.kop.utils.KopTopic;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.pulsar.common.schema.KeyValue;

/**
 * Messages stored for the group topic has versions for both the key and value fields. Key
 * version is used to indicate the type of the message (also to differentiate different types
 * of messages from being compacted together if they have the same field values); and value
 * version is used to evolve the messages within their data types:
 *
 * <p>key version 0:       group consumption offset
 *    -> value version 0:       [offset, metadata, timestamp]
 *
 * <p>key version 1:       group consumption offset
 *    -> value version 1:       [offset, metadata, commit_timestamp, expire_timestamp]
 *
 * <p>key version 2:       group metadata
 *     -> value version 0:       [protocol_type, generation, protocol, leader, members]
 */
@Slf4j
public final class GroupMetadataConstants {

    static final short CURRENT_OFFSET_KEY_SCHEMA_VERSION = 1;
    static final short CURRENT_GROUP_KEY_SCHEMA_VERSION = 2;

    static final Schema OFFSET_COMMIT_KEY_SCHEMA = new Schema(
        new Field("group", STRING),
        new Field("topic", STRING),
        new Field("partition", INT32)
    );
    static final BoundField OFFSET_KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("group");
    static final BoundField OFFSET_KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("topic");
    static final BoundField OFFSET_KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("partition");

    static final Schema OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(
        new Field("offset", INT64),
        new Field("metadata", STRING, "Associated metadata.", ""),
        new Field("timestamp", INT64)
    );
    static final BoundField OFFSET_VALUE_OFFSET_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset");
    static final BoundField OFFSET_VALUE_METADATA_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata");
    static final BoundField OFFSET_VALUE_TIMESTAMP_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp");

    static final Schema OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(
        new Field("offset", INT64),
        new Field("metadata", STRING, "Associated metadata.", ""),
        new Field("commit_timestamp", INT64),
        new Field("expire_timestamp", INT64)
    );
    static final BoundField OFFSET_VALUE_OFFSET_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset");
    static final BoundField OFFSET_VALUE_METADATA_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata");
    static final BoundField OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1 =
        OFFSET_COMMIT_VALUE_SCHEMA_V1.get("commit_timestamp");
    static final BoundField OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1 =
        OFFSET_COMMIT_VALUE_SCHEMA_V1.get("expire_timestamp");

    static final Schema GROUP_METADATA_KEY_SCHEMA = new Schema(new Field("group", STRING));
    static final BoundField GROUP_KEY_GROUP_FIELD = GROUP_METADATA_KEY_SCHEMA.get("group");

    static final String MEMBER_ID_KEY = "member_id";
    static final String CLIENT_ID_KEY = "client_id";
    static final String CLIENT_HOST_KEY = "client_host";
    static final String REBALANCE_TIMEOUT_KEY = "rebalance_timeout";
    static final String SESSION_TIMEOUT_KEY = "session_timeout";
    static final String SUBSCRIPTION_KEY = "subscription";
    static final String ASSIGNMENT_KEY = "assignment";

    static final Schema MEMBER_METADATA_V0 = new Schema(
        new Field(MEMBER_ID_KEY, STRING),
        new Field(CLIENT_ID_KEY, STRING),
        new Field(CLIENT_HOST_KEY, STRING),
        new Field(SESSION_TIMEOUT_KEY, INT32),
        new Field(SUBSCRIPTION_KEY, BYTES),
        new Field(ASSIGNMENT_KEY, BYTES));

    static final Schema MEMBER_METADATA_V1 = new Schema(
        new Field(MEMBER_ID_KEY, STRING),
        new Field(CLIENT_ID_KEY, STRING),
        new Field(CLIENT_HOST_KEY, STRING),
        new Field(REBALANCE_TIMEOUT_KEY, INT32),
        new Field(SESSION_TIMEOUT_KEY, INT32),
        new Field(SUBSCRIPTION_KEY, BYTES),
        new Field(ASSIGNMENT_KEY, BYTES));

    static final String PROTOCOL_TYPE_KEY = "protocol_type";
    static final String GENERATION_KEY = "generation";
    static final String PROTOCOL_KEY = "protocol";
    static final String LEADER_KEY = "leader";
    static final String MEMBERS_KEY = "members";

    static final Schema GROUP_METADATA_VALUE_SCHEMA_V0 = new Schema(
        new Field(PROTOCOL_TYPE_KEY, STRING),
        new Field(GENERATION_KEY, INT32),
        new Field(PROTOCOL_KEY, NULLABLE_STRING),
        new Field(LEADER_KEY, NULLABLE_STRING),
        new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V0)));

    static final Schema GROUP_METADATA_VALUE_SCHEMA_V1 = new Schema(
        new Field(PROTOCOL_TYPE_KEY, STRING),
        new Field(GENERATION_KEY, INT32),
        new Field(PROTOCOL_KEY, NULLABLE_STRING),
        new Field(LEADER_KEY, NULLABLE_STRING),
        new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V1)));

    // map of versions to key schemas as data types
    static final Map<Integer, Schema> MESSAGE_TYPE_SCHEMAS = asMap(
        kv(0, OFFSET_COMMIT_KEY_SCHEMA),
        kv(1, OFFSET_COMMIT_KEY_SCHEMA),
        kv(2, GROUP_METADATA_KEY_SCHEMA)
    );

    // map of version of offset value schemas
    static final Map<Integer, Schema> OFFSET_VALUE_SCHEMAS = asMap(
        kv(0, OFFSET_COMMIT_VALUE_SCHEMA_V0),
        kv(1, OFFSET_COMMIT_VALUE_SCHEMA_V1)
    );
    static final short CURRENT_OFFSET_VALUE_SCHEMA_VERSION = 1;

    // map of version of group metadata value schemas
    static final Map<Integer, Schema> GROUP_VALUE_SCHEMAS = asMap(
        kv(0, GROUP_METADATA_VALUE_SCHEMA_V0),
        kv(1, GROUP_METADATA_VALUE_SCHEMA_V1)
    );
    static final short CURRENT_GROUP_VALUE_SCHEMA_VERSION = 1;

    static final Schema CURRENT_OFFSET_KEY_SCHEMA = schemaForKey(CURRENT_OFFSET_KEY_SCHEMA_VERSION);
    static final Schema CURRENT_GROUP_KEY_SCHEMA = schemaForKey(CURRENT_GROUP_KEY_SCHEMA_VERSION);

    static final Schema CURRENT_OFFSET_VALUE_SCHEMA = schemaForOffset(CURRENT_OFFSET_VALUE_SCHEMA_VERSION);
    static final Schema CURRENT_GROUP_VALUE_SCHEMA = schemaForGroup(CURRENT_GROUP_VALUE_SCHEMA_VERSION);

    private static Schema schemaForKey(int version) {
        Schema schema = MESSAGE_TYPE_SCHEMAS.get(version);
        if (null == schema) {
            throw new KafkaException("Unknown offset schema version " + version);
        }
        return schema;
    }

    private static Schema schemaForOffset(int version) {
        Schema schema = OFFSET_VALUE_SCHEMAS.get(version);
        if (null == schema) {
            throw new KafkaException("Unknown offset schema version " + version);
        }
        return schema;
    }

    private static Schema schemaForGroup(int version) {
        Schema schema = GROUP_VALUE_SCHEMAS.get(version);
        if (null == schema) {
            throw new KafkaException("Unknown group metadata version " + version);
        }
        return schema;
    }

    private static <K, V> KeyValue<K, V> kv(K key, V value) {
        return new KeyValue<>(key, value);
    }

    private static <K, V> Map<K, V> asMap(KeyValue<K, V> ...kvs) {
        return Lists.newArrayList(kvs)
            .stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> e.getValue()
            ));
    }

    /**
     * Generates the key for offset commit message for given (group, topic, partition).
     *
     * @return key for offset commit message
     */
    static byte[] offsetCommitKey(String group,
                                  TopicPartition topicPartition) {
        return offsetCommitKey(group, topicPartition, (short) 0);
    }

    static byte[] offsetCommitKey(String group,
                                  TopicPartition topicPartition,
                                  short versionId) {
        // Some test cases may use the original topic name to commit the offset
        // directly from this method, so we need to ensure that all the topics
        // has chances to be converted
        if (topicPartition.partition() >= 0 && !KopTopic.isFullTopicName(topicPartition.topic())) {
            try {
                topicPartition = new TopicPartition(
                        new KopTopic(topicPartition.topic(), null).getFullName(), topicPartition.partition());
            } catch (KoPTopicException e) {
                // In theory, this place will not be executed
                log.warn("Invalid topic name: {}", topicPartition.topic(), e);
                return null;
            }
        }

        Struct key = new Struct(CURRENT_OFFSET_KEY_SCHEMA);
        key.set(OFFSET_KEY_GROUP_FIELD, group);
        key.set(OFFSET_KEY_TOPIC_FIELD, topicPartition.topic());
        key.set(OFFSET_KEY_PARTITION_FIELD, topicPartition.partition());

        ByteBuffer byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf());
        byteBuffer.putShort(CURRENT_OFFSET_KEY_SCHEMA_VERSION);
        key.writeTo(byteBuffer);
        return byteBuffer.array();
    }

    /**
     * Generates the key for group metadata message for given group.
     *
     * @return key bytes for group metadata message
     */
    static byte[] groupMetadataKey(String group) {
        Struct key = new Struct(CURRENT_GROUP_KEY_SCHEMA);
        key.set(GROUP_KEY_GROUP_FIELD, group);
        ByteBuffer byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf());
        byteBuffer.putShort(CURRENT_GROUP_KEY_SCHEMA_VERSION);
        key.writeTo(byteBuffer);
        return byteBuffer.array();
    }

    /**
     * Generates the payload for offset commit message from given offset and metadata.
     *
     * @param offsetAndMetadata consumer's current offset and metadata
     * @return payload for offset commit message
     */
    static byte[] offsetCommitValue(OffsetAndMetadata offsetAndMetadata) {
        Struct value = new Struct(CURRENT_OFFSET_VALUE_SCHEMA);
        value.set(OFFSET_VALUE_OFFSET_FIELD_V1, offsetAndMetadata.offset());
        value.set(OFFSET_VALUE_METADATA_FIELD_V1, offsetAndMetadata.metadata());
        value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1, offsetAndMetadata.commitTimestamp());
        value.set(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1, offsetAndMetadata.expireTimestamp());
        ByteBuffer byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf());
        byteBuffer.putShort(CURRENT_GROUP_VALUE_SCHEMA_VERSION);
        value.writeTo(byteBuffer);
        return byteBuffer.array();
    }


    static byte[] groupMetadataValue(GroupMetadata groupMetadata,
                                     Map<String, byte[]> assignment) {
        return groupMetadataValue(groupMetadata, assignment, (short) 0);
    }

    /**
     * Generates the payload for group metadata message from given offset and metadata
     * assuming the generation id, selected protocol, leader and member assignment are all available.
     *
     * @param groupMetadata current group metadata
     * @param assignment the assignment for the rebalancing generation
     * @param version the version of the value message to use
     * @return payload for offset commit message
     */
    static byte[] groupMetadataValue(GroupMetadata groupMetadata,
                                     Map<String, byte[]> assignment,
                                     short version) {
        Struct value;
        if (version == 0) {
            value = new Struct(GROUP_METADATA_VALUE_SCHEMA_V0);
        } else {
            value = new Struct(CURRENT_GROUP_VALUE_SCHEMA);
        }

        value.set(PROTOCOL_TYPE_KEY, groupMetadata.protocolType().orElse(""));
        value.set(GENERATION_KEY, groupMetadata.generationId());
        value.set(PROTOCOL_KEY, groupMetadata.protocolOrNull());
        value.set(LEADER_KEY, groupMetadata.leaderOrNull());

        List<Struct> memberStructs = groupMetadata.allMemberMetadata().stream().map(memberMetadata -> {
            Struct memberStruct = value.instance(MEMBERS_KEY);
            memberStruct.set(MEMBER_ID_KEY, memberMetadata.memberId());
            memberStruct.set(CLIENT_ID_KEY, memberMetadata.clientId());
            memberStruct.set(CLIENT_HOST_KEY, memberMetadata.clientHost());
            memberStruct.set(SESSION_TIMEOUT_KEY, memberMetadata.sessionTimeoutMs());

            if (version > 0) {
                memberStruct.set(REBALANCE_TIMEOUT_KEY, memberMetadata.rebalanceTimeoutMs());
            }

            // The group is non-empty, so the current protocol must be defined
            String protocol = groupMetadata.protocolOrNull();
            if (protocol == null) {
                throw new IllegalStateException("Attempted to write non-empty group metadata with no defined protocol");
            }

            byte[] metadata = memberMetadata.metadata(protocol);
            memberStruct.set(SUBSCRIPTION_KEY, ByteBuffer.wrap(metadata));

            byte[] memberAssignment = assignment.get(memberMetadata.memberId());
            checkState(
                memberAssignment != null,
                "Member assignment is null for member %s", memberMetadata.memberId());

            memberStruct.set(ASSIGNMENT_KEY, ByteBuffer.wrap(memberAssignment));

            return memberStruct;
        }).collect(Collectors.toList());

        value.set(MEMBERS_KEY, memberStructs.toArray());

        ByteBuffer byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf());
        byteBuffer.putShort(version);
        value.writeTo(byteBuffer);
        return byteBuffer.array();
    }

    /**
     * Decodes the offset messages' key.
     */
    public static BaseKey readMessageKey(ByteBuffer buffer) {
        short version = buffer.getShort();
        Schema keySchema = schemaForKey(version);
        Struct key = keySchema.read(buffer);

        if (version <= CURRENT_OFFSET_KEY_SCHEMA_VERSION) {
            // version 0 and 1 refer to offset
            String group = key.getString(OFFSET_KEY_GROUP_FIELD);
            String topic = key.getString(OFFSET_KEY_TOPIC_FIELD);
            int partition = key.getInt(OFFSET_KEY_PARTITION_FIELD);
            return new OffsetKey(
                version,
                new GroupTopicPartition(
                    group,
                    topic,
                    partition
                )
            );
        } else if (version == CURRENT_GROUP_KEY_SCHEMA_VERSION) {
            // version 2 refers to group
            String group = key.getString(GROUP_KEY_GROUP_FIELD);

            return new GroupMetadataKey(version, group);
        } else {
            throw new IllegalStateException("Unknown version " + version + " for group metadata message");
        }
    }

    public static OffsetAndMetadata readOffsetMessageValue(ByteBuffer buffer) {
        if (null == buffer) {
            return null;
        }

        short version = buffer.getShort();
        Schema valueSchema = schemaForOffset(version);
        Struct value = valueSchema.read(buffer);

        if (version == 0) {
            long offset = value.getLong(OFFSET_VALUE_OFFSET_FIELD_V0);
            String metadata = value.getString(OFFSET_VALUE_METADATA_FIELD_V0);
            long timestamp = value.getLong(OFFSET_VALUE_TIMESTAMP_FIELD_V0);

            return OffsetAndMetadata.apply(offset, metadata, timestamp);
        } else if (version == 1){
            long offset = value.getLong(OFFSET_VALUE_OFFSET_FIELD_V1);
            String metadata = value.getString(OFFSET_VALUE_METADATA_FIELD_V1);
            long commitTimestamp = value.getLong(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1);
            long expireTimestamp = value.getLong(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1);

            return OffsetAndMetadata.apply(offset, metadata, commitTimestamp, expireTimestamp);
        } else {
            throw  new IllegalStateException("Unknown offset message version " + version);
        }
    }

    static GroupMetadata readGroupMessageValue(String groupId,
                                               ByteBuffer buffer) {
        if (null == buffer) { // tombstone
            return null;
        }

        short version = buffer.getShort();
        Schema valueSchema = schemaForGroup(version);
        Struct value = valueSchema.read(buffer);

        if (version == 0 || version == 1) {
            int generationId = value.getInt(GENERATION_KEY);
            String protocolType = value.getString(PROTOCOL_TYPE_KEY);
            String protocol = value.getString(PROTOCOL_KEY);
            String leaderId = value.getString(LEADER_KEY);
            Object[] memberMetadataArray = value.getArray(MEMBERS_KEY);
            GroupState initialState;
            if (memberMetadataArray.length == 0) {
                initialState = GroupState.Empty;
            } else {
                initialState = GroupState.Stable;
            }

            List<MemberMetadata> members = Lists.newArrayList(memberMetadataArray)
                .stream()
                .map(memberMetadataObj -> {
                    Struct memberMetadata = (Struct) memberMetadataObj;
                    String memberId = memberMetadata.getString(MEMBER_ID_KEY);
                    String clientId = memberMetadata.getString(CLIENT_ID_KEY);
                    String clientHost = memberMetadata.getString(CLIENT_HOST_KEY);
                    int sessionTimeout = memberMetadata.getInt(SESSION_TIMEOUT_KEY);
                    int rebalanceTimeout;
                    if (version == 0) {
                        rebalanceTimeout = sessionTimeout;
                    } else {
                        rebalanceTimeout = memberMetadata.getInt(REBALANCE_TIMEOUT_KEY);
                    }
                    ByteBuffer subscription = memberMetadata.getBytes(SUBSCRIPTION_KEY);
                    byte[] subscriptionData = new byte[subscription.remaining()];
                    subscription.get(subscriptionData);
                    Map<String, byte[]> protocols = new HashMap<>();
                    protocols.put(protocol, subscriptionData);
                    return new MemberMetadata(
                        memberId,
                        groupId,
                        clientId,
                        clientHost,
                        rebalanceTimeout,
                        sessionTimeout,
                        protocolType,
                        protocols
                    );
                }).collect(Collectors.toList());

            return GroupMetadata.loadGroup(
                groupId,
                initialState,
                generationId,
                protocolType,
                protocol,
                leaderId,
                members
            );
        } else {
            throw new IllegalStateException("Unknown group metadata message version");
        }
    }

    private GroupMetadataConstants() {}

}
