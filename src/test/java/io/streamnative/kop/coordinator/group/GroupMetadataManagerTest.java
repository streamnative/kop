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
package io.streamnative.kop.coordinator.group;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import io.streamnative.kop.MockKafkaServiceBaseTest;
import io.streamnative.kop.coordinator.group.GroupMetadataManager.BaseKey;
import io.streamnative.kop.coordinator.group.GroupMetadataManager.GroupMetadataKey;
import io.streamnative.kop.utils.MockTime;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link GroupMetadataManager}.
 */
@Slf4j
public class GroupMetadataManagerTest extends MockKafkaServiceBaseTest {

    private static final String groupId = "foo";
    private static final int groupPartitionId = 0;
    private static final TopicPartition groupTopicPartition =
        new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, groupPartitionId);
    private static final String protocolType = "protocolType";
    private static final int rebalanceTimeout = 60000;
    private static final int sessionTimeout = 10000;

    MockTime time = null;
    GroupMetadataManager groupMetadataManager = null;
    Producer<byte[]> producer = null;
    Reader<byte[]> consumer = null;
    OffsetConfig offsetConfig = OffsetConfig.builder().build();

    @Before
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        log.info("Admin : {}", admin);
        admin.clusters().createCluster("test",
            new ClusterData("http://127.0.0.1:" + brokerWebservicePort));

        admin.tenants().createTenant("public",
            new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
        admin.namespaces().setRetention("public/default",
            new RetentionPolicies(20, 100));

        time = new MockTime();
        groupMetadataManager = new GroupMetadataManager(
            1,
            offsetConfig,
            producer,
            consumer,
            time
        );
    }

    @After
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testGroupNotExits() {
        // group is not owned
        assertFalse(groupMetadataManager.groupNotExists(groupId));

        groupMetadataManager.addPartitionOwnership(groupPartitionId);
        // group is owned but does not exist yet
        assertTrue(groupMetadataManager.groupNotExists(groupId));

        GroupMetadata group = new GroupMetadata(groupId, GroupState.Empty);
        groupMetadataManager.addGroup(group);

        // group is owned but not Dead
        assertFalse(groupMetadataManager.groupNotExists(groupId));

        group.transitionTo(GroupState.Dead);
        // group is owned and Dead
        assertTrue(groupMetadataManager.groupNotExists(groupId));
    }

    @Test
    public void testAddGroup() {
        GroupMetadata group = new GroupMetadata("foo", GroupState.Empty);
        assertEquals(group, groupMetadataManager.addGroup(group));
        assertEquals(group, groupMetadataManager.addGroup(
            new GroupMetadata("foo", GroupState.Empty)
        ));
    }

    /**
     * A group metadata manager test runner.
     */
    @FunctionalInterface
    public interface GroupMetadataManagerTester {

        void test(GroupMetadataManager groupMetadataManager,
                  Consumer<byte[]> consumer) throws Exception;

    }

    void runGroupMetadataManagerTester(final String topicName,
                                       GroupMetadataManagerTester tester) throws Exception {
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topicName)
            .create();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(topicName)
            .subscriptionName("test-sub")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();
        @Cleanup
        Reader<byte[]> reader = pulsarClient.newReader()
            .topic(topicName)
            .startMessageId(MessageId.earliest)
            .create();
        groupMetadataManager = new GroupMetadataManager(
            1,
            offsetConfig,
            producer,
            reader,
            time
        );
        tester.test(groupMetadataManager, consumer);
    }

    @Test
    public void testStoreEmptyGroup() throws Exception {
        final String topicName = "test-store-empty-group";

        runGroupMetadataManagerTester(topicName, (groupMetadataManager, consumer) -> {
            int generation = 27;
            String protocolType = "consumer";
            GroupMetadata group = GroupMetadata.loadGroup(
                groupId,
                GroupState.Empty,
                generation,
                protocolType,
                null,
                null,
                Collections.emptyList()
            );
            groupMetadataManager.addGroup(group);

            Errors errors = groupMetadataManager.storeGroup(group, Collections.emptyMap()).get();
            assertEquals(Errors.NONE, errors);

            Message<byte[]> message = consumer.receive();
            assertTrue(message.getEventTime() > 0L);
            assertTrue(message.hasKey());
            byte[] key = message.getKeyBytes();
            byte[] value = message.getValue();

            BaseKey bk = GroupMetadataConstants.readMessageKey(ByteBuffer.wrap(key));
            assertTrue(bk instanceof GroupMetadataKey);
            GroupMetadataKey gmk = (GroupMetadataKey) bk;
            assertEquals(groupId, gmk.key());

            GroupMetadata gm = GroupMetadataConstants.readGroupMessageValue(
                groupId, ByteBuffer.wrap(value)
            );
            assertTrue(gm.is(GroupState.Empty));
            assertEquals(generation, gm.generationId());
            assertEquals(Optional.of(protocolType), gm.protocolType());
        });
    }

    @Test
    public void testStoreEmptySimpleGroup() throws Exception {
        final String topicName = "test-store-empty-simple-group";

        runGroupMetadataManagerTester(topicName, (groupMetadataManager, consumer) -> {

            GroupMetadata group = new GroupMetadata(groupId, GroupState.Empty);
            groupMetadataManager.addGroup(group);

            Errors errors = groupMetadataManager.storeGroup(group, Collections.emptyMap()).get();
            assertEquals(Errors.NONE, errors);

            Message<byte[]> message = consumer.receive();
            assertTrue(message.getEventTime() > 0L);
            assertTrue(message.hasKey());
            byte[] key = message.getKeyBytes();
            byte[] value = message.getValue();

            BaseKey bk = GroupMetadataConstants.readMessageKey(ByteBuffer.wrap(key));
            assertTrue(bk instanceof GroupMetadataKey);
            GroupMetadataKey gmk = (GroupMetadataKey) bk;
            assertEquals(groupId, gmk.key());

            GroupMetadata gm = GroupMetadataConstants.readGroupMessageValue(
                groupId, ByteBuffer.wrap(value)
            );
            assertTrue(gm.is(GroupState.Empty));
            assertEquals(0, gm.generationId());
            assertEquals(Optional.empty(), gm.protocolType());

        });
    }

    @Test
    public void testStoreNoneEmptyGroup() throws Exception {
        final String topicName = "test-store-non-empty-group";

        runGroupMetadataManagerTester(topicName, (groupMetadataManager, consumer) -> {
            String memberId = "memberId";
            String clientId = "clientId";
            String clientHost = "localhost";

            GroupMetadata group = new GroupMetadata(groupId, GroupState.Empty);
            groupMetadataManager.addGroup(group);

            Map<String, byte[]> protocols = new HashMap<>();
            protocols.put("protocol", new byte[0]);
            MemberMetadata member = new MemberMetadata(
                memberId,
                groupId,
                clientId,
                clientHost,
                rebalanceTimeout,
                sessionTimeout,
                protocolType,
                protocols
            );
            CompletableFuture<JoinGroupResult> joinFuture = new CompletableFuture<>();
            member.awaitingJoinCallback(joinFuture);
            group.add(member);
            group.transitionTo(GroupState.PreparingRebalance);
            group.initNextGeneration();

            Map<String, byte[]> assignments = new HashMap<>();
            assignments.put(memberId, new byte[0]);
            Errors errors = groupMetadataManager.storeGroup(group, assignments).get();
            assertEquals(Errors.NONE, errors);

            Message<byte[]> message = consumer.receive();
            assertTrue(message.getEventTime() > 0L);
            assertTrue(message.hasKey());
            byte[] key = message.getKeyBytes();
            byte[] value = message.getValue();

            BaseKey bk = GroupMetadataConstants.readMessageKey(ByteBuffer.wrap(key));
            assertTrue(bk instanceof GroupMetadataKey);
            GroupMetadataKey gmk = (GroupMetadataKey) bk;
            assertEquals(groupId, gmk.key());

            GroupMetadata gm = GroupMetadataConstants.readGroupMessageValue(
                groupId, ByteBuffer.wrap(value)
            );
            assertEquals(GroupState.Stable, gm.currentState());
            assertEquals(1, gm.generationId());
            assertEquals(Optional.of(protocolType), gm.protocolType());
            assertEquals("protocol", gm.protocolOrNull());
            assertTrue(gm.has(memberId));
        });
    }

}
