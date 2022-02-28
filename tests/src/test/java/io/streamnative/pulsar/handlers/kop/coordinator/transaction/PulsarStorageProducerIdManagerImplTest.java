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
package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.awaitility.Awaitility;
import org.powermock.reflect.Whitebox;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Producer id manager test.
 */
@Slf4j
public class PulsarStorageProducerIdManagerImplTest extends KopProtocolHandlerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testGetProducerId() throws Exception {
        // we need a non-partitioned topic
        pulsar.getAdminClient().topics().createNonPartitionedTopic("testGetProducerId");

        ProducerIdManager manager1 = new PulsarStorageProducerIdManagerImpl(
                "testGetProducerId", pulsar.getClient());
        manager1.initialize().get();
        ProducerIdManager manager2 = new PulsarStorageProducerIdManagerImpl(
                "testGetProducerId", pulsar.getClient());
        manager2.initialize().get();

        long pid1 = manager1.generateProducerId().get();
        long pid2 = manager2.generateProducerId().get();

        assertEquals(PulsarStorageProducerIdManagerImpl.BLOCK_SIZE, pid1);
        assertEquals(pid1 + PulsarStorageProducerIdManagerImpl.BLOCK_SIZE, pid2);

        for (long i = 1; i < PulsarStorageProducerIdManagerImpl.BLOCK_SIZE; i++) {
            assertEquals(pid1 + i, manager1.generateProducerId().get().longValue());
        }

        for (long i = 1; i < PulsarStorageProducerIdManagerImpl.BLOCK_SIZE; i++) {
            assertEquals(pid2 + i, manager2.generateProducerId().get().longValue());
        }

        assertEquals(pid2 + PulsarStorageProducerIdManagerImpl.BLOCK_SIZE,
                manager1.generateProducerId().get().longValue());
        assertEquals(pid2 + PulsarStorageProducerIdManagerImpl.BLOCK_SIZE * 2,
                manager2.generateProducerId().get().longValue());
    }


    @Test
    public void testGetProducerIdWithoutDuplicates() throws Exception {

        // we need a non-partitioned topic
        pulsar.getAdminClient().topics().createNonPartitionedTopic("testGetProducerIdWithoutDuplicates");

        ProducerIdManager manager1 = new PulsarStorageProducerIdManagerImpl(
                "testGetProducerIdWithoutDuplicates", pulsar.getClient());
        manager1.initialize().get();
        ProducerIdManager manager2 = new PulsarStorageProducerIdManagerImpl(
                "testGetProducerIdWithoutDuplicates", pulsar.getClient());
        manager2.initialize().get();

        long pid1 = manager1.generateProducerId().get();
        long pid2 = manager2.generateProducerId().get();

        assertEquals(PulsarStorageProducerIdManagerImpl.BLOCK_SIZE, pid1);
        assertEquals(pid1 + PulsarStorageProducerIdManagerImpl.BLOCK_SIZE, pid2);

        Set<Long> checkDuplicates = new HashSet<>();
        checkDuplicates.add(pid1);
        checkDuplicates.add(pid2);
        Random r = new Random(1032);
        for (long i = 0; i < PulsarStorageProducerIdManagerImpl.BLOCK_SIZE * 4; i++) {
            final boolean useFirst = r.nextBoolean();
            final ProducerIdManager manager;
            if (useFirst) {
                manager = manager1;
            } else {
                manager = manager2;
            }
            final long generated = manager.generateProducerId().get().longValue();
            assertTrue(checkDuplicates.add(generated));
            assertTrue(generated > 0);
        }
        assertEquals(checkDuplicates.size(), PulsarStorageProducerIdManagerImpl.BLOCK_SIZE * 4 + 2);
    }

    @Test
    public void testGetProducerIdWithRetentionAndTopicDeleted() throws Exception {
        String namespace = "public/namespace-no-retention";
        pulsar.getAdminClient().namespaces().createNamespace(namespace);
        pulsar.getAdminClient().namespaces().setRetention(namespace,
                new RetentionPolicies(0, 0));
        pulsar.getAdminClient().namespaces().setInactiveTopicPolicies(namespace, new InactiveTopicPolicies(
                InactiveTopicDeleteMode.delete_when_no_subscriptions, 0, true));

        String topic = namespace + "/testGetProducerIdNoRetention";
        // we need a non-partitioned topic
        pulsar.getAdminClient().topics().createNonPartitionedTopic(topic);

        ProducerIdManager manager1 = new PulsarStorageProducerIdManagerImpl(topic, pulsar.getClient());
        manager1.initialize().get();
        long pid1 = manager1.generateProducerId().get();
        assertEquals(PulsarStorageProducerIdManagerImpl.BLOCK_SIZE, pid1);
        manager1.shutdown();

        List<String> list = pulsar.getAdminClient().topics().getList(namespace);
        log.info("topics {}", list);
        assertEquals(1, list.size());

        // wait for topic to be automatically deleted
        Awaitility.await().untilAsserted(
                () -> {
                    pulsar.getBrokerService().checkGC();
                    List<String> list2 = pulsar.getAdminClient().topics().getList(namespace);
                    log.info("topics {}", list2);
                    assertTrue(list2.isEmpty());
                });


        // start from scratch
        ProducerIdManager manager2 = new PulsarStorageProducerIdManagerImpl(topic, pulsar.getClient());
        manager2.initialize().get();
        long pid2 = manager2.generateProducerId().get();
        assertEquals(PulsarStorageProducerIdManagerImpl.BLOCK_SIZE, pid2);
        manager2.shutdown();
    }

    @Test(enabled = false, description = "This test does not pass on Luna Streaming, but on ASF Pulsar it works,"
            + "  in LS we are missing some fixes about Reader#hasMessageAvailalable")
    public void testGetProducerIdWithTTL() throws Exception {
        String namespace = "public/namespace-no-retention-ttl";
        pulsar.getAdminClient().namespaces().createNamespace(namespace);
        pulsar.getAdminClient().namespaces().setRetention(namespace,
                new RetentionPolicies(0, 0));
        pulsar.getAdminClient().namespaces().setInactiveTopicPolicies(namespace, new InactiveTopicPolicies(
                InactiveTopicDeleteMode.delete_when_no_subscriptions, Integer.MAX_VALUE, false));
        pulsar.getAdminClient().namespaces().setNamespaceMessageTTL(namespace, 1);

        String topic = namespace + "/testGetProducerIdNoRetention";
        // we need a non-partitioned topic
        pulsar.getAdminClient().topics().createNonPartitionedTopic(topic);

        ProducerIdManager manager1 = new PulsarStorageProducerIdManagerImpl(topic, pulsar.getClient(), 1);
        manager1.initialize().get();
        long pid1 = manager1.generateProducerId().get();
        assertEquals(1, pid1);
        manager1.shutdown();

        Thread.sleep(3000);

        admin.topics().unload(topic);
        admin.topics().getStats(topic);

        PersistentTopicInternalStats stats = pulsar.getAdminClient().topics().getInternalStats(topic);
        log.info("stats {}", stats);
        Whitebox.invokeMethod(pulsar.getBrokerService(), "checkConsumedLedgers");

        // wait for topic to be automatically trimmed
        Awaitility
                .await()
                .pollInterval(5, TimeUnit.SECONDS)
                .untilAsserted(
                () -> {
                    Whitebox.invokeMethod(pulsar.getBrokerService(), "checkConsumedLedgers");
                    PersistentTopicInternalStats stats2 = pulsar.getAdminClient().topics().getInternalStats(topic);
                    log.info("stats2 {}", stats2);
                    assertEquals(0, stats2.numberOfEntries);
                });

        // start from scratch, we lost all the messages
        ProducerIdManager manager2 = new PulsarStorageProducerIdManagerImpl(topic, pulsar.getClient(), 1);
        manager2.initialize().get();
        long pid2 = manager2.generateProducerId().get();
        assertEquals(1, pid2);
        manager2.shutdown();
    }
}
