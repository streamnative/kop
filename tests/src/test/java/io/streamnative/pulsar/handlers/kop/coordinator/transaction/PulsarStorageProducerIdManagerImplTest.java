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
import java.util.Random;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
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
}
