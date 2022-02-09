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

import static org.testng.AssertJUnit.assertEquals;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import java.util.Random;
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
        pulsar.getAdminClient().topics().createNonPartitionedTopic("producerId");

        ProducerIdManager manager1 = new PulsarStorageProducerIdManagerImpl("producerId", pulsar.getClient());
        manager1.initialize().get();
        ProducerIdManager manager2 = new PulsarStorageProducerIdManagerImpl("producerId", pulsar.getClient());
        manager2.initialize().get();

        long pid1 = manager1.generateProducerId().get();
        long pid2 = manager2.generateProducerId().get();

        assertEquals(1, pid1);
        assertEquals(2, pid2);

        Random r = new Random(1032);
        for (long i = 1; i < 100; i++) {
            ProducerIdManager manager = r.nextBoolean() ? manager1 : manager2;
            assertEquals(pid2 + i, manager.generateProducerId().get().longValue());
        }
    }

}
