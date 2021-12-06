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
import static org.testng.AssertJUnit.fail;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Producer id manager test.
 */
@Slf4j
public class ProducerIdManagerTest extends KopProtocolHandlerTestBase {

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

    @BeforeMethod
    protected void cleanZNode() throws Exception {
        Stat stat = mockZooKeeper.exists(ProducerIdManager.KOP_PID_BLOCK_ZNODE, null);
        if (stat != null) {
            mockZooKeeper.delete(ProducerIdManager.KOP_PID_BLOCK_ZNODE, -1);
        }
    }

    @Test
    public void testGetProducerId() throws Exception {
        ProducerIdManager manager1 = new ProducerIdManager(0, pulsar.getLocalMetadataStore());
        manager1.initialize().get();
        ProducerIdManager manager2 = new ProducerIdManager(1, pulsar.getLocalMetadataStore());
        manager2.initialize().get();

        long pid1 = manager1.generateProducerId().get();
        long pid2 = manager2.generateProducerId().get();

        assertEquals(0, pid1);
        assertEquals(ProducerIdManager.PID_BLOCK_SIZE.longValue(), pid2);

        for (long i = 1; i < ProducerIdManager.PID_BLOCK_SIZE; i++) {
            assertEquals(pid1 + i, manager1.generateProducerId().get().longValue());
        }

        for (long i = 1; i < ProducerIdManager.PID_BLOCK_SIZE; i++) {
            assertEquals(pid2 + i, manager2.generateProducerId().get().longValue());
        }

        assertEquals(pid2 + ProducerIdManager.PID_BLOCK_SIZE, manager1.generateProducerId().get().longValue());
        assertEquals(pid2 + ProducerIdManager.PID_BLOCK_SIZE * 2, manager2.generateProducerId().get().longValue());
    }

    @Test
    public void testExceedProducerIdLimit() throws Exception {
        mockZooKeeper.create(ProducerIdManager.KOP_PID_BLOCK_ZNODE, null, null, null);
        mockZooKeeper.setData(ProducerIdManager.KOP_PID_BLOCK_ZNODE,
                ProducerIdManager.generateProducerIdBlockJson(
                        new ProducerIdManager.ProducerIdBlock(
                                1, Long.MAX_VALUE - ProducerIdManager.PID_BLOCK_SIZE, Long.MAX_VALUE)), -1);

        ProducerIdManager producerIdManager = new ProducerIdManager(0, pulsar.getLocalMetadataStore());
        try {
            producerIdManager.initialize().get();
            fail("Have exhausted all producerIds, the initialize operation should be failed.");
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getMessage(), "Have exhausted all producerIds.");
        }
    }

}
