package io.streamnative.pulsar.handlers.kop.coordinator.transaction;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import io.streamnative.pulsar.handlers.kop.KopProtocolHandlerTestBase;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
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

    @AfterMethod
    protected void cleanZNode() throws Exception {
        Stat stat = mockZooKeeper.exists(ProducerIdManager.kopPidBlockZNode, null);
        if (stat != null) {
            mockZooKeeper.delete(ProducerIdManager.kopPidBlockZNode, -1);
        }
    }

    @Test
    public void testGetProducerId() throws Exception {
        ProducerIdManager manager1 = new ProducerIdManager(0, mockZooKeeper);
        manager1.initialize().get();
        ProducerIdManager manager2 = new ProducerIdManager(1, mockZooKeeper);
        manager2.initialize().get();

        long pid1 = manager1.generateProducerId().get();
        long pid2 = manager2.generateProducerId().get();

        assertEquals(0, pid1);
        assertEquals(ProducerIdManager.pidBlockSize.longValue(), pid2);

        for (long i = 1; i < ProducerIdManager.pidBlockSize; i++) {
            assertEquals(pid1 + i, manager1.generateProducerId().get().longValue());
        }

        for (long i = 1; i < ProducerIdManager.pidBlockSize; i++) {
            assertEquals(pid2 + i, manager2.generateProducerId().get().longValue());
        }

        assertEquals(pid2 + ProducerIdManager.pidBlockSize, manager1.generateProducerId().get().longValue());
        assertEquals(pid2 + ProducerIdManager.pidBlockSize * 2, manager2.generateProducerId().get().longValue());
    }

    @Test
    public void testExceedProducerIdLimit() throws Exception {
        mockZooKeeper.create(ProducerIdManager.kopPidBlockZNode, null, null, null);
        mockZooKeeper.setData(ProducerIdManager.kopPidBlockZNode,
                ProducerIdManager.generateProducerIdBlockJson(
                        new ProducerIdManager.ProducerIdBlock(
                                1, Long.MAX_VALUE - ProducerIdManager.pidBlockSize, Long.MAX_VALUE)), -1);

        ProducerIdManager producerIdManager = new ProducerIdManager(0, mockZooKeeper);
        try {
            producerIdManager.initialize().get();
            fail("Have exhausted all producerIds, the initialize operation should be failed.");
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getMessage(), "Have exhausted all producerIds.");
        }
    }

}
