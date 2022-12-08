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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProducerIdManagerImplTest {

    @Test
    public void verifyThreadSafetyForTwoConcurrentNewProducerIdBlockCalls() throws Exception {
        // Initialize a fake metadata store such that the futures are not completed. This will allow
        // for low level control during this test.
        CompletableFuture<Optional<GetResult>> getFuture = new CompletableFuture<>();
        CompletableFuture<Stat> completedPutFuture = new CompletableFuture<>();
        // The value is not used, so mock with all "zero" values
        completedPutFuture.complete(new Stat("", 0, 0, 0, false, false));

        MetadataStoreExtended mockedMetadataStore = mock(MetadataStoreExtended.class);
        when(mockedMetadataStore.get(anyString())).thenReturn(getFuture);
        when(mockedMetadataStore.put(anyString(), any(), any())).thenReturn(completedPutFuture);

        ProducerIdManagerImpl producerIdManager = new ProducerIdManagerImpl(1, mockedMetadataStore);
        // Trigger two calls to increase the producer id block.
        CompletableFuture<Void> firstNewBlock = producerIdManager.getNewProducerIdBlock();
        CompletableFuture<Void> secondNewBlock = producerIdManager.getNewProducerIdBlock();

        Assert.assertFalse(firstNewBlock.isDone());
        Assert.assertFalse(secondNewBlock.isDone());

        // Relies on the fact that completing the future also triggers the callbacks to run in same thread
        getFuture.complete(Optional.empty());

        // Ensure that both calls completed
        Assert.assertTrue(firstNewBlock.isDone());
        Assert.assertTrue(secondNewBlock.isDone());
        Assert.assertFalse(firstNewBlock.isCompletedExceptionally());
        Assert.assertFalse(secondNewBlock.isCompletedExceptionally());

        // Ensure that the next producer id is the first value
        Assert.assertEquals(producerIdManager.generateProducerId().get().intValue(), 0, "The first id should be 0.");
    }

    @Test
    public void verifyProducerIdManagerForManyBrokersAndManyNewProducers() throws Exception {
        int expectedNumIds = 1000000;
        int numBrokers = 10;
        LocalMemoryMetadataStore metadataStore =
                new LocalMemoryMetadataStore("memory:localhost", MetadataStoreConfig.builder().build());
        List<ProducerIdManagerImpl> producerIdManagers = new ArrayList<>(numBrokers);
        for (int i = 0; i < numBrokers; i++) {
            ProducerIdManagerImpl producerIdManager = new ProducerIdManagerImpl(i, metadataStore);
            producerIdManagers.add(producerIdManager);
            producerIdManager.initialize();
        }

        List<CompletableFuture<Long>> futureIds = new ArrayList<>(expectedNumIds);

        for (int i = 0; i < expectedNumIds; i++) {
            for (ProducerIdManagerImpl producerIdManager : producerIdManagers) {
                futureIds.add(producerIdManager.generateProducerId());
            }
        }

        CompletableFuture.allOf(futureIds.toArray(new CompletableFuture[0])).get();

        HashSet<Long> ids = new HashSet<>();
        for (CompletableFuture<Long> futureId : futureIds) {
            Assert.assertTrue(ids.add(futureId.get()), String.format("Expected %d to be a unique id", futureId.get()));
        }
        Assert.assertEquals(ids.size(), expectedNumIds * numBrokers);
    }

    @Test
    public void tooManyConcurrentNewProducersShouldFail() throws Exception {
        long blockSize = ProducerIdManagerImpl.PID_BLOCK_SIZE;
        int brokerId = 1;
        // Initialize a fake metadata store such that the futures are not completed. This will allow
        // for low level control during this test.
        CompletableFuture<Optional<GetResult>> firstGetFuture = new CompletableFuture<>();
        CompletableFuture<Optional<GetResult>> secondGetFuture = new CompletableFuture<>();
        CompletableFuture<Stat> firstPutFuture = new CompletableFuture<>();
        // The value is not used, and we mock the get results, so the put is essentially ignored
        firstPutFuture.complete(new Stat("", 0, 0, 0, false, false));

        MetadataStoreExtended mockedMetadataStore = mock(MetadataStoreExtended.class);
        when(mockedMetadataStore.get(anyString())).thenReturn(firstGetFuture).thenReturn(secondGetFuture);
        when(mockedMetadataStore.put(anyString(), any(), any())).thenReturn(firstPutFuture);

        ProducerIdManagerImpl producerIdManager = new ProducerIdManagerImpl(brokerId, mockedMetadataStore);
        producerIdManager.initialize();
        // Relies on the fact that completing the future also triggers the callbacks to run
        firstGetFuture.complete(Optional.empty());
        List<CompletableFuture<Long>> futureIds = new ArrayList<>((int) blockSize + 1);

        // Create one blockSize worth of producer ids
        for (int i = 0; i < blockSize; i++) {
            Assert.assertEquals(producerIdManager.generateProducerId().get().intValue(), i);
        }

        // Now create callbacks for blockSize + 1 producer ids.
        for (int i = 0; i < blockSize + 1; i++) {
            futureIds.add(producerIdManager.generateProducerId());
        }

        // Relies on the fact that completing the future also triggers the callbacks to run
        ProducerIdManagerImpl.ProducerIdBlock zeroBlock = ProducerIdManagerImpl.ProducerIdBlock
                .builder()
                .brokerId(brokerId)
                .blockStartId(0L)
                .blockEndId(ProducerIdManagerImpl.PID_BLOCK_SIZE - 1)
                .build();
        // This stat is not actually used
        Stat stat = new Stat("", 0, 0, 0, false, false);
        GetResult result = new GetResult(ProducerIdManagerImpl.generateProducerIdBlockJson(zeroBlock), stat);
        secondGetFuture.complete(Optional.of(result));

        int countFailed = 0;
        HashSet<Long> set = new HashSet<>();
        for (CompletableFuture<Long> id : futureIds) {
            if (id.isDone()) {
              if (id.isCompletedExceptionally()) {
                  countFailed++;
              } else {
                  set.add(id.get());
              }
            } else {
                Assert.fail();
            }
        }

        Assert.assertEquals(countFailed, 1, "Only one producer id should have failed");
        Assert.assertEquals(set.size(), blockSize, "Ensures all ids are unique and that no extra ids were created.");
    }

}
