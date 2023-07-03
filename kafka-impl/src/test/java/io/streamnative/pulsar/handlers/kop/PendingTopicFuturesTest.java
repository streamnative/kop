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
package io.streamnative.pulsar.handlers.kop;

import static org.mockito.Mockito.mock;

import io.streamnative.pulsar.handlers.kop.storage.PartitionLog;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test for PendingTopicFutures.
 */
@Slf4j
public class PendingTopicFuturesTest {

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    private static <T> List<T> fill(int n, T element) {
        final List<T> list = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            list.add(element);
        }
        return list;
    }

    private static List<Integer> range(int start, int end) {
        final List<Integer> list = new ArrayList<>();
        for (int i = start; i < end; i++) {
            list.add(i);
        }
        return list;
    }

    @Test(timeOut = 10000)
    void testNormalComplete() throws ExecutionException, InterruptedException {
        final PendingTopicFutures pendingTopicFutures = new PendingTopicFutures(null);
        final CompletableFuture<PartitionLog> topicFuture = new CompletableFuture<>();
        final List<Integer> completedIndexes = new ArrayList<>();
        final List<Integer> changesOfPendingCount = new ArrayList<>();
        int randomNum = ThreadLocalRandom.current().nextInt(0, 9);

        for (int i = 0; i < 10; i++) {
            final int index = i;
            pendingTopicFutures.addListener(
                    topicFuture, ignored -> completedIndexes.add(index), (ignore) -> {});
            changesOfPendingCount.add(pendingTopicFutures.size());
            if (randomNum == i) {
                topicFuture.complete(mock(PartitionLog.class));
            }
        }

        // all futures are completed, the size becomes 0 again.
        Assert.assertEquals(pendingTopicFutures.waitAndGetSize(), 0);

        // assert all `normalComplete`s are called
        log.info("completedIndexes: {}", completedIndexes);
        Assert.assertEquals(completedIndexes, range(0, 10));

        // assert the last N futures are completed immediately
        log.info("Changes of pending futures' count: {}", changesOfPendingCount);
        final int index = changesOfPendingCount.indexOf(0);
        Assert.assertTrue(index > 0);
        Assert.assertEquals(changesOfPendingCount.subList(0, index), range(1, index + 1));
        Assert.assertEquals(changesOfPendingCount.subList(index, 10), fill(10 - index, 0));
    }

    @Test(timeOut = 10000)
    void testExceptionalComplete() throws ExecutionException, InterruptedException {
        final PendingTopicFutures pendingTopicFutures = new PendingTopicFutures(null);
        final CompletableFuture<PartitionLog> topicFuture = new CompletableFuture<>();
        final List<String> exceptionMessages = new ArrayList<>();
        final List<Integer> changesOfPendingCount = new ArrayList<>();

        int randomNum = ThreadLocalRandom.current().nextInt(0, 9);
        for (int i = 0; i < 10; i++) {
            pendingTopicFutures.addListener(topicFuture, topic -> {}, (ex) -> {
                exceptionMessages.add(ex.getMessage());
            });
            changesOfPendingCount.add(pendingTopicFutures.size());

            if (randomNum == i) {
                topicFuture.completeExceptionally(new RuntimeException("error"));
            }
        }

        // all futures are completed, the size becomes 0 again.
        Assert.assertEquals(pendingTopicFutures.waitAndGetSize(), 0);

        // assert all `exceptionalComplete`s are called
        log.info("exceptionMessages: {}", exceptionMessages);
        Assert.assertEquals(exceptionMessages, fill(10, "error"));

        // assert the last N futures are completed immediately
        log.info("Changes of pending futures' count: {}", changesOfPendingCount);
        final int index = changesOfPendingCount.indexOf(0);
        Assert.assertTrue(index > 0);
        Assert.assertEquals(changesOfPendingCount.subList(0, index), range(1, index + 1));
        Assert.assertEquals(changesOfPendingCount.subList(index, 10), fill(10 - index, 0));
    }

    @Test(timeOut = 10000)
    void testParallelAccess() throws ExecutionException, InterruptedException {
        final PendingTopicFutures pendingTopicFutures = new PendingTopicFutures(null);
        final CompletableFuture<PartitionLog> topicFuture = new CompletableFuture<>();
        final List<Integer> completedIndexes = new CopyOnWriteArrayList<>();
        int randomNum = ThreadLocalRandom.current().nextInt(0, 9);

        ExecutorService threadPool = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                final int index = i;
                futures.add(threadPool.submit(() -> {
                    pendingTopicFutures.addListener(
                            topicFuture, ignored -> completedIndexes.add(index), (ignore) -> {
                            });
                    if (randomNum == index) {
                        topicFuture.complete(mock(PartitionLog.class));
                    }
                }));
            }
            // verify everything worked well
            for (Future<?> f : futures) {
                f.get();
            }
        } finally {
            threadPool.shutdown();
        }

        // all futures are completed, the size becomes 0 again.
        Assert.assertEquals(pendingTopicFutures.waitAndGetSize(), 0);

        Collections.sort(completedIndexes);

        // assert all `normalComplete`s are called
        log.info("completedIndexes: {}", completedIndexes);
        Assert.assertEquals(completedIndexes, range(0, 10));
    }
}
