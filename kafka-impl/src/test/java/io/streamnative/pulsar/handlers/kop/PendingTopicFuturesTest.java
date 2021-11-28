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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
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
        final CompletableFuture<Optional<PersistentTopic>> topicFuture = CompletableFuture.supplyAsync(() -> {
            sleep(800);
            return Optional.empty();
        });
        final List<Integer> completedIndexes = new ArrayList<>();
        final List<Integer> changesOfPendingCount = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            final int index = i;
            pendingTopicFutures.addListener(
                    topicFuture, ignored -> completedIndexes.add(index), new CompletableFuture<>());
            changesOfPendingCount.add(pendingTopicFutures.size());
            sleep(234);
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
        final CompletableFuture<Optional<PersistentTopic>> topicFuture = CompletableFuture.supplyAsync(() -> {
            sleep(800);
            throw new RuntimeException("error");
        });
        final List<String> exceptionMessages = new ArrayList<>();
        final List<Integer> changesOfPendingCount = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            CompletableFuture<Long> longCompletableFuture = new CompletableFuture<>();
            longCompletableFuture.exceptionally(ex -> {
                exceptionMessages.add(ex.getMessage());
                return null;
            });
            pendingTopicFutures.addListener(topicFuture, topic -> {}, longCompletableFuture);
            changesOfPendingCount.add(pendingTopicFutures.size());
            sleep(200);
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
}
