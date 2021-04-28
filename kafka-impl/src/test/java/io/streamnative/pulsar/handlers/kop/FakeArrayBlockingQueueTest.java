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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for FakeArrayBlockingQueueTest.
 */
public class FakeArrayBlockingQueueTest {

    private FakeArrayBlockingQueue createFullQueue(int capacity) throws InterruptedException {
        final FakeArrayBlockingQueue queue = new FakeArrayBlockingQueue(capacity);
        for (int i = 0; i < capacity; i++) {
            queue.put();
        }
        return queue;
    }

    @Test(timeOut = 10000)
    public void testClear() throws Exception {
        final int capacity = 100;
        final FakeArrayBlockingQueue queue = createFullQueue(capacity);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicBoolean completed = new AtomicBoolean(false);
        executor.execute(() -> {
            try {
                queue.put(); // blocked until clear() is called
            } catch (InterruptedException ignored) {
            }
            completed.set(true);
        });
        queue.clear();
        executor.awaitTermination(1, TimeUnit.SECONDS);
        executor.shutdownNow(); // cancel the current putAgain task

        for (int i = 0; i < capacity - 1; i++) {
            queue.put(); // never blocked
        }
    }

    @Test(timeOut = 10000)
    public void testPoll() throws Exception {
        final int capacity = 100;
        final FakeArrayBlockingQueue queue = createFullQueue(capacity);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        final AtomicBoolean completed = new AtomicBoolean(false);
        final Runnable putAgain = () -> {
            try {
                queue.put(); // blocked until poll() is called
            } catch (InterruptedException ignored) {
            }
            completed.set(true);
        };
        executor.execute(putAgain);
        executor.awaitTermination(1, TimeUnit.SECONDS);
        executor.shutdownNow(); // cancel the current putAgain task
        Assert.assertFalse(completed.get());

        executor = Executors.newSingleThreadExecutor();
        executor.execute(putAgain);
        queue.poll();
        executor.awaitTermination(1, TimeUnit.SECONDS);
        Assert.assertTrue(completed.get());

        for (int i = 0; i < capacity / 2; i++) {
            queue.poll();
        }
        for (int i = 0; i < capacity / 2; i++) {
            queue.put(); // never blocked
        }
    }

    @Test
    public void testExceptionCases() {
        try {
            new FakeArrayBlockingQueue(0);
            Assert.fail();
        } catch (IllegalArgumentException ignored) {
        }
        try {
            new FakeArrayBlockingQueue(-1);
            Assert.fail();
        } catch (IllegalArgumentException ignored) {
        }

        final FakeArrayBlockingQueue queue = new FakeArrayBlockingQueue(1);
        try {
            queue.poll();
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertEquals(e.getMessage(), "poll() is called when count is 0");
        }
    }
}
