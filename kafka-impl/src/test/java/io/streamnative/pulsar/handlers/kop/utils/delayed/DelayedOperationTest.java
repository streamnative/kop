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
package io.streamnative.pulsar.handlers.kop.utils.delayed;

import static io.streamnative.pulsar.handlers.kop.utils.CoreUtils.inLock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.kop.utils.TestUtils;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.SneakyThrows;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link DelayedOperation}.
 */
public class DelayedOperationTest {

    /**
     * A mock delayed operation.
     */
    static class MockDelayedOperation extends DelayedOperation {

        private final Optional<Lock> responseLockOpt;
        boolean completable = false;

        protected MockDelayedOperation(long delayMs) {
            this(delayMs, Optional.empty(), Optional.empty());
        }

        protected MockDelayedOperation(long delayMs,
                                       Optional<Lock> lockOpt,
                                       Optional<Lock> responseLockOpt) {
            super(delayMs, lockOpt);
            this.responseLockOpt = responseLockOpt;
        }

        public synchronized void awaitExpiration() throws InterruptedException {
            wait();
        }

        @Override
        public void onExpiration() {
            // no-op
        }

        @Override
        public void onComplete() {
            responseLockOpt.map(lock -> {
                if (!lock.tryLock()) {
                    throw new IllegalStateException("Response callback lock could not be acquired in callback");
                }
                return null;
            });
            synchronized (this) {
                notify();
            }
        }

        @Override
        public boolean tryComplete(boolean notify) {
            if (completable) {
                return forceComplete();
            } else {
                return false;
            }
        }
    }

    static class TestDelayOperation extends MockDelayedOperation {

        private final int index;
        private final Object key;
        private final AtomicInteger completionAttemptsRemaining;
        private final int maxDelayMs;

        TestDelayOperation(int index,
                           int completionAttempts,
                           int maxDelayMs) {
            super(10000L);
            this.index = index;
            this.key = "key" + index;
            this.maxDelayMs = maxDelayMs;
            this.completionAttemptsRemaining = new AtomicInteger(completionAttempts);
        }

        @SneakyThrows
        @Override
        public boolean tryComplete(boolean notify) {
            boolean shouldComplete = completable;
            Thread.sleep(ThreadLocalRandom.current().nextInt(maxDelayMs));
            if (shouldComplete) {
                return forceComplete();
            } else {
                return false;
            }
        }
    }

    DelayedOperationPurgatory<MockDelayedOperation> purgatory = null;
    ScheduledExecutorService executorService = null;

    @Before
    public void setup() {
        purgatory = DelayedOperationPurgatory.<MockDelayedOperation>builder()
            .purgatoryName("mock")
            .build();
    }

    @After
    public void teardown() {
        purgatory.shutdown();
        if (null != executorService) {
            executorService.shutdown();
        }
    }

    @Test
    public void testRequestSatisfaction() {
        MockDelayedOperation r1 = new MockDelayedOperation(100000L);
        MockDelayedOperation r2 = new MockDelayedOperation(100000L);
        assertEquals(
            "With no waiting requests, nothing should be satisfied",
            0, purgatory.checkAndComplete("test1"));
        assertFalse(
            "r1 not satisfied and hence watched",
            purgatory.tryCompleteElseWatch(r1, Lists.newArrayList("test1")));
        assertEquals(
            "Still nothing satisfied",
            0, purgatory.checkAndComplete("test1"));
        assertFalse(
            "r2 not satisfied and hence watched",
            purgatory.tryCompleteElseWatch(r2, Lists.newArrayList("test2")));
        assertEquals(
            "Still nothing satisfied",
            0, purgatory.checkAndComplete("test2"));
        r1.completable = true;
        assertEquals(
            "r1 satisfied",
            1, purgatory.checkAndComplete("test1"));
        assertEquals(
            "Nothing satisfied",
            0, purgatory.checkAndComplete("test1"));
        r2.completable = true;
        assertEquals(
            "r2 satisfied",
            1, purgatory.checkAndComplete("test2"));
        assertEquals(
            "Nothing satisfied",
            0, purgatory.checkAndComplete("test2"));
    }

    @Test
    public void testRequestExpiry() throws Exception {
        long expiration = 20L;
        long start = Time.SYSTEM.hiResClockMs();
        MockDelayedOperation r1 = new MockDelayedOperation(expiration);
        MockDelayedOperation r2 = new MockDelayedOperation(200000L);
        assertFalse(
            "r1 not satisfied and hence watched",
            purgatory.tryCompleteElseWatch(r1, Lists.newArrayList("test1")));
        assertFalse(
            "r2 not satisfied and hence watched",
            purgatory.tryCompleteElseWatch(r2, Lists.newArrayList("test2")));
        r1.awaitExpiration();
        long elapsed = Time.SYSTEM.hiResClockMs() - start;
        assertTrue
            ("r1 completed due to expiration",
            r1.isCompleted());
        assertFalse("r2 hasn't completed", r2.isCompleted());
        assertTrue(
            "Time for expiration $elapsed should at least " + expiration,
            elapsed >= expiration);
    }

    @Test
    public void testRequestPurge() {
        MockDelayedOperation r1 = new MockDelayedOperation(100000L);
        MockDelayedOperation r2 = new MockDelayedOperation(100000L);
        MockDelayedOperation r3 = new MockDelayedOperation(100000L);
        purgatory.tryCompleteElseWatch(r1, Lists.newArrayList("test1"));
        purgatory.tryCompleteElseWatch(r2, Lists.newArrayList("test1", "test2"));
        purgatory.tryCompleteElseWatch(r3, Lists.newArrayList("test1", "test2", "test3"));

        assertEquals(
            "Purgatory should have 3 total delayed operations",
            3, purgatory.delayed());
        assertEquals(
            "Purgatory should have 6 watched elements",
            6, purgatory.watched());

        // complete the operations, it should immediately be purged from the delayed operation
        r2.completable = true;
        r2.tryComplete(false);
        assertEquals(
            "Purgatory should have 2 total delayed operations instead of " + purgatory.delayed(),
            2, purgatory.delayed());

        r3.completable = true;
        r3.tryComplete(false);
        assertEquals(
            "Purgatory should have 1 total delayed operations instead of " + purgatory.delayed(),
            1, purgatory.delayed());

        // checking a watch should purge the watch list
        purgatory.checkAndComplete("test1");
        assertEquals(
            "Purgatory should have 4 watched elements instead of " + purgatory.watched(),
            4, purgatory.watched());

        purgatory.checkAndComplete("test2");
        assertEquals(
            "Purgatory should have 2 watched elements instead of " + purgatory.watched(),
            2, purgatory.watched());

        purgatory.checkAndComplete("test3");
        assertEquals(
            "Purgatory should have 1 watched elements instead of " + purgatory.watched(),
            1, purgatory.watched());
    }

    @Test
    public void shouldCancelForKeyReturningCancelledOperations() {
        purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Lists.newArrayList("key"));
        purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Lists.newArrayList("key"));
        purgatory.tryCompleteElseWatch(new MockDelayedOperation(10000L), Lists.newArrayList("key2"));

        List<MockDelayedOperation> cancelledOperations = purgatory.cancelForKey("key");
        assertEquals(2, cancelledOperations.size());
        assertEquals(1, purgatory.delayed());
        assertEquals(1, purgatory.watched());
    }

    @Test
    public void shouldReturnNilOperationsOnCancelForKeyWhenKeyDoesntExist() {
        List<MockDelayedOperation> cancelledOperations = purgatory.cancelForKey("key");
        assertTrue(cancelledOperations.isEmpty());
    }

    /**
     * Verify that if there is lock contention between two threads attempting to complete,
     * completion is performed without any blocking in either thread.
     */
    @Test
    public void testTryCompleteLockContention() throws Exception {
        executorService = Executors.newSingleThreadScheduledExecutor();
        AtomicInteger completionAttemptsRemaining = new AtomicInteger(Integer.MAX_VALUE);
        Semaphore tryCompleteSemaphore = new Semaphore(1);
        String key = "key";

        MockDelayedOperation op = new MockDelayedOperation(100000L) {
            @SneakyThrows
            @Override
            public boolean tryComplete(boolean notify) {
                boolean shouldComplete = completionAttemptsRemaining.decrementAndGet() <= 0;
                tryCompleteSemaphore.acquire();
                try {
                    if (shouldComplete) {
                        return forceComplete();
                    } else {
                        return false;
                    }
                } finally {
                    tryCompleteSemaphore.release();
                }
            }
        };

        purgatory.tryCompleteElseWatch(op, Lists.newArrayList(key));
        completionAttemptsRemaining.set(2);
        tryCompleteSemaphore.acquire();
        Future<?> future = runOnAnotherThread(() -> purgatory.checkAndComplete(key), false);
        TestUtils.waitUntilTrue(
            () -> tryCompleteSemaphore.hasQueuedThreads(),
            () -> "Not attempting to complete",
            10000,
            200);
        purgatory.checkAndComplete(key); // this should not block even though lock is not free
        assertFalse("Operation should not have completed", op.isCompleted());
        tryCompleteSemaphore.release();
        future.get(10, TimeUnit.SECONDS);
        assertTrue("Operation should have completed", op.isCompleted());
    }

    /**
     * Test `tryComplete` with multiple threads to verify that there are no timing windows
     * when completion is not performed even if the thread that makes the operation completable
     * may not be able to acquire the operation lock. Since it is difficult to test all scenarios,
     * this test uses random delays with a large number of threads.
     */
    @Test
    public void testTryCompleteWithMultipleThreads() {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(20);
        this.executorService = executor;
        Random random = ThreadLocalRandom.current();
        int maxDelayMs = 10;
        final int completionAttempts = 20;

        List<TestDelayOperation> ops = IntStream.range(0, 100).mapToObj(index -> {
            TestDelayOperation op = new TestDelayOperation(index, completionAttempts, maxDelayMs);
            purgatory.tryCompleteElseWatch(op, Lists.newArrayList(op.key));
            return op;
        }).collect(Collectors.toList());

        List<Future<?>> futures = IntStream.rangeClosed(1, completionAttempts)
            .mapToObj(i ->
                ops.stream().map(
                    op -> scheduleTryComplete(op, random.nextInt(maxDelayMs)))
                .collect(Collectors.toList()))
            .flatMap(List::stream)
            .collect(Collectors.toList());
        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                // no-op
            }
        });

        ops.forEach(op -> assertTrue("Operation should have completed", op.isCompleted()));
    }

    Future<?> scheduleTryComplete(TestDelayOperation op, long delayMs) {
        return executorService.schedule(() -> {
            if (op.completionAttemptsRemaining.decrementAndGet() == 0) {
                op.completable = true;
            }
            purgatory.checkAndComplete(op.key);
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testDelayedOperationLock() throws Exception {
        verifyDelayedOperationLock(() -> new MockDelayedOperation(100000L), false);
    }

    @Test
    public void testDelayedOperationLockOverride() throws Exception {
        verifyDelayedOperationLock(() -> {
            ReentrantLock lock = new ReentrantLock();
            return new MockDelayedOperation(100000L, Optional.of(lock), Optional.of(lock));
        }, false);

        verifyDelayedOperationLock(() -> new MockDelayedOperation(
            100000L,
            Optional.empty(),
            Optional.of(new ReentrantLock())
        ), true);
    }

    void verifyDelayedOperationLock(Supplier<MockDelayedOperation> mockDelayedOperation, boolean mismatchedLocks)
        throws Exception {
        String key = "key";
        executorService = Executors.newSingleThreadScheduledExecutor();

        Function<Integer, List<MockDelayedOperation>> createDelayedOperations = count ->
            IntStream.rangeClosed(1, count).mapToObj(i -> {
                MockDelayedOperation op = mockDelayedOperation.get();
                purgatory.tryCompleteElseWatch(op, Lists.newArrayList(key));
                assertFalse("Not completable", op.isCompleted());
                return op;
            }).collect(Collectors.toList());

        Function<Integer, List<MockDelayedOperation>> createCompletableOperations = count ->
            IntStream.rangeClosed(1, count).mapToObj(i -> {
                MockDelayedOperation op = mockDelayedOperation.get();
                op.completable = true;
                return op;
            }).collect(Collectors.toList());

        BiFunction<List<MockDelayedOperation>, List<MockDelayedOperation>, Void> checkAndComplete =
            (completableOps, expectedComplete) -> {
                completableOps.forEach(op -> op.completable = true);
                int completed = purgatory.checkAndComplete(key);
                assertEquals(expectedComplete.size(), completed);
                expectedComplete.forEach(op -> assertTrue(
                    "Should have completed",
                    op.isCompleted()
                ));
                Set<MockDelayedOperation> expectedNotComplete = completableOps.stream().collect(Collectors.toSet());
                expectedComplete.forEach(op -> expectedNotComplete.remove(op));
                expectedNotComplete.forEach(op -> assertFalse("Should not have completed", op.isCompleted()));
                return null;
            };

        // If locks are free all completable operations should complete
        List<MockDelayedOperation> ops = createDelayedOperations.apply(2);
        checkAndComplete.apply(ops, ops);

        // Lock held by current thread, completable operations should complete
        ops = createDelayedOperations.apply(2);
        final List<MockDelayedOperation> ops2 = ops;
        inLock(ops.get(1).lock, () -> {
            checkAndComplete.apply(ops2, ops2);
            return null;
        });

        // Lock held by another thread, should not block, only operations that can be
        // locked without blocking on the current thread should complete
        ops = createDelayedOperations.apply(2);
        final List<MockDelayedOperation> ops3 = ops;
        runOnAnotherThread(() -> ops3.get(0).lock.lock(), true);
        try {
            checkAndComplete.apply(ops, Lists.newArrayList(ops.get(1)));
        } finally {
            runOnAnotherThread(() -> ops3.get(0).lock.unlock(), true);
            checkAndComplete.apply(Lists.newArrayList(ops.get(0)), Lists.newArrayList(ops.get(0)));
        }

        // Lock acquired by response callback held by another thread, should not block
        // if the response lock is used as operation lock, only operations
        // that can be locked without blocking on the current thread should complete
        ops = createDelayedOperations.apply(2);
        final List<MockDelayedOperation> ops4 = ops;
        ops.get(0).responseLockOpt.map(lock -> {
            try {
                runOnAnotherThread(() -> lock.lock(), true);
                try {
                    try {
                        checkAndComplete.apply(ops4, Lists.newArrayList(ops4.get(1)));
                        assertFalse("Should have failed with mismatched locks", mismatchedLocks);
                    } catch (IllegalStateException e) {
                        assertTrue("Should not have failed with valid locks", mismatchedLocks);
                    }
                } finally {
                    runOnAnotherThread(() -> lock.unlock(), true);
                    checkAndComplete.apply(Lists.newArrayList(ops4.get(0)), Lists.newArrayList(ops4.get(0)));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return null;
        });

        // Immediately completable operations should complete without locking
        ops = createCompletableOperations.apply(2);
        ops.forEach(op -> {
            assertTrue("Should have completed", purgatory.tryCompleteElseWatch(op, Lists.newArrayList(key)));
            assertTrue("Should have completed", op.isCompleted());
        });
    }

    private Future<?> runOnAnotherThread(Runnable f, boolean shouldComplete) throws Exception {
        Future<?> future = executorService.submit(f);
        if (shouldComplete) {
            future.get();
        } else {
            assertFalse("Should not have completed", future.isDone());
        }
        return future;
    }

}
