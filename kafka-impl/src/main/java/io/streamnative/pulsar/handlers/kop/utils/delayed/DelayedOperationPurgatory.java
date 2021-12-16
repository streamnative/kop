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

import static com.google.common.base.Preconditions.checkArgument;
import static io.streamnative.pulsar.handlers.kop.utils.CoreUtils.inReadLock;
import static io.streamnative.pulsar.handlers.kop.utils.CoreUtils.inWriteLock;

import io.streamnative.pulsar.handlers.kop.utils.ShutdownableThread;
import io.streamnative.pulsar.handlers.kop.utils.timer.SystemTimer;
import io.streamnative.pulsar.handlers.kop.utils.timer.Timer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 */
@Slf4j
public class DelayedOperationPurgatory<T extends DelayedOperation> {

    public static <T extends DelayedOperation> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Builder to build a delayed operation purgatory.
     */
    public static class Builder<T extends DelayedOperation> {

        private String purgatoryName;
        private Timer timer;
        private int purgeInterval = 1000;
        private boolean reaperEnabled = true;
        private boolean timerEnabled = true;

        private Builder() {}

        public Builder<T> purgatoryName(String purgatoryName) {
            this.purgatoryName = purgatoryName;
            return this;
        }

        public Builder<T> timeoutTimer(Timer timer) {
            this.timer = timer;
            return this;
        }

        public Builder<T> purgeInterval(int purgeInterval) {
            this.purgeInterval = purgeInterval;
            return this;
        }

        public Builder<T> reaperEnabled(boolean reaperEnabled) {
            this.reaperEnabled = reaperEnabled;
            return this;
        }

        public Builder<T> timerEnabled(boolean timerEnabled) {
            this.timerEnabled = timerEnabled;
            return this;
        }

        public DelayedOperationPurgatory<T> build() {
            boolean ownTimer;
            if (null == timer) {
                ownTimer = true;
                timer = SystemTimer.builder().executorName(purgatoryName).build();
            } else {
                ownTimer = false;
            }
            return new DelayedOperationPurgatory<>(
                purgatoryName,
                timer,
                ownTimer,
                purgeInterval,
                reaperEnabled,
                timerEnabled
            );
        }
    }

    private final String purgatoryName;
    private final boolean ownTimer;
    private final Timer timeoutTimer;
    private final int purgeInterval;
    private final boolean reaperEnabled;
    private final boolean timerEnabled;

    /* a list of operation watching keys */
    private final ConcurrentMap<Object, Watchers> watchersForKey;

    private final ReentrantReadWriteLock removeWatchersLock = new ReentrantReadWriteLock();

    // the number of estimated total operations in the purgatory
    private final AtomicInteger estimatedTotalOperations = new AtomicInteger(0);

    /* background thread expiring operations that have timed out */
    private final ShutdownableThread expirationReaper;

    public DelayedOperationPurgatory(
        String purgatoryName,
        Timer timeoutTimer,
        boolean ownTimer,
        int purgeInterval,
        boolean reaperEnabled,
        boolean timerEnabled
    ) {
        this.purgatoryName = purgatoryName;
        this.timeoutTimer = timeoutTimer;
        this.ownTimer = ownTimer;
        this.purgeInterval = purgeInterval;
        this.reaperEnabled = reaperEnabled;
        this.timerEnabled = timerEnabled;

        this.watchersForKey = new ConcurrentHashMap<>();
        this.expirationReaper = new ShutdownableThread(
            String.format("ExpirationReaper-%s", purgatoryName)
        ) {
            @Override
            protected void doWork() {
                advanceClock(200L);
            }
        };

        if (reaperEnabled) {
            expirationReaper.start();
        }
    }

    /**
     * Check if the operation can be completed, if not watch it based on the given watch keys
     *
     * <p>Note that a delayed operation can be watched on multiple keys. It is possible that
     * an operation is completed after it has been added to the watch list for some, but
     * not all of the keys. In this case, the operation is considered completed and won't
     * be added to the watch list of the remaining keys. The expiration reaper thread will
     * remove this operation from any watcher list in which the operation exists.
     *
     * @param operation the delayed operation to be checked
     * @param watchKeys keys for bookkeeping the operation
     * @return true iff the delayed operations can be completed by the caller
     */
    public boolean tryCompleteElseWatch(T operation, List<Object> watchKeys) {
        checkArgument(!watchKeys.isEmpty(), "The watch key list can't be empty");

        // The cost of tryComplete() is typically proportional to the number of keys. Calling
        // tryComplete() for each key is going to be expensive if there are many keys. Instead,
        // we do the check in the following way. Call tryComplete(). If the operation is not completed,
        // we just add the operation to all keys. Then we call tryComplete() again. At this time, if
        // the operation is still not completed, we are guaranteed that it won't miss any future triggering
        // event since the operation is already on the watcher list for all keys. This does mean that
        // if the operation is completed (by another thread) between the two tryComplete() calls, the
        // operation is unnecessarily added for watch. However, this is a less severe issue since the
        // expire reaper will clean it up periodically.

        // At this point the only thread that can attempt this operation is this current thread
        // Hence it is safe to tryComplete() without a lock
        boolean isCompletedByMe = operation.tryComplete(false);
        if (isCompletedByMe) {
            return true;
        }

        boolean watchCreated = false;
        for (Object key : watchKeys) {
            // If the operation is already completed, stop adding it to the rest of the watcher list.
            if (operation.isCompleted()) {
                return false;
            }
            watchForOperation(key, operation);

            if (!watchCreated) {
                watchCreated = true;
                estimatedTotalOperations.incrementAndGet();
            }
        }

        isCompletedByMe = operation.maybeTryComplete(false);
        if (isCompletedByMe) {
            return true;
        }

        // if it cannot be completed by now and hence is watched, add to the expire queue also
        if (!operation.isCompleted()) {
            if (timerEnabled) {
                timeoutTimer.add(operation);
            }
            if (operation.isCompleted()) {
                // cancel the timer task
                operation.cancel();
            }
        }

        return false;
    }

    /**
     * Check if some delayed operations can be completed with the given watch key,
     * and if yes complete them.
     *
     * @return the number of completed operations during this process
     */
    public int checkAndComplete(Object key) {
        Watchers watchers = inReadLock(
            removeWatchersLock,
            () -> watchersForKey.get(key));
        if (null == watchers) {
            return 0;
        } else {
            return watchers.tryCompleteWatched();
        }
    }

    /**
     * Return the total size of watch lists the purgatory. Since an operation may be watched
     * on multiple lists, and some of its watched entries may still be in the watch lists
     * even when it has been completed, this number may be larger than the number of real operations watched
     */
    public int watched() {
        return allWatchers().stream().mapToInt(Watchers::countWatched).sum();
    }

    /**
     * Return the number of delayed operations in the expiry queue.
     */
    public int delayed() {
        return timeoutTimer.size();
    }

    /**
     * Cancel watching on any delayed operations for the given key. Note the operation will not be completed
     */
    public List<T> cancelForKey(Object key) {
        return inWriteLock(removeWatchersLock, () -> {
            Watchers watchers = watchersForKey.remove(key);
            if (watchers != null) {
                return watchers.cancel();
            } else {
                return Collections.emptyList();
            }
        });
    }
    /*
     * Return all the current watcher lists,
     * note that the returned watchers may be removed from the list by other threads
     */
    private Collection<Watchers> allWatchers() {
        return inReadLock(removeWatchersLock, () -> watchersForKey.values());
    }

    /*
     * Return the watch list of the given key, note that we need to
     * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
     */
    private void watchForOperation(Object key, T operation) {
        inReadLock(removeWatchersLock, () -> {
            watchersForKey.computeIfAbsent(key, (k) -> new Watchers(k))
                .watch(operation);
            return null;
        });
    }

    /**
     * Remove the key from watcher lists if its list is empty.
     */
    private void removeKeyIfEmpty(Object key, Watchers watchers) {
        inWriteLock(removeWatchersLock, () -> {
            // if the current key is no longer correlated to the watchers to remove, skip
            if (watchersForKey.get(key) != watchers) {
                return null;
            }

            if (watchers != null && watchers.isEmpty()) {
                watchersForKey.remove(key);
            }
            return null;
        });
    }

    /**
     * Shutdown the expire reaper thread.
     */
    public void shutdown() {
        if (reaperEnabled) {
            try {
                expirationReaper.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted at shutting down expiration reaper for {}", purgatoryName);
            }
        }
        if (ownTimer) {
            timeoutTimer.shutdown();
        }
    }

    /**
     * A linked list of watched delayed operations based on some key.
     */
    private class Watchers {

        private final Object key;
        private final ConcurrentLinkedQueue<T> operations = new ConcurrentLinkedQueue<>();

        Watchers(Object key) {
            this.key = key;
        }

        // count the current number of watched operations. This is O(n), so use isEmpty() if possible
        public int countWatched() {
            return operations.size();
        }

        public boolean isEmpty() {
            return operations.isEmpty();
        }

        // add the element to watch
        public void watch(T t) {
            operations.add(t);
        }

        // traverse the list and try to complete some watched elements
        public int tryCompleteWatched() {
            int completed = 0;

            Iterator<T> iter = operations.iterator();
            while (iter.hasNext()) {
                T curr = iter.next();
                if (curr.isCompleted()) {
                    // another thread has completed this operation, just remove it
                    iter.remove();
                } else if (curr.maybeTryComplete(true)) {
                    iter.remove();
                    completed += 1;
                }
            }

            if (operations.isEmpty()) {
                removeKeyIfEmpty(key, this);
            }

            return completed;
        }

        public List<T> cancel() {
            Iterator<T> iter = operations.iterator();
            List<T> cancelled = new ArrayList<>();
            while (iter.hasNext()) {
                T curr = iter.next();
                curr.cancel();
                iter.remove();
                cancelled.add(curr);
            }
            return cancelled;
        }

        // traverse the list and purge elements that are already completed by others
        int purgeCompleted() {
            int purged = 0;

            Iterator<T> iter = operations.iterator();
            while (iter.hasNext()) {
                T curr = iter.next();
                if (curr.isCompleted()) {
                    iter.remove();
                    purged += 1;
                }
            }

            if (operations.isEmpty()) {
                removeKeyIfEmpty(key, this);
            }

            return purged;
        }
    }

    public void advanceClock(long timeoutMs) {
        timeoutTimer.advanceClock(timeoutMs);

        // Trigger a purge if the number of completed but still being watched operations is larger than
        // the purge threshold. That number is computed by the difference btw the estimated total number of
        // operations and the number of pending delayed operations.
        if (estimatedTotalOperations.get() - delayed() > purgeInterval) {
            // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
            // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
            // a little overestimated total number of operations.
            estimatedTotalOperations.getAndSet(delayed());
            if (log.isDebugEnabled()) {
                log.debug("{} Begin purging watch lists", purgatoryName);
            }
            int purged = allWatchers().stream().mapToInt(Watchers::purgeCompleted).sum();
            if (log.isDebugEnabled()) {
                log.debug("{} Purged {} elements from watch lists.", purgatoryName, purged);
            }
        }
    }


}
