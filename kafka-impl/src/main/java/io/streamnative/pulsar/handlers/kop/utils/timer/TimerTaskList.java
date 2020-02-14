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
package io.streamnative.pulsar.handlers.kop.utils.timer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;

/**
 * The timer task list is a java implementation of Kafka implementation.
 */
@SuppressFBWarnings({
    "EQ_COMPARETO_USE_OBJECT_EQUALS",
    "HE_EQUALS_USE_HASHCODE"
})
@Slf4j
@ThreadSafe
public class TimerTaskList implements Delayed {

    private final AtomicInteger taskCounter;
    private final AtomicLong expiration;

    // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
    // root.next points to the head
    // root.prev points to the tail
    private final TimerTaskEntry root;

    public TimerTaskList(AtomicInteger taskCounter) {
        this.taskCounter = taskCounter;
        this.root = new TimerTaskEntry(null, -1);
        this.root.next = root;
        this.root.prev = root;
        this.expiration = new AtomicLong(-1L);
    }

    // Set the bucket's expiration time
    // Returns true if the expiration time is changed
    public boolean setExpiration(long expirationMs) {
        return expiration.getAndSet(expirationMs) != expirationMs;
    }

    // Get the bucket's expiration time
    public long getExpiration() {
        return expiration.get();
    }

    public synchronized void forEach(Consumer<TimerTask> f) {
        TimerTaskEntry entry = root.next;
        while (entry != root) {
            final TimerTaskEntry nextEntry = entry.next;
            if (!entry.cancelled()) {
                f.accept(entry.timerTask);
            }
            entry = nextEntry;
        }
    }

    // add a timer task entry to this list
    public void add(TimerTaskEntry timerTaskEntry) {
        boolean done = false;
        while (!done) {
            // Remove the timer task entry if it is already in any other list
            // We do this outside of the sync block below to avoid deadlocking.
            // We may retry until timerTaskEntry.list becomes null.
            timerTaskEntry.remove();

            synchronized (this) {
                synchronized (timerTaskEntry) {
                    if (timerTaskEntry.list == null) {
                        // put the timer task entry to the end of the list. (root.prev points to the tail entry)
                        TimerTaskEntry tail = root.prev;
                        timerTaskEntry.next = root;
                        timerTaskEntry.prev = tail;
                        timerTaskEntry.list = this;
                        tail.next = timerTaskEntry;
                        root.prev = timerTaskEntry;
                        taskCounter.incrementAndGet();
                        done = true;
                    }
                }
            }
        }
    }

    // Remove the specified timer task entry from this list
    public void remove(TimerTaskEntry timerTaskEntry) {
        synchronized (this) {
            synchronized (timerTaskEntry) {
                if (timerTaskEntry.list == this) {
                    timerTaskEntry.next.prev = timerTaskEntry.prev;
                    timerTaskEntry.prev.next = timerTaskEntry.next;
                    timerTaskEntry.next = null;
                    timerTaskEntry.prev = null;
                    timerTaskEntry.list = null;
                    taskCounter.decrementAndGet();
                }
            }
        }
    }

    // Remove all task entries and apply the supplied function to each of them
    public synchronized void flush(Consumer<TimerTaskEntry> f) {
        TimerTaskEntry head = root.next;
        while (head != root) {
            remove(head);
            f.accept(head);
            head = root.next;
        }
        expiration.set(-1L);
    }

    public long getDelay(TimeUnit unit) {
        return unit.convert(Math.max(getExpiration() - Time.SYSTEM.hiResClockMs(), 0), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        TimerTaskList other = (TimerTaskList) o;

        if (getExpiration() < other.getExpiration()) {
            return -1;
        } else if (getExpiration() > other.getExpiration()) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * A timer task entry in the timer task list.
     */
    @Accessors(fluent = true)
    protected static class TimerTaskEntry implements Comparable<TimerTaskEntry> {

        @Getter
        private final TimerTask timerTask;
        @Getter
        private final long expirationMs;
        private volatile TimerTaskList list = null;
        private TimerTaskEntry next = null;
        private TimerTaskEntry prev = null;

        public TimerTaskEntry(TimerTask timerTask,
                              long expirationMs) {
            this.timerTask = timerTask;
            this.expirationMs = expirationMs;
            // if this timerTask is already held by an existing timer task entry,
            // setTimerTaskEntry will remove it.
            if (null != timerTask) {
                timerTask.setTimerTaskEntry(this);
            }
        }

        public boolean cancelled() {
            return timerTask.getTimerTaskEntry() != this;
        }

        public void remove() {
            TimerTaskList currentList = list;
            // If remove is called when another thread is moving the entry from a task entry list to another,
            // this may fail to remove the entry due to the change of value of list. Thus, we retry until the
            // list becomes null. In a rare case, this thread sees null and exits the loop, but the other thread
            // insert the entry to another list later.
            while (currentList != null) {
                currentList.remove(this);
                currentList = list;
            }
        }

        @Override
        public int compareTo(TimerTaskEntry o) {
            return Long.compare(this.expirationMs, o.expirationMs);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TimerTaskEntry)) {
                return false;
            }
            TimerTaskEntry other = (TimerTaskEntry) obj;
            return compareTo(other) == 0
                && list == other.list
                && next == other.next
                && prev == other.prev
                && timerTask == other.timerTask;
        }
    }


}
