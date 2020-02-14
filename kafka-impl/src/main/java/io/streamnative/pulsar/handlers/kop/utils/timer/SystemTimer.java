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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.streamnative.pulsar.handlers.kop.utils.timer.TimerTaskList.TimerTaskEntry;
import java.util.Objects;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;

/**
 * A system timer implementation.
 */
@Slf4j
@ThreadSafe
public class SystemTimer implements Timer {

    /**
     * Create a system timer builder.
     *
     * @return a system timer builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder to build a system timer.
     */
    public static class Builder {

        private String executorName;
        private long tickMs = 1;
        private int wheelSize = 20;
        private long startMs = Time.SYSTEM.hiResClockMs();

        private Builder() {}

        public Builder executorName(String executorName) {
            this.executorName = executorName;
            return this;
        }

        public Builder tickMs(long tickMs) {
            this.tickMs = tickMs;
            return this;
        }

        public Builder wheelSize(int wheelSize) {
            this.wheelSize = wheelSize;
            return this;
        }

        public Builder startMs(long startMs) {
            this.startMs = startMs;
            return this;
        }

        public SystemTimer build() {
            Objects.requireNonNull(executorName, "No executor name is provided");

            return new SystemTimer(
                executorName,
                tickMs,
                wheelSize,
                startMs
            );
        }

    }

    private final ExecutorService taskExecutor;
    private final DelayQueue<TimerTaskList> delayQueue;
    private final AtomicInteger taskCounter;
    private final TimingWheel timingWheel;

    // Locks used to protect data structures while ticking
    private final ReentrantReadWriteLock readWriteLock;
    private final Lock readLock;
    private final Lock writeLock;
    private final Consumer<TimerTaskEntry> reinsert;

    private SystemTimer(String executorName,
                        long tickMs,
                        int wheelSize,
                        long startMs) {
        this.taskExecutor = Executors.newFixedThreadPool(
            1, new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("system-timer-%d")
                .build()
        );
        this.delayQueue = new DelayQueue();
        this.taskCounter = new AtomicInteger(0);
        this.timingWheel = new TimingWheel(
            tickMs,
            wheelSize,
            startMs,
            taskCounter,
            delayQueue
        );
        this.readWriteLock = new ReentrantReadWriteLock();
        this.readLock = readWriteLock.readLock();
        this.writeLock = readWriteLock.writeLock();
        this.reinsert = timerTaskEntry -> addTimerTaskEntry(timerTaskEntry);
    }

    @Override
    public void add(TimerTask timerTask) {
        readLock.lock();
        try {
            addTimerTaskEntry(new TimerTaskEntry(
                timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs()
            ));
        } finally {
            readLock.unlock();
        }
    }

    private void addTimerTaskEntry(TimerTaskEntry timerTaskEntry) {
        if (!timingWheel.add(timerTaskEntry)) {
            // Already expired or cancelled
            if (!timerTaskEntry.cancelled()) {
                taskExecutor.submit(timerTaskEntry.timerTask());
            }
        }
    }

    @SneakyThrows
    @Override
    public boolean advanceClock(long timeoutMs) {
        TimerTaskList bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (null != bucket) {
            writeLock.lock();
            try {
                while (null != bucket) {
                    timingWheel.advanceClock(bucket.getExpiration());
                    bucket.flush(reinsert);
                    bucket = delayQueue.poll();
                }
            } finally {
                writeLock.unlock();
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int size() {
        return taskCounter.get();
    }

    @Override
    public void shutdown() {
        taskExecutor.shutdown();
    }

}
