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

import io.streamnative.pulsar.handlers.kop.utils.timer.TimerTaskList.TimerTaskEntry;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * A mock implementation of {@link Timer}.
 */
@Accessors(fluent = true)
public class MockTimer implements Timer {

    @Getter
    private final MockTime time = new MockTime();
    private final PriorityQueue<TimerTaskEntry> taskQueue = new PriorityQueue<>(Comparator.reverseOrder());

    @Override
    public void add(TimerTask timerTask) {
        if (timerTask.delayMs <= 0) {
            timerTask.run();
        } else {
            synchronized (taskQueue) {
                taskQueue.add(
                    new TimerTaskEntry(
                        timerTask, timerTask.delayMs + time.milliseconds()));
            }
        }
    }

    @Override
    public boolean advanceClock(long timeoutMs) {
        time.sleep(timeoutMs);

        boolean executed = false;
        final long now = time.milliseconds();
        boolean hasMore = true;

        while (hasMore) {
            hasMore = false;
            TimerTaskEntry head;
            synchronized (taskQueue) {
                head = taskQueue.peek();
                if (null != head && now > head.expirationMs()) {
                    head = taskQueue.poll();
                    hasMore = !taskQueue.isEmpty();
                } else {
                    head = null;
                }
            }
            if (null != head) {
                if (!head.cancelled()) {
                    TimerTask task = head.timerTask();
                    task.run();
                    executed = true;
                }
            }
        }

        return executed;
    }

    @Override
    public int size() {
        synchronized (taskQueue) {
            return taskQueue.size();
        }
    }

    @Override
    public void shutdown() {
        // no-op
    }
}
