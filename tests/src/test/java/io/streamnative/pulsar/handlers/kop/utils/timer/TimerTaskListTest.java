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

import static org.junit.Assert.assertEquals;

import io.streamnative.pulsar.handlers.kop.utils.timer.TimerTaskList.TimerTaskEntry;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

/**
 * Unit test {@link TimerTaskList}.
 */
public class TimerTaskListTest {

    /**
     * Test task.
     */
    private static class TestTask extends TimerTask {

        protected TestTask(long delayMs) {
            super(delayMs);
        }

        @Override
        public void run() {

        }

    }

    private int size(TimerTaskList list) {
        AtomicInteger count = new AtomicInteger(0);
        list.forEach(ignored -> count.incrementAndGet());
        return count.get();
    }

    @Test
    public void testAll() {
        AtomicInteger sharedCounter = new AtomicInteger(0);
        TimerTaskList list1 = new TimerTaskList(sharedCounter);
        TimerTaskList list2 = new TimerTaskList(sharedCounter);
        TimerTaskList list3 = new TimerTaskList(sharedCounter);

        List<TimerTask> tasks = IntStream.rangeClosed(1, 10).mapToObj(i -> {
            TestTask task = new TestTask(0L);
            list1.add(new TimerTaskEntry(task, 10L));
            assertEquals(i, sharedCounter.get());
            return task;
        }).collect(Collectors.toList());

        assertEquals(tasks.size(), sharedCounter.get());

        // reinserting the existing tasks shouldn't change the task count.
        tasks.subList(0, 4).forEach(task -> {
            int prevCount = sharedCounter.get();
            // new TimerTaskEntry(task) will remove the existing entry from the list
            list2.add(new TimerTaskEntry(task, 10L));
            assertEquals(prevCount, sharedCounter.get());
        });
        assertEquals(10 - 4, size(list1));
        assertEquals(4, size(list2));
        assertEquals(tasks.size(), sharedCounter.get());

        // reinserting the existing tasks shouldn't change the task count
        tasks.subList(4, 10).forEach(task -> {
            int prevCount = sharedCounter.get();
            // new TimerTaskEntry(task) will remove the existing entry from the list
            list3.add(new TimerTaskEntry(task, 10L));
            assertEquals(prevCount, sharedCounter.get());
        });
        assertEquals(0, size(list1));
        assertEquals(4, size(list2));
        assertEquals(6, size(list3));
        assertEquals(tasks.size(), sharedCounter.get());

        // cancel tasks in the lists
        list1.forEach(TimerTask::cancel);
        assertEquals(0, size(list1));
        assertEquals(4, size(list2));
        assertEquals(6, size(list3));

        list2.forEach(TimerTask::cancel);
        assertEquals(0, size(list1));
        assertEquals(0, size(list2));
        assertEquals(6, size(list3));

        list3.forEach(TimerTask::cancel);
        assertEquals(0, size(list1));
        assertEquals(0, size(list2));
        assertEquals(0, size(list3));
    }

}
