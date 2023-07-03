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

import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test {@link Timer}.
 */
@Slf4j
public class TimerTest {

    private static class TestTask extends TimerTask {
        private final int id;
        private final CountDownLatch latch;
        private final List<Integer> output;
        private final AtomicBoolean completed = new AtomicBoolean(false);

        public TestTask(long delayMs,
                        int id,
                        CountDownLatch latch,
                        List<Integer> output) {
            super(delayMs);
            this.id = id;
            this.latch = latch;
            this.output = output;
        }

        public void run() {
            if (completed.compareAndSet(false, true)) {
                synchronized (output) {
                    output.add(id);
                }
                latch.countDown();
            }
        }
    }

    private Timer timer = null;

    @BeforeMethod
    public void setup() {
        this.timer = SystemTimer.builder()
            .executorName("test")
            .tickMs(1)
            .wheelSize(3)
            .startMs(Time.SYSTEM.hiResClockMs())
            .build();
    }

    @AfterMethod
    public void teardown() {
        timer.shutdown();
    }

    @Test
    public void testAlreadyExpiredTask() {
        final List<Integer> output = new ArrayList<>();
        final List<CountDownLatch> latches = IntStream.range(-5, 0).mapToObj(i -> {
            CountDownLatch latch = new CountDownLatch(1);
            timer.add(new TestTask(i, i, latch, output));
            return latch;
        }).collect(Collectors.toList());

        timer.advanceClock(0);

        latches.forEach(latch -> {
            try {
                assertTrue("already expired tasks should run immediately", latch.await(3, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Should not reach here");
            }
        });

        assertEquals(
            "Output of already expired tasks",
            Lists.newArrayList(-5, -4, -3, -2, -1),
            output
        );
    }

    @Test
    public void testTaskExpiration() {
        final List<Integer> output = new ArrayList<>();
        final List<TestTask> tasks = new ArrayList<>();
        final List<Integer> ids = new ArrayList<>();
        final List<CountDownLatch> latches = IntStream.range(0, 5).mapToObj(i -> {
            CountDownLatch latch = new CountDownLatch(1);
            tasks.add(new TestTask(i, i, latch, output));
            ids.add(i);
            return latch;
        }).collect(Collectors.toList());
        latches.addAll(IntStream.range(10, 100).mapToObj(i -> {
            CountDownLatch latch = new CountDownLatch(1);
            tasks.add(new TestTask(i, i, latch, output));
            tasks.add(new TestTask(i, i, latch, output));
            ids.add(i);
            ids.add(i);
            return latch;
        }).collect(Collectors.toList()));
        latches.addAll(IntStream.range(100, 500).mapToObj(i -> {
            CountDownLatch latch = new CountDownLatch(1);
            tasks.add(new TestTask(i, i, latch, output));
            ids.add(i);
            return latch;
        }).collect(Collectors.toList()));

        // randomly submit requests
        tasks.forEach(task -> timer.add(task));

        while (timer.advanceClock(2000)) {}

        latches.forEach(latch -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Should not reach here");
            }
        });

        Collections.sort(ids);
        assertEquals(
            "output should match",
            ids,
            output
        );
    }

}
