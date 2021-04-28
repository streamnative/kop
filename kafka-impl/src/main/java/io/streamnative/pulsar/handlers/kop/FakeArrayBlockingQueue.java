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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simulate the ArrayBlockingQueue, but we don't store the actual element.
 */
public class FakeArrayBlockingQueue {

    private final ReentrantLock lock = new ReentrantLock(false);
    private final Condition notFull = lock.newCondition();
    private int count = 0;
    private final int capacity;

    public FakeArrayBlockingQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException();
        }
        this.capacity = capacity;
    }

    public void put() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (count == capacity) {
                notFull.await();
            }
            count++; // enqueue
        } finally {
            lock.unlock();
        }
    }

    public void poll() {
        lock.lock();
        try {
            if (count == 0) {
                throw new IllegalStateException("poll() is called when count is 0");
            }
            count--; // dequeue
            notFull.signal();
        } finally {
            lock.unlock();
        }
    }

    public void clear() {
        lock.lock();
        try {
            count = 0;
            notFull.signal();
        } finally {
            lock.unlock();
        }
    }
}
