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
package io.streamnative.pulsar.handlers.kop.utils;

/**
 * A mutable cell that holds a value of type `Long`.
 * One should generally prefer using value-based programming (i.e.
 * passing and returning `Long` values), but this class can be useful in some scenarios.
 * <p>
 * Unlike `AtomicLong`, this class is not thread-safe and there are no atomicity guarantees.
 */
public class LongRef {

    private long value;

    public LongRef(long value) {
        this.value = value;
    }

    public long addAndGet(long delta) {
        value += delta;
        return value;
    }

    public long getAndAdd(long delta) {
        long result = value;
        value += delta;
        return result;
    }

    public long getAndIncrement() {
        long result = value;
        value += 1;
        return result;
    }

    public long incrementAndGet() {
        value += 1;
        return value;
    }

    public long getAndDecrement() {
        long result = value;
        value -= 1;
        return result;
    }

    public long decrementAndGet() {
        value -= 1;
        return value;
    }

    public long value() {
        return value;
    }
}
