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
package io.streamnative.kop.utils;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.Supplier;
import lombok.experimental.UtilityClass;

/**
 * Core utils.
 */
@UtilityClass
public final class CoreUtils {

    public static <T> T inLock(Lock lock, Supplier<T> supplier) {
        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    public static <T> T inReadLock(ReadWriteLock lock, Supplier<T> supplier) {
        return inLock(lock.readLock(), supplier);
    }

    public static <T> T inWriteLock(ReadWriteLock lock, Supplier<T> supplier) {
        return inLock(lock.writeLock(), supplier);
    }

    private CoreUtils() {}
}
