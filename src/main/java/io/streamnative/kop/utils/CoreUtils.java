package io.streamnative.kop.utils;

import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

/**
 * Core utility functions.
 */
public final class CoreUtils {

    /**
     * Retrieve a value under the protection of a lock.
     *
     * @param lock the lock to protect the operation
     * @param supplier the supplier function to return the value
     * @return the value retrieved from the function
     */
    public static <T> T inLock(Lock lock, Supplier<T> supplier) {
        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    private CoreUtils() {}
}
