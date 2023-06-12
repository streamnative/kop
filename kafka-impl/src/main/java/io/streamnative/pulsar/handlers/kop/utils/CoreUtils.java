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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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

    public static <K, V> Map<Boolean, Map<K, V>> partition(Map<K, V> map,
                                                           Predicate<K> predicate) {
         return map.entrySet()
            .stream()
            .collect(Collectors.partitioningBy(
                e -> predicate.test(e.getKey()),
                Collectors.toMap(Entry::getKey, Entry::getValue)
            ));
    }

    public static <K, V1, V2> Map<K, V2> mapValue(Map<K, V1> map,
                                                  Function<V1, V2> func) {
        return map.entrySet()
            .stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> func.apply(e.getValue())
            ));
    }

    public static <K, V1, V2> Map<K, V2> mapKeyValue(Map<K, V1> map,
                                                     Function<Map.Entry<K, V1>, V2> func) {
        return map.entrySet()
            .stream()
            .collect(Collectors.toMap(
                e -> e.getKey(),
                e -> func.apply(e)
            ));
    }

    public static <R, T> List<R> listToList(final List<T> list,
                                            final Function<T, R> function) {
        return list.stream().map(function).collect(Collectors.toList());
    }

    public static <K, V> Map<K, V> listToMap(final List<K> list,
                                             final Function<K, V> function) {
        return list.stream().collect(Collectors.toMap(key -> key, function));
    }

    public static <K, V, R> List<R> mapToList(final Map<K, V> map,
                                              final BiFunction<K, V, R> function) {
        return map.entrySet().stream().map(e -> function.apply(e.getKey(), e.getValue())).collect(Collectors.toList());
    }

    public static <K1, K2, K3, V> Map<K2, Map<K3, V>> groupBy(final Map<K1, V> map,
                                                              final Function<K1, K2> groupFunction,
                                                              final Function<K1, K3> keyMapper) {
        return map.entrySet().stream().collect(Collectors.groupingBy(e -> groupFunction.apply(e.getKey()),
                Collectors.toMap(e -> keyMapper.apply(e.getKey()), Entry::getValue)));
    }

    public static <K1, K2, V> Map<K2, Map<K1, V>> groupBy(final Map<K1, V> map,
                                                          final Function<K1, K2> groupFunction) {
        return groupBy(map, groupFunction, key -> key);
    }

    public static <T> CompletableFuture<Void> waitForAll(final Collection<CompletableFuture<T>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    public static <T, R> CompletableFuture<R> waitForAll(final Collection<CompletableFuture<T>> futures,
                                                         final Function<List<T>, R> function) {
        return waitForAll(futures).thenApply(__ ->
                function.apply(futures.stream().map(CompletableFuture::join).collect(Collectors.toList())));
    }
}
