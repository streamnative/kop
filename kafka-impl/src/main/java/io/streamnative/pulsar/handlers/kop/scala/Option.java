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
package io.streamnative.pulsar.handlers.kop.scala;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class Option<T> {

    private static final Option<?> EMPTY = new Option<>(null);
    private final T value;

    public static <T> Option<T> of(final T value) {
        return (value != null) ? new Option<>(value) : empty();
    }

    public static <T> Option<T> empty() {
        @SuppressWarnings("unchecked")
        Option<T> option = (Option<T>) EMPTY;
        return option;
    }

    /**
     * Simulate pattern matching in Scala.
     *
     * In scala, pattern matching of an `Option` instance is like:
     *
     * ```scala
     * option match {
     *     case None => f1()
     *     case Some(value) => f2(value)
     * }
     * ```
     *
     * With this class, you can write:
     *
     * ```java
     * option.match(f1, f2);
     * ```
     *
     * @param noneCallback
     * @param someCallback
     */
    public void match(final Runnable noneCallback, final Consumer<T> someCallback) {
        if (value != null) {
            someCallback.accept(value);
        } else {
            noneCallback.run();
        }
    }

    public boolean isDefined() {
        return !isEmpty();
    }

    public boolean isEmpty() {
        return value == null;
    }

    public T get() {
        return value;
    }

    public <R> Option<R> map(final Function<T, R> function) {
        return isDefined() ? Option.of(function.apply(value)) : empty();
    }

    public T getOrElse(final Supplier<T> supplier) {
        return isDefined() ? value : supplier.get();
    }

    private Option(T value) {
        this.value = value;
    }
}
