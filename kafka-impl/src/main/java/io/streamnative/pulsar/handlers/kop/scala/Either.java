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
import lombok.Getter;

/**
 * A simple Java migration of <a href="https://www.scala-lang.org/api/2.13.6/scala/util/Either.html">Scala Either</a>.
 *
 * Since there is no pattern matching before Java 16, this class provides a {@link Either#match} method to simulate it.
 *
 * In scala, pattern matching of an `Either` instance is like:
 *
 * ```scala
 * either match {
 *     case Left(left) => f1(left)
 *     case Right(right) => f2(right)
 * }
 * ```
 *
 * With this class, you can write:
 *
 * ```java
 * // f1(left) will be called if left is not null, otherwise f2(right) will be called.
 * either.match(f1, f2);
 * ```
 *
 * In scala, you can create `Either` instances by `Left(x)` or `Right(x)`. With this class, you should call the static
 * methods ({@link Either#left} and {@link Either#right}) to achieve the same goal.
 *
 * @param <V> the type of the 1st possible value (the left side)
 * @param <W> the type of the 2nd possible value (the right side)
 */
@Getter
public class Either<V, W> {

    private final V left;
    private final W right;

    public boolean isLeft() {
        return left != null;
    }

    public static <V, W> Either<V, W> left(final V left) {
        return new Either<>(left, null);
    }

    public static <V, W> Either<V, W> right(final W right) {
        return new Either<>(null, right);
    }

    public void match(final Consumer<V> leftConsumer, final Consumer<W> rightConsumer) {
        if (left == null) {
            rightConsumer.accept(right);
        } else {
            leftConsumer.accept(left);
        }
    }

    public <R> Either<V, R> map(final Function<W, R> function) {
        if (left == null) {
            return Either.right(function.apply(right));
        } else {
            @SuppressWarnings("unchecked")
            Either<V, R> other = (Either<V, R>) this;
            return other;
        }
    }

    private Either(final V left, final W right) {
        this.left = left;
        this.right = right;
    }
}
