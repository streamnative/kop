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

@Getter
public class Either<LeftT, RightT> {

    private final LeftT left;
    private final RightT right;

    public boolean isLeft() {
        return left != null;
    }

    public static <LeftT, RightT> Either<LeftT, RightT> left(final LeftT left) {
        return new Either<>(left, null);
    }

    public static <LeftT, RightT> Either<LeftT, RightT> right(final RightT right) {
        return new Either<>(null, right);
    }

    /**
     * Simulate pattern matching in Scala.
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
     * either.match(f1, f2);
     * ```
     *
     * @param leftConsumer
     * @param rightConsumer
     */
    public void match(final Consumer<LeftT> leftConsumer, final Consumer<RightT> rightConsumer) {
        if (left == null) {
            rightConsumer.accept(right);
        } else {
            leftConsumer.accept(left);
        }
    }

    public <T> Either<LeftT, T> map(final Function<RightT, T> function) {
        if (left == null) {
            return Either.right(function.apply(right));
        } else {
            @SuppressWarnings("unchecked")
            Either<LeftT, T> other = (Either<LeftT, T>) this;
            return other;
        }
    }

    protected Either(final LeftT left, final RightT right) {
        this.left = left;
        this.right = right;
    }
}
