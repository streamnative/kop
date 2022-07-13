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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class EitherTest {

    @Test
    public void testMatch() {
        final AtomicInteger leftResult = new AtomicInteger(0);
        final AtomicInteger rightResult = new AtomicInteger(0);
        Either<Integer, Integer> either = Either.left(100);
        either.match(leftResult::set, rightResult::set);
        assertEquals(leftResult.intValue(), 100);
        assertEquals(rightResult.intValue(), 0);

        either = Either.right(200);
        either.match(leftResult::set, rightResult::set);
        assertEquals(leftResult.intValue(), 100);
        assertEquals(rightResult.intValue(), 200);
    }

    @Test
    public void testMap() {
        Either<Integer, Integer> either0 = Either.left(1);
        final Either<Integer, ?> either1 = either0.map(i -> "msg-" + i);
        assertSame(either0, either1);

        either0 = Either.right(2);
        final Either<Integer, String> either2 = either0.map(i -> "msg-" + i);
        assertNull(either2.getLeft());
        assertEquals(either2.getRight(), "msg-2");
    }
}
