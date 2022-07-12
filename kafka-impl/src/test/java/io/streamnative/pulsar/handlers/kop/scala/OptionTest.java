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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class OptionTest {

    @Test
    public void testGetter() {
        // Option#of is equivalent to Optional#ofNullable
        Option<Integer> option = Option.of(null);
        assertSame(option, Option.empty());
        assertFalse(option.isDefined());
        assertTrue(option.isEmpty());
        assertNull(option.get());

        option = Option.of(1);
        assertTrue(option.isDefined());
        assertFalse(option.isEmpty());
        assertEquals(option.get(), Integer.valueOf(1));
    }

    @Test
    public void testMatch() {
        final AtomicBoolean empty = new AtomicBoolean(false);
        final AtomicInteger value = new AtomicInteger(-1);

        Option<Integer> option = Option.empty();
        option.match(() -> empty.set(true), value::set);
        assertTrue(empty.get());
        assertEquals(value.intValue(), -1);

        empty.set(false);
        option = Option.of(1);
        option.match(() -> empty.set(true), value::set);
        assertFalse(empty.get());
        assertEquals(value.intValue(), 1);
    }

    @Test
    public void testMap() {
        Option<Integer> optInt = Option.empty();
        Option<String> optString = optInt.map(i -> "msg-" + i);
        assertSame(optString, Option.empty());

        optInt = Option.of(1);
        optString = optInt.map(i -> "msg-" + i);
        assertEquals(optString.get(), "msg-1");
    }

    @Test
    public void testGetOrElse() {
        Option<Integer> option = Option.empty();
        assertEquals(option.getOrElse(() -> 1), Integer.valueOf(1));
        option = Option.of(2);
        assertEquals(option.getOrElse(() -> 1), Integer.valueOf(2));
    }
}
