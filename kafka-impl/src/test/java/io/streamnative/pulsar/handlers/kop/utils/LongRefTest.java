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

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for LongRef.
 */
public class LongRefTest {

    @Test
    public void testBasicOperations() {
        long count = 100;
        LongRef longRef = new LongRef(count);
        Assert.assertEquals(count, longRef.value());

        // test increment
        Assert.assertEquals(++count, longRef.incrementAndGet());
        Assert.assertEquals(count++, longRef.getAndIncrement());
        Assert.assertEquals(count, longRef.value());

        // test decrement
        Assert.assertEquals(--count, longRef.decrementAndGet());
        Assert.assertEquals(count--, longRef.getAndDecrement());
        Assert.assertEquals(count, longRef.value());

        // test add delta
        long delta = 5;
        Assert.assertEquals(count += delta, longRef.addAndGet(delta));
        Assert.assertEquals(count, longRef.getAndAdd(delta));
        Assert.assertEquals(count += delta, longRef.value());

    }
}
