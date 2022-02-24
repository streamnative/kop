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
package io.streamnative.pulsar.handlers.kop;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.testng.annotations.Test;

public class ListPairTest {

    @Test
    public void testGetter() {
        ListPair<Integer> pair = ListPair.of(Collections.emptyMap());
        assertEquals(pair.getSuccessfulList(), Collections.emptyList());
        assertEquals(pair.getFailedList(), Collections.emptyList());

        pair = ListPair.of(null);
        assertEquals(pair.getSuccessfulList(), Collections.emptyList());
        assertEquals(pair.getFailedList(), Collections.emptyList());

        final Map<Boolean, List<Integer>> data = new HashMap<>();
        data.put(true, Arrays.asList(1, 2, 3));
        pair = ListPair.of(data);
        assertEquals(pair.getSuccessfulList(), Arrays.asList(1, 2, 3));
        assertEquals(pair.getFailedList(), Collections.emptyList());

        data.remove(true);
        data.put(false, Arrays.asList(4, 5));
        pair = ListPair.of(data);
        assertEquals(pair.getSuccessfulList(), Collections.emptyList());
        assertEquals(pair.getFailedList(), Arrays.asList(4, 5));

        data.put(true, Arrays.asList(6, 7));
        assertEquals(pair.getSuccessfulList(), Arrays.asList(6, 7));
        assertEquals(pair.getFailedList(), Arrays.asList(4, 5));
    }

    @Test
    public void testSplit() {
        final ListPair<Integer> pair = ListPair.split(Stream.of(1, 2, 3, 4, 5), i -> i % 2 == 0);
        assertEquals(pair.getSuccessfulList(), Arrays.asList(2, 4));
        assertEquals(pair.getFailedList(), Arrays.asList(1, 3, 5));
    }

    @Test
    public void testMap() {
        final ListPair<String> pair = ListPair.split(Stream.of(1, 2, 3), i -> i % 2 == 0)
                .map(i -> "msg-" + i);
        assertEquals(pair.getSuccessfulList(), Collections.singletonList("msg-2"));
        assertEquals(pair.getFailedList(), Arrays.asList("msg-1", "msg-3"));
    }
}
