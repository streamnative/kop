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

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;

public class CoreUtilsTest {

    @Test
    public void testListOperations() {
        final List<Integer> integers = Arrays.asList(1, 2, 3);
        final List<String> strings = CoreUtils.listToList(integers, i -> "msg-" + i);
        assertEquals(strings, Arrays.asList("msg-1", "msg-2", "msg-3"));

        final Map<Integer, String> map = CoreUtils.listToMap(integers, i -> "value-" + i);
        final Map<Integer, String> expectedMap = new HashMap<>();
        expectedMap.put(1, "value-1");
        expectedMap.put(2, "value-2");
        expectedMap.put(3, "value-3");
        assertEquals(map, expectedMap);

        assertEquals(
                CoreUtils.mapToList(map, (i, s) -> i + "-" + s),
                Arrays.asList("1-value-1", "2-value-2", "3-value-3")
        );
    }

    @Test
    public void testWaitForAll() throws ExecutionException, InterruptedException {
        final List<CompletableFuture<Integer>> futures = Arrays.asList(
                CompletableFuture.completedFuture(1),
                CompletableFuture.completedFuture(2),
                CompletableFuture.completedFuture(3));
        final List<String> strings = CoreUtils.waitForAll(futures,
                integers -> CoreUtils.listToList(integers, Object::toString)).get();
        assertEquals(strings, Arrays.asList("1", "2", "3"));
    }
}
