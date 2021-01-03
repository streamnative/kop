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
package io.streamnative.pulsar.handlers.kop.streams;

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.streams.StreamsConfig;
import org.testng.annotations.Test;

/**
 * BugStreamTableJoinTest with `entryFormat=pulsar`.
 */
public class BugStreamTableJoinPulsarTest extends BugStreamTableJoinTestBase {

    public BugStreamTableJoinPulsarTest() {
        super("pulsar");
    }

    @Test(timeOut = 30000)
    void testLeft() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appID + "-left");
        leftStream.leftJoin(rightTable, valueJoiner).to(outputTopic);

        final List<List<String>> expectedResult = Arrays.asList(
                null,
                null,
                // TODO: The expected result should be Collections.singletonList("A-null"). It looks like the two tokens
                //  were not joined to one word.
                Arrays.asList("-null", "A-"),
                null
        );
        runTest(expectedResult);
    }
}
