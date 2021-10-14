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

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Basic end-to-end test with `entryFormat=kafka`.
 */
@Slf4j
public class BasicEndToEndV1KafkaTest extends BasicEndToEndKafkaTest {

    public BasicEndToEndV1KafkaTest() {
        super("kafka");
    }

    @DataProvider(name = "enableBatching")
    public static Object[][] enableBatching() {
        return new Object[][]{ { true }, { false } };
    }

    @Test(timeOut = 20000)
    public void testNullValueMessages() throws Exception {
        super.testNullValueMessages();
    }

    @Test(timeOut = 20000)
    public void testDeleteClosedTopics() throws Exception {
        super.testDeleteClosedTopics();
    }

    @Test(timeOut = 30000)
    public void testPollEmptyTopic() throws Exception {
        super.testPollEmptyTopic();
    }

    @Test(timeOut = 20000, dataProvider = "enableBatching")
    public void testPulsarProduceKafkaConsume(boolean enableBatching) throws Exception {
        super.testPulsarProduceKafkaConsume(enableBatching);
    }

    @Test(timeOut = 20000)
    public void testMixedProduceKafkaConsume() throws Exception {
        super.testMixedProduceKafkaConsume();
    }
}
