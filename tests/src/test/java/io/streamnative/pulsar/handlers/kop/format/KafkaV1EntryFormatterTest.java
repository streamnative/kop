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
package io.streamnative.pulsar.handlers.kop.format;

import org.testng.annotations.Test;

/**
 * Test for entryFormat=kafka.
 */
public class KafkaV1EntryFormatterTest extends EntryFormatterTestBase {

    public KafkaV1EntryFormatterTest() {
        super("kafka");
    }

    @Test(timeOut = 20000, enabled = false)
    public void testChangeKafkaEntryFormat() throws Exception {
        super.testChangeKafkaEntryFormat();
    }

}
