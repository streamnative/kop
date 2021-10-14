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
 * Test for entryFormat=mixed_kafka.
 */
public class KafkaMixedEntryFormatterTest extends EntryFormatterTestBase {

    public KafkaMixedEntryFormatterTest() {
        super("mixed_kafka");
    }

    @Test(timeOut = 20000)
    public void testChangeKafkaEntryFormat() throws Exception {
        super.testChangeKafkaEntryFormat();
    }

}
