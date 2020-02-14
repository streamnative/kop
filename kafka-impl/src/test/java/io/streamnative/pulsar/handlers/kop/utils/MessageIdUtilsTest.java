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

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.testng.annotations.Test;

/**
 * Validate TopicNameUtils.
 */
@Slf4j
public class MessageIdUtilsTest {

    @Test(timeOut = 20000)
    public void testMessageIdConvert() throws Exception {
        long ledgerId = 77777;
        long entryId = 7777;
        PositionImpl position = new PositionImpl(ledgerId, entryId);

        long offset = MessageIdUtils.getOffset(ledgerId, entryId);
        PositionImpl position1 = MessageIdUtils.getPosition(offset);

        assertEquals(position, position1);
    }
}
