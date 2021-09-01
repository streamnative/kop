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
package io.streamnative.pulsar.handlers.kop.security;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test SaslAuthenticator.
 *
 * @see SaslAuthenticator
 */
public class SaslAuthenticatorTest {

    @Test
    public void testSizePrefixed() {
        final byte[] response = new byte[0];
        final ByteBuf byteBuf = SaslAuthenticator.sizePrefixed(ByteBuffer.wrap(response));
        Assert.assertEquals(byteBuf.readerIndex(), 0);
        Assert.assertEquals(byteBuf.writerIndex(), 0);
        Assert.assertEquals(byteBuf.capacity(), 4);
    }

}
