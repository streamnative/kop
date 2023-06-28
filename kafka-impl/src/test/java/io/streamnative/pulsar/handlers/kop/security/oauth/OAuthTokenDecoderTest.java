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
package io.streamnative.pulsar.handlers.kop.security.oauth;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.io.IOException;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;


/**
 * Test {@link OAuthTokenDecoder}.
 */
public class OAuthTokenDecoderTest {

    @Test
    public void testDecode() throws IOException {
        Pair<String, ExtensionTokenData> result = OAuthTokenDecoder.decode("my-token");
        assertEquals(result.getLeft(), "my-token");
        assertNull(result.getRight());
        result = OAuthTokenDecoder.decode("my-token"
                + OAuthTokenDecoder.EXTENSION_DATA_DELIMITER
                + "eyJ0ZW5hbnQiOiJteS10ZW5hbnQiLCJncm91cElkIjoibXktZ3JvdXAtaWQifQ==");
        assertEquals(result.getLeft(), "my-token");
        assertEquals(result.getRight(), new ExtensionTokenData("my-tenant", "my-group-id"));
    }
}
