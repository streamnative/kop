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

import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;

import io.streamnative.pulsar.handlers.kop.SaslAuth;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Testing the deserialization of the SASL's payload.
 */
@Slf4j
public class SaslUtilsTest {

    @Test(timeOut = 2000)
    public void testDecodingSasl() throws Exception {
        byte[] message = this.saslMessage("authorizationID", "user", "token:my-awesome-token");
        SaslAuth result = SaslUtils.parseSaslAuthBytes(message);
        assertEquals(result.getUsername(), "user");
        assertEquals(result.getAuthMethod(), "token");
        assertEquals(result.getAuthData(), "my-awesome-token");
    }

    @DataProvider(name = "malformedSasl")
    public static Object[][] malformedSasl() {
        return new Object[][] {
            { "tenant/ns", "mytoken"},
            { "tenant/ns", ":"},
            { "tenant/ns", "a:"},
            { "tenant/ns", ":a"},
            { "", "a:a"},
            { "s", "aa"},
        };
    }

    @Test(timeOut = 20000, dataProvider = "malformedSasl")
    public void testMalformedSasl(String user, String password) {
        try {
            byte[] message = this.saslMessage("authorizationID", user, password);
            SaslUtils.parseSaslAuthBytes(message);
            fail("should have failed");
        } catch (Exception e) {}
    }

    /**
     * Taken from https://github.com/apache/kafka/blob/2.4/clients/src/test/java/org/apache/kafka/common/security/plain/internals/PlainSaslServerTest.java#L112.
     */
    private byte[] saslMessage(String authorizationId, String userName, String password) {
        String nul = "\u0000";
        String message = String.format("%s%s%s%s%s", authorizationId, nul, userName, nul, password);
        return message.getBytes(StandardCharsets.UTF_8);
    }
}
