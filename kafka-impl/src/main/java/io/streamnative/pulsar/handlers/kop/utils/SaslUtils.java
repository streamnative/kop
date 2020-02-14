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

import static java.nio.charset.StandardCharsets.UTF_8;

import io.streamnative.pulsar.handlers.kop.SaslAuth;
import java.io.IOException;
import javax.security.sasl.SaslException;

/**
 * Provide convenient methods to decode SASL from Kafka.
 */
public class SaslUtils {

    /**
     * Decode SASL auth bytes.
     * @param buf of sasl auth
     * @throws IOException
     */
    public static SaslAuth parseSaslAuthBytes(byte[] buf) throws IOException {
        String[] tokens;
        tokens = new String(buf, UTF_8).split("\u0000");
        if (tokens.length != 3) {
            throw new SaslException("Invalid SASL/PLAIN response: expected 3 tokens, got " + tokens.length);
        }
        String username = tokens[1];
        String password = tokens[2];

        if (username.isEmpty()) {
            throw new IOException("Authentication failed: username not specified");
        }
        if (password.isEmpty()) {
            throw new IOException("Authentication failed: password not specified");
        }

        if (password.split(":").length != 2) {
            throw new IOException("Authentication failed: password not specified");
        }

        String authMethod = password.split(":")[0];
        if (authMethod.isEmpty()) {
            throw new IOException("Authentication failed: authMethod not specified");
        }

        password = password.split(":")[1];
        return new SaslAuth(username, authMethod, password);
    }
}
