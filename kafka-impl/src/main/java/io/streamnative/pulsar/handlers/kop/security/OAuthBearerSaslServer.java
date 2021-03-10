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

import java.util.Arrays;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * The SaslServer implementation for SASL/OAUTHBEARER.
 */
public class OAuthBearerSaslServer implements SaslServer {

    public static final String OAUTHBEARER_MECHANISM = "OAUTHBEARER";

    private boolean complete;
    private String authorizationId;
    // TODO: constructor

    @Override
    public String getMechanismName() {
        return OAUTHBEARER_MECHANISM;
    }

    @Override
    public byte[] evaluateResponse(byte[] response) throws SaslException {
        // TODO:
        complete = true;
        authorizationId = "example";
        return new byte[0];
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public String getAuthorizationID() {
        return authorizationId;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }
        return Arrays.copyOfRange(incoming, offset, offset + len);
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }
        return Arrays.copyOfRange(outgoing, offset, offset + len);
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        return null;
    }

    @Override
    public void dispose() throws SaslException {

    }
}
