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

import io.streamnative.pulsar.handlers.kop.SaslAuth;
import io.streamnative.pulsar.handlers.kop.utils.SaslUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import javax.naming.AuthenticationException;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.api.AuthData;

/**
 * The SaslServer implementation for SASL/PLAIN.
 */
public class PlainSaslServer implements SaslServer {

    public static final String PLAIN_MECHANISM = "PLAIN";

    private final AuthenticationService authenticationService;
    private final PulsarAdmin admin;

    private boolean complete;
    private String authorizationId;

    public PlainSaslServer(AuthenticationService authenticationService, PulsarAdmin admin) {
        this.authenticationService = authenticationService;
        this.admin = admin;
    }

    @Override
    public String getMechanismName() {
        return PLAIN_MECHANISM;
    }

    @Override
    public byte[] evaluateResponse(byte[] response) throws SaslException {
        SaslAuth saslAuth;
        try {
            saslAuth = SaslUtils.parseSaslAuthBytes(response);
        } catch (IOException e) {
            throw new SaslException(e.getMessage());
        }

        AuthenticationProvider authenticationProvider =
                authenticationService.getAuthenticationProvider(saslAuth.getAuthMethod());
        if (authenticationProvider == null) {
            throw new SaslException("No AuthenticationProvider found for method " + saslAuth.getAuthMethod());
        }

        try {
            final AuthenticationState authState = authenticationProvider.newAuthState(
                    AuthData.of(saslAuth.getAuthData().getBytes(StandardCharsets.UTF_8)), null, null);
            final String role = authState.getAuthRole();
            if (StringUtils.isEmpty(role)) {
                throw new AuthenticationException("Role cannot be empty.");
            }

            authorizationId = authState.getAuthRole();
            complete = true;
            return new byte[0];
        } catch (AuthenticationException e) {
            throw new SaslException(e.getMessage());
        }
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
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }
        return null;
    }

    @Override
    public void dispose() throws SaslException {
    }
}
