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

import static io.streamnative.pulsar.handlers.kop.security.SaslAuthenticator.AUTH_DATA_SOURCE_PROP;
import static io.streamnative.pulsar.handlers.kop.security.SaslAuthenticator.USER_NAME_PROP;

import io.streamnative.pulsar.handlers.kop.KafkaServiceConfiguration;
import io.streamnative.pulsar.handlers.kop.SaslAuth;
import io.streamnative.pulsar.handlers.kop.utils.SaslUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.naming.AuthenticationException;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;

/**
 * The SaslServer implementation for SASL/PLAIN.
 */
@Slf4j
public class PlainSaslServer implements SaslServer {

    public static final String PLAIN_MECHANISM = "PLAIN";

    private final AuthenticationService authenticationService;
    private final KafkaServiceConfiguration config;

    private boolean complete;
    private String authorizationId;
    private String username;
    private AuthenticationDataSource authDataSource;
    private Set<String> proxyRoles;

    public PlainSaslServer(AuthenticationService authenticationService,
                           KafkaServiceConfiguration config,
                           Set<String> proxyRoles) {
        this.authenticationService = authenticationService;
        this.config = config;
        this.proxyRoles = proxyRoles;
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
        username = saslAuth.getUsername();

        AuthenticationProvider authenticationProvider =
                authenticationService.getAuthenticationProvider(saslAuth.getAuthMethod());
        if (authenticationProvider == null) {
            throw new SaslException("No AuthenticationProvider found for method " + saslAuth.getAuthMethod());
        }

        try {
            final AuthData authData = AuthData.of(saslAuth.getAuthData().getBytes(StandardCharsets.UTF_8));
            final AuthenticationState authState = authenticationProvider.newAuthState(authData, null, null);
            authState.authenticateAsync(authData).get(config.getRequestTimeoutMs(), TimeUnit.MILLISECONDS);
            final String role = authState.getAuthRole();
            if (StringUtils.isEmpty(role)) {
                throw new AuthenticationException("Role cannot be empty.");
            }
            if (proxyRoles != null && proxyRoles.contains(authState.getAuthRole())) {
                // the Proxy passes the OriginalPrincipal as "username"
                authorizationId = saslAuth.getUsername();
                authDataSource = authState.getAuthDataSource();
                username = null; // PULSAR TENANT
                if (authorizationId.contains("/")) {
                    // the proxy uses username/originalPrincipal as "username"
                    int lastSlash = authorizationId.lastIndexOf('/');
                    username = authorizationId.substring(lastSlash + 1);
                    authorizationId = authorizationId.substring(0, lastSlash);
                }
                log.info("Authenticated Proxy role {} as user role {} tenant (username) {}", authState.getAuthRole(),
                        authorizationId, username);
                if (proxyRoles.contains(authorizationId)) {
                    throw new SaslException("The proxy (with role " + authState.getAuthRole()
                            + ") tried to forward another proxy user (with role " + authorizationId + ")");
                }
            } else {
                authorizationId = authState.getAuthRole();
                authDataSource = authState.getAuthDataSource();
                log.info("Authenticated User {}, AuthDataSource {}", authorizationId, authDataSource);
            }
            complete = true;
            return new byte[0];
        } catch (AuthenticationException | ExecutionException | InterruptedException | TimeoutException e) {
            throw new SaslException(e.getMessage());
        }
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public String getAuthorizationID() {
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }
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
        if (USER_NAME_PROP.equals(propName)) {
            return username;
        }
        if (AUTH_DATA_SOURCE_PROP.equals(propName)) {
            return authDataSource;
        }
        return null;
    }

    @Override
    public void dispose() throws SaslException {
    }
}
