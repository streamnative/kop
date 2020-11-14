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

import static java.nio.charset.StandardCharsets.UTF_8;

import io.streamnative.pulsar.handlers.kop.SaslAuth;
import io.streamnative.pulsar.handlers.kop.utils.SaslUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.naming.AuthenticationException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.utils.Utils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.policies.data.AuthAction;

/**
 * The SASL authenticator.
 */
@Slf4j
public class SaslAuthenticator {

    private enum State {
        HANDSHAKE_OR_VERSIONS_REQUEST,
        HANDSHAKE_REQUEST,
        AUTHENTICATE,
        COMPLETE
    }

    /**
     * The exception to indicate that the authenticator's state is illegal when processing some requests.
     */
    public static class IllegalStateException extends AuthenticationException {

        public IllegalStateException(String msg, State actualState, State expectedState) {
            super(msg + " actual state: " + actualState + " expected state: " + expectedState);
        }
    }

    /**
     * The exception to indicate that the client provided mechanism is not supported.
     */
    public static class UnsupportedSaslMechanismException extends AuthenticationException {

        public UnsupportedSaslMechanismException(String mechanism) {
            super("SASL mechanism '" + mechanism + "' requested by client is not supported");
        }
    }

    private final AuthenticationService authenticationService;
    private final PulsarAdmin admin;
    private final Set<String> allowedMechanisms;
    private State state = State.HANDSHAKE_OR_VERSIONS_REQUEST;
    @Getter
    private AuthenticationState authState = null;
    @Getter
    private AuthenticationDataSource authDataSource = null;
    @Getter
    private String authRole = null;

    public SaslAuthenticator(PulsarService pulsarService, Set<String> allowedMechanisms) throws PulsarServerException {
        this.authenticationService = pulsarService.getBrokerService().getAuthenticationService();
        this.admin = pulsarService.getAdminClient();
        this.allowedMechanisms = allowedMechanisms;
    }

    public void authenticate(RequestHeader header,
                             AbstractRequest request,
                             CompletableFuture<AbstractResponse> response) throws AuthenticationException {
        switch (state) {
            case HANDSHAKE_OR_VERSIONS_REQUEST:
            case HANDSHAKE_REQUEST:
                handleKafkaRequest(header, request, response);
                break;
            case AUTHENTICATE:
                handleAuthenticate(header, request, response);
                break;
            default:
                break;
        }
    }

    public boolean complete() {
        return state == State.COMPLETE;
    }

    public void reset() {
        state = State.HANDSHAKE_OR_VERSIONS_REQUEST;
        authState = null;
        authDataSource = null;
        authRole = null;
    }

    private void setState(State state) {
        this.state = state;
        if (log.isDebugEnabled()) {
            log.debug("Set SaslAuthenticator's state to {}", state);
        }
    }

    private void handleKafkaRequest(RequestHeader header,
                                    AbstractRequest request,
                                    CompletableFuture<AbstractResponse> responseFuture) throws AuthenticationException {
        ApiKeys apiKey = header.apiKey();
        if (apiKey == ApiKeys.API_VERSIONS) {
            handleApiVersionsRequest((ApiVersionsRequest) request, responseFuture);
        } else if (apiKey == ApiKeys.SASL_HANDSHAKE) {
            // If SaslHandshakeRequest version is v0, a series of SASL client and server tokens corresponding to the
            // mechanism are sent as opaque packets without wrapping the messages with Kafka protocol headers.
            // However, KoP always parses the header before a request is processed. Therefore, SaslHandshakeRequest v0
            // shouldn't be supported, we need to send error response to client during handshake.
            if (header.apiVersion() < 1) {
                AuthenticationException e = new AuthenticationException("KoP doesn't support SaslHandshake v0");
                responseFuture.complete(request.getErrorResponse(e));
                throw e;
            }
            handleHandshakeRequest((SaslHandshakeRequest) request, responseFuture);
        } else {
            throw new AuthenticationException("Unexpected Kafka request of type " + apiKey + " during SASL handshake");
        }
    }

    private void handleAuthenticate(RequestHeader header,
                                    AbstractRequest request,
                                    CompletableFuture<AbstractResponse> responseFuture) throws AuthenticationException {
        ApiKeys apiKey = header.apiKey();
        short version = header.apiVersion();
        if (apiKey != ApiKeys.SASL_AUTHENTICATE) {
            AuthenticationException e = new AuthenticationException(
                    "Unexpected Kafka request of type " + apiKey + " during SASL authentication");
            responseFuture.complete(request.getErrorResponse(e));
            throw e;
        }
        if (!apiKey.isVersionSupported(version)) {
            throw new AuthenticationException("Version " + version + " is not supported for apiKey " + apiKey);
        }

        SaslAuthenticateRequest saslAuthenticateRequest = (SaslAuthenticateRequest) request;
        SaslAuth saslAuth;
        try {
            saslAuth = SaslUtils.parseSaslAuthBytes(Utils.toArray(saslAuthenticateRequest.saslAuthBytes()));
        } catch (IOException e) {
            responseFuture.complete(new SaslAuthenticateResponse(Errors.SASL_AUTHENTICATION_FAILED, e.getMessage()));
            return;
        }

        AuthenticationProvider authenticationProvider =
                authenticationService.getAuthenticationProvider(saslAuth.getAuthMethod());
        if (authenticationProvider == null) {
            AuthenticationException e = new AuthenticationException(
                    "No AuthenticationProvider found for method " + saslAuth.getAuthMethod());
            responseFuture.complete(request.getErrorResponse(e));
            throw e;
        }

        try {
            authState = authenticationProvider.newAuthState(
                    AuthData.of(saslAuth.getAuthData().getBytes(UTF_8)), null, null);
        } catch (AuthenticationException e) {
            responseFuture.complete(new SaslAuthenticateResponse(Errors.SASL_AUTHENTICATION_FAILED, e.getMessage()));
            return;
        }
        authDataSource = authState.getAuthDataSource();
        authRole = authState.getAuthRole();

        // TODO: we need to let KafkaRequestHandler do the authorization works. Here we just check the permissions
        //  of the namespace, which is the namespace. See https://github.com/streamnative/kop/issues/236
        final String namespace = saslAuth.getUsername();
        try {
            Map<String, Set<AuthAction>> permissions = admin.namespaces().getPermissions(namespace);
            if (permissions.containsKey(authRole)) {
                responseFuture.complete(new SaslAuthenticateResponse(Errors.NONE, ""));
                setState(State.COMPLETE);
            } else {
                responseFuture.complete(new SaslAuthenticateResponse(
                        Errors.SASL_AUTHENTICATION_FAILED,
                        "Role: " + authRole + " is not allowed on namespace " + namespace));
            }
        } catch (PulsarAdminException e) {
            AuthenticationException authException =
                    new AuthenticationException("Failed to get permissions of " + namespace + ": " + e.getMessage());
            responseFuture.complete(
                    new SaslAuthenticateResponse(Errors.SASL_AUTHENTICATION_FAILED, authException.getMessage()));
            responseFuture.complete(request.getErrorResponse(authException));
        }
    }

    private void handleApiVersionsRequest(ApiVersionsRequest request,
                                          CompletableFuture<AbstractResponse> responseFuture)
            throws AuthenticationException {
        if (state != State.HANDSHAKE_OR_VERSIONS_REQUEST) {
            throw new IllegalStateException(
                    "Receive ApiVersions request", state, State.HANDSHAKE_OR_VERSIONS_REQUEST);
        }
        if (request.hasUnsupportedRequestVersion()) {
            responseFuture.complete(request.getErrorResponse(0, Errors.UNSUPPORTED_VERSION.exception()));
        } else {
            responseFuture.complete(ApiVersionsResponse.defaultApiVersionsResponse());
            // Handshake request must be followed by the ApiVersions request
            setState(State.HANDSHAKE_REQUEST);
        }
    }

    private void handleHandshakeRequest(SaslHandshakeRequest request,
                                        CompletableFuture<AbstractResponse> responseFuture)
            throws AuthenticationException {

        final String mechanism = request.mechanism();
        if (mechanism != null && !mechanism.equals("PLAIN")) {
            // TODO: support more mechanisms, see https://github.com/streamnative/kop/issues/235
            AuthenticationException e = new AuthenticationException("KoP only support PLAIN mechanism");
            responseFuture.complete(request.getErrorResponse(e));
            throw e;
        }
        if (allowedMechanisms.contains(mechanism)) {
            if (log.isDebugEnabled()) {
                log.debug("Using SASL mechanism '{}' provided by client", mechanism);
            }
            responseFuture.complete(new SaslHandshakeResponse(Errors.NONE, allowedMechanisms));
            setState(State.AUTHENTICATE);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("SASL mechanism '{}' requested by client is not supported", mechanism);
            }
            responseFuture.complete(new SaslHandshakeResponse(Errors.UNSUPPORTED_SASL_MECHANISM, allowedMechanisms));
            throw new UnsupportedSaslMechanismException(mechanism);
        }
    }
}
