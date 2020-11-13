/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.naming.AuthenticationException;

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
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.api.AuthData;

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

    private final BrokerService brokerService;
    private final Set<String> allowedMechanisms;
    private State state = State.HANDSHAKE_OR_VERSIONS_REQUEST;
    private AuthData authData = null;

    public SaslAuthenticator(BrokerService brokerService, Set<String> allowedMechanisms) {
        this.brokerService = brokerService;
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
                handleSaslToken(header, request, response);
                break;
            default:
                break;
        }
    }

    public boolean complete() {
        return state == State.COMPLETE;
    }

    public AuthData getAuthData() {
        return complete() ? authData : null;
    }

    public void reset() {
        state = State.HANDSHAKE_OR_VERSIONS_REQUEST;
        authData = null;
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

    private void handleSaslToken(RequestHeader header,
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
        SaslAuth saslAuth = null;
        try {
            saslAuth = SaslUtils.parseSaslAuthBytes(Utils.toArray(saslAuthenticateRequest.saslAuthBytes()));
        } catch (IOException e) {
            responseFuture.complete(new SaslAuthenticateResponse(Errors.SASL_AUTHENTICATION_FAILED, e.getMessage()));
            return;
        }
        AuthenticationProvider authenticationProvider =
                brokerService.getAuthenticationService().getAuthenticationProvider(saslAuth.getAuthMethod());
        if (authenticationProvider == null) {
            AuthenticationException e = new AuthenticationException(
                    "No AuthenticationProvider found for method " + saslAuth.getAuthMethod());
            responseFuture.complete(request.getErrorResponse(e));
            throw e;
        }

        authData = AuthData.of(saslAuth.getAuthData().getBytes(UTF_8));
        setState(State.COMPLETE);
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
            // TODO: support more mechanisms
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
