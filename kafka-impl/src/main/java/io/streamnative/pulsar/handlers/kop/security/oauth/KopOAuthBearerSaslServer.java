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

import static io.streamnative.pulsar.handlers.kop.security.SaslAuthenticator.AUTH_DATA_SOURCE_PROP;
import static io.streamnative.pulsar.handlers.kop.security.SaslAuthenticator.USER_NAME_PROP;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerClientInitialResponse;

@Slf4j
public class KopOAuthBearerSaslServer implements SaslServer {

    // Copy from OAuthBearerSaslClient.BYTE_CONTROL_A
    private static final byte BYTE_CONTROL_A = (byte) 0x01;
    private static final String NEGOTIATED_PROPERTY_KEY_TOKEN = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM + ".token";
    private static final String INTERNAL_ERROR_ON_SERVER =
            "Authentication could not be performed due to an internal error on the server";

    private final AuthenticateCallbackHandler callbackHandler;
    private final String defaultKafkaMetadataTenant;

    private boolean complete;
    private KopOAuthBearerToken tokenForNegotiatedProperty = null;
    private String errorMessage = null;

    public KopOAuthBearerSaslServer(CallbackHandler callbackHandler, String defaultKafkaMetadataTenant) {
        if (!(Objects.requireNonNull(callbackHandler) instanceof AuthenticateCallbackHandler)) {
            throw new IllegalArgumentException(String.format("Callback handler must be castable to %s: %s",
                    AuthenticateCallbackHandler.class.getName(), callbackHandler.getClass().getName()));
        }
        this.callbackHandler = (AuthenticateCallbackHandler) callbackHandler;
        this.defaultKafkaMetadataTenant = defaultKafkaMetadataTenant;
    }

    /**
     * @throws SaslAuthenticationException
     *             if access token cannot be validated
     *             <p>
     *             <b>Note:</b> This method may throw
     *             {@link SaslAuthenticationException} to provide custom error
     *             messages to clients. But care should be taken to avoid including
     *             any information in the exception message that should not be
     *             leaked to unauthenticated clients. It may be safer to throw
     *             {@link SaslException} in some cases so that a standard error
     *             message is returned to clients.
     *             </p>
     */
    @Override
    public byte[] evaluateResponse(byte[] response) throws SaslException, SaslAuthenticationException {
        if (response.length == 1 && response[0] == BYTE_CONTROL_A && errorMessage != null) {
            if (log.isDebugEnabled()){
                log.debug("Received %x01 response from client after it received our error");
            }
            throw new SaslAuthenticationException(errorMessage);
        }
        errorMessage = null;
        OAuthBearerClientInitialResponse clientResponse;
        try {
            clientResponse = new OAuthBearerClientInitialResponse(response);
        } catch (SaslException e) {
            log.debug(e.getMessage());
            throw e;
        }
        return process(clientResponse.tokenValue(), clientResponse.authorizationId());
    }

    @Override
    public String getAuthorizationID() {
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }
        return tokenForNegotiatedProperty.principalName();
    }

    @Override
    public String getMechanismName() {
        return OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        if (!complete) {
            throw new IllegalStateException("Authentication exchange has not completed");
        }

        if (AUTH_DATA_SOURCE_PROP.equals(propName)) {
            return tokenForNegotiatedProperty.authDataSource();
        }
        if (USER_NAME_PROP.equals(propName)) {
            if (tokenForNegotiatedProperty.tenant() != null) {
                return tokenForNegotiatedProperty.tenant();
            }
            return defaultKafkaMetadataTenant;
        }
        return NEGOTIATED_PROPERTY_KEY_TOKEN.equals(propName) ? tokenForNegotiatedProperty : null;
    }

    @Override
    public boolean isComplete() {
        return complete;
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
    public void dispose() throws SaslException {
        complete = false;
        tokenForNegotiatedProperty = null;
    }

    private byte[] process(String tokenValue, String authorizationId) throws SaslException {
        KopOAuthBearerValidatorCallback callback = new KopOAuthBearerValidatorCallback(tokenValue);
        try {
            callbackHandler.handle(new Callback[]{callback});
        } catch (IOException | UnsupportedCallbackException e) {
            String msg = String.format("%s: %s", INTERNAL_ERROR_ON_SERVER, e.getMessage());
            if (log.isDebugEnabled()) {
                log.debug(msg, e);
            }
            throw new SaslException(msg);
        }
        KopOAuthBearerToken token = callback.token();
        if (token == null) {
            errorMessage = jsonErrorResponse(callback.errorStatus(), callback.errorScope(),
                    callback.errorOpenIDConfiguration());
            if (log.isDebugEnabled()) {
                log.debug(errorMessage);
            }
            return errorMessage.getBytes(StandardCharsets.UTF_8);
        }
        /*
         * We support the client specifying an authorization ID as per the SASL
         * specification, but it must match the principal name if it is specified.
         */
        if (!authorizationId.isEmpty() && !authorizationId.equals(token.principalName())) {
            throw new SaslAuthenticationException(String.format(
                    "Authentication failed: Client requested an authorization id (%s) "
                            + "that is different from the token's principal name (%s)",
                    authorizationId, token.principalName()));
        }

        tokenForNegotiatedProperty = token;
        complete = true;
        if (log.isDebugEnabled()) {
            log.debug("Successfully authenticate User={}", token.principalName());
        }
        return new byte[0];
    }

    private static String jsonErrorResponse(String errorStatus, String errorScope, String errorOpenIDConfiguration) {
        String jsonErrorResponse = String.format("{\"status\":\"%s\"", errorStatus);
        if (errorScope != null) {
            jsonErrorResponse = String.format("%s, \"scope\":\"%s\"", jsonErrorResponse, errorScope);
        }
        if (errorOpenIDConfiguration != null) {
            jsonErrorResponse = String.format("%s, \"openid-configuration\":\"%s\"", jsonErrorResponse,
                    errorOpenIDConfiguration);
        }
        jsonErrorResponse = String.format("%s}", jsonErrorResponse);
        return jsonErrorResponse;
    }

}
