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

import io.streamnative.pulsar.handlers.kop.security.SaslAuthenticator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.naming.AuthenticationException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerIllegalTokenException;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerValidationResult;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;

/**
 * OAuth 2.0 server callback handler.
 */
public class OauthValidatorCallbackHandler implements AuthenticateCallbackHandler {

    private ServerConfig config = null;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        if (!OAuthBearerLoginModule.OAUTHBEARER_MECHANISM.equals(saslMechanism)) {
            throw new IllegalArgumentException("Unexpected SASL mechanism: " + saslMechanism);
        }
        if (Objects.requireNonNull(jaasConfigEntries).size() != 1 || jaasConfigEntries.get(0) == null) {
            throw new IllegalArgumentException(
                    String.format("Must supply exactly 1 non-null JAAS mechanism configuration (size was %d)",
                            jaasConfigEntries.size()));
        }
        final Map<String, String> options = (Map<String, String>) jaasConfigEntries.get(0).getOptions();
        if (options == null) {
            throw new IllegalArgumentException("JAAS configuration options is null");
        }
        this.config = new ServerConfig(options);
    }

    @Override
    public void close() {
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerValidatorCallback) {
                OAuthBearerValidatorCallback validatorCallback = (OAuthBearerValidatorCallback) callback;
                try {
                    handleCallback(validatorCallback);
                } catch (OAuthBearerIllegalTokenException e) {
                    final OAuthBearerValidationResult failureReason = e.reason();
                    final String failureScope = failureReason.failureScope();
                    validatorCallback.error(failureScope != null ? "insufficient_scope" : "invalid_token",
                            failureScope, failureReason.failureOpenIdConfig());
                }
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private void handleCallback(OAuthBearerValidatorCallback callback) {
        if (callback.tokenValue() == null) {
            throw new IllegalArgumentException("Callback has null token value!");
        }
        if (SaslAuthenticator.getAuthenticationService() == null) {
            throw new RuntimeException("AuthenticationService is null during token validation");
        }
        final AuthenticationProvider authenticationProvider =
                SaslAuthenticator.getAuthenticationService().getAuthenticationProvider(config.getValidateMethod());
        if (authenticationProvider == null) {
            throw new RuntimeException("No AuthenticationProvider found for method " + config.getValidateMethod());
        }

        final String token = callback.tokenValue();
        try {
            final AuthenticationState authState = authenticationProvider.newAuthState(
                    AuthData.of(token.getBytes(StandardCharsets.UTF_8)), null, null);
            final String role = authState.getAuthRole();
            callback.token(new OAuthBearerToken() {
                @Override
                public String value() {
                    return token;
                }

                @Override
                public Set<String> scope() {
                    return null;
                }

                @Override
                public long lifetimeMs() {
                    // TODO: convert "exp" claim to ms.
                    return Long.MAX_VALUE;
                }

                @Override
                public String principalName() {
                    return role;
                }

                @Override
                public Long startTimeMs() {
                    // TODO: convert "iat" claim to ms.
                    return Long.MAX_VALUE;
                }
            });
        } catch (AuthenticationException e) {
            throw new OAuthBearerIllegalTokenException(OAuthBearerValidationResult.newFailure(e.getMessage()));
        }
    }
}
