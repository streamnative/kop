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

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.pulsar.handlers.kop.security.SaslAuthenticator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.naming.AuthenticationException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerIllegalTokenException;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerValidationResult;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.authentication.AuthenticationState;
import org.apache.pulsar.common.api.AuthData;

/**
 * OAuth 2.0 server callback handler.
 */
@Slf4j
public class OauthValidatorCallbackHandler implements AuthenticateCallbackHandler {

    private static final String DELIMITER = "__with_tenant_";

    private ServerConfig config = null;
    private AuthenticationService authenticationService;
    private int requestTimeoutMs;


    public OauthValidatorCallbackHandler() {}

    @VisibleForTesting
    protected OauthValidatorCallbackHandler(ServerConfig config, AuthenticationService authenticationService) {
        this.config = config;
        this.authenticationService = authenticationService;
    }

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

        if (configs == null || configs.isEmpty()
                || !configs.containsKey(SaslAuthenticator.AUTHENTICATION_SERVER_OBJ)) {
            throw new IllegalArgumentException("Configs map do not contains AuthenticationService.");
        }

        this.authenticationService = (AuthenticationService) configs.get(SaslAuthenticator.AUTHENTICATION_SERVER_OBJ);
        this.config = new ServerConfig(options);
        this.requestTimeoutMs = (Integer) configs.get(SaslAuthenticator.REQUEST_TIMEOUT_MS);
    }

    @Override
    public void close() {
        // empty
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof KopOAuthBearerValidatorCallback) {
                KopOAuthBearerValidatorCallback validatorCallback = (KopOAuthBearerValidatorCallback) callback;
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

    @VisibleForTesting
    protected void handleCallback(KopOAuthBearerValidatorCallback callback) {
        if (callback.tokenValue() == null) {
            throw new IllegalArgumentException("Callback has null token value!");
        }
        if (authenticationService == null) {
            throw new IllegalStateException("AuthenticationService is null during token validation");
        }
        final AuthenticationProvider authenticationProvider =
                authenticationService.getAuthenticationProvider(config.getValidateMethod());
        if (authenticationProvider == null) {
            throw new IllegalStateException("No AuthenticationProvider found for method " + config.getValidateMethod());
        }

        final String tokenWithTenant = callback.tokenValue();

        // Extract real token.
        Pair<String, String> tokenAndTenant = OAuthTokenDecoder.decode(tokenWithTenant);
        final String token = tokenAndTenant.getLeft();
        final String tenant = tokenAndTenant.getRight();

        try {
            AuthData authData = AuthData.of(token.getBytes(StandardCharsets.UTF_8));
            final AuthenticationState authState = authenticationProvider.newAuthState(
                    authData, null, null);
            authState.authenticateAsync(authData).get(requestTimeoutMs, TimeUnit.MILLISECONDS);
            final String role = authState.getAuthRole();
            AuthenticationDataSource authDataSource = authState.getAuthDataSource();
            callback.token(new KopOAuthBearerToken() {
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
                public AuthenticationDataSource authDataSource() {
                    return authDataSource;
                }

                @Override
                public String tenant() {
                    return tenant;
                }

                @Override
                public Long startTimeMs() {
                    // TODO: convert "iat" claim to ms.
                    return Long.MAX_VALUE;
                }
            });
        } catch (AuthenticationException | InterruptedException | ExecutionException | TimeoutException e) {
            log.error("OAuth validator callback handler new auth state failed: ", e);
            throw new OAuthBearerIllegalTokenException(OAuthBearerValidationResult.newFailure(e.getMessage()));
        }
    }
}
