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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.naming.AuthenticationException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.common.api.AuthData;

/**
 * OAuth 2.0 login callback handler.
 */
public class OauthLoginCallbackHandler implements AuthenticateCallbackHandler {

    private static final String DEFAULT_PRINCIPAL_CLAIM_NAME = "sub";
    private static final String DEFAULT_SCOPE_CLAIM_NAME = "scope";
    private ClientConfig clientConfig = null;

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
        clientConfig = new ClientConfig(options);
    }

    @Override
    public void close() {
        // empty
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        if (clientConfig == null) {
            throw new IllegalStateException("Callback handler not configured");
        }
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                try {
                    handleCallback((OAuthBearerTokenCallback) callback);
                } catch (KafkaException e) {
                    throw new IOException(e.getMessage(), e);
                }
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private void handleCallback(OAuthBearerTokenCallback callback) {
        if (callback.token() != null) {
            throw new IllegalArgumentException("Callback had a token already");
        }
        final Authentication authentication = AuthenticationFactoryOAuth2.clientCredentials(
                clientConfig.getIssuerUrl(),
                clientConfig.getCredentialsUrl(),
                clientConfig.getAudience()
        );
        try {
            authentication.start();
            final AuthenticationDataProvider provider = authentication.getAuthData();
            final AuthData authData = provider.authenticate(AuthData.INIT_AUTH_DATA);
            callback.token(new OAuthBearerToken() {
                @Override
                public String value() {
                    return new String(authData.getBytes(), StandardCharsets.UTF_8);
                }

                @Override
                public Set<String> scope() {
                    return Collections.singleton(DEFAULT_SCOPE_CLAIM_NAME);
                }

                @Override
                public long lifetimeMs() {
                    // TODO: convert "exp" claim to ms.
                    return Long.MAX_VALUE;
                }

                @Override
                public String principalName() {
                    return DEFAULT_PRINCIPAL_CLAIM_NAME;
                }

                @Override
                public Long startTimeMs() {
                    // TODO: convert "iat" claim to ms.
                    return Long.MAX_VALUE;
                }
            });
        } catch (PulsarClientException | AuthenticationException e) {
            throw new KafkaException(e);
        }
    }
}
