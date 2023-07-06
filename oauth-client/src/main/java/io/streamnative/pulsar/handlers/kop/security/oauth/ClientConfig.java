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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import lombok.Getter;

/**
 * The client configs associated with OauthLoginCallbackHandler.
 *
 * @see OauthLoginCallbackHandler
 */
@Getter
public class ClientConfig {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final ObjectReader CLIENT_INFO_READER = OBJECT_MAPPER.readerFor(ClientInfo.class);

    public static final String OAUTH_ISSUER_URL = "oauth.issuer.url";
    public static final String OAUTH_CREDENTIALS_URL = "oauth.credentials.url";
    public static final String OAUTH_AUDIENCE = "oauth.audience";
    public static final String OAUTH_SCOPE = "oauth.scope";

    private final URL issuerUrl;
    private final URL credentialsUrl;
    private final String audience;
    private final String scope;
    private final ClientInfo clientInfo;

    public ClientConfig(Map<String, String> configs) {
        final String issuerUrlString = configs.get(OAUTH_ISSUER_URL);
        if (issuerUrlString == null) {
            throw new IllegalArgumentException("no key for " + OAUTH_ISSUER_URL);
        }
        try {
            this.issuerUrl = new URL(issuerUrlString);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(String.format(
                    "invalid %s \"%s\": %s", OAUTH_ISSUER_URL, issuerUrlString, e.getMessage()));
        }

        final String credentialsUrlString = configs.get(OAUTH_CREDENTIALS_URL);
        if (credentialsUrlString == null) {
            throw new IllegalArgumentException("no key for " + OAUTH_CREDENTIALS_URL);
        }
        try {
            this.credentialsUrl = new URL(credentialsUrlString);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(String.format(
                    "invalid %s \"%s\": %s", OAUTH_CREDENTIALS_URL, credentialsUrlString, e.getMessage()));
        }
        try {
            final URLConnection connection = getCredentialsUrl().openConnection();
            try (InputStream inputStream = connection.getInputStream()) {
                this.clientInfo = CLIENT_INFO_READER.readValue(inputStream);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format(
                    "failed to load client credentials from %s: %s", credentialsUrlString, e.getMessage()));
        }
        this.audience = configs.getOrDefault(OAUTH_AUDIENCE, null);
        this.scope = configs.getOrDefault(OAUTH_SCOPE, null);
    }
}
