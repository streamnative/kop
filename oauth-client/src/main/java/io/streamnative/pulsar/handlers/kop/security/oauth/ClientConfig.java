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
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
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
    private final io.streamnative.pulsar.handlers.kop.security.oauth.url.URL credentialsUrl;
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
            this.credentialsUrl = new io.streamnative.pulsar.handlers.kop.security.oauth.url.URL(credentialsUrlString);
        } catch (MalformedURLException | URISyntaxException | InstantiationException | IllegalAccessException e) {
            throw new IllegalArgumentException(String.format(
                    "invalid %s \"%s\": %s", OAUTH_CREDENTIALS_URL, credentialsUrlString, e.getMessage()));
        }
        try {
            this.clientInfo = ClientConfig.loadClientInfo(credentialsUrlString);
        } catch (IOException e) {
            throw new IllegalArgumentException(String.format(
                    "failed to load client credentials from %s: %s", credentialsUrlString, e.getMessage()));
        }
        this.audience = configs.getOrDefault(OAUTH_AUDIENCE, null);
        this.scope = configs.getOrDefault(OAUTH_SCOPE, null);
    }

    private static ClientInfo loadClientInfo(String credentialsUrl) throws IOException {
        try {
            URLConnection urlConnection = new io.streamnative.pulsar.handlers.kop.security.oauth.url.URL(credentialsUrl)
                    .openConnection();
            try {
                String protocol = urlConnection.getURL().getProtocol();
                String contentType = urlConnection.getContentType();
                if ("data".equals(protocol) && !"application/json".equals(contentType)) {
                    throw new IllegalArgumentException(
                            "Unsupported media type or encoding format: " + urlConnection.getContentType());
                }
                ClientInfo privateKey;
                try (Reader r = new InputStreamReader((InputStream) urlConnection.getContent(),
                        StandardCharsets.UTF_8)) {
                    privateKey = CLIENT_INFO_READER.readValue(r);
                }
                return privateKey;
            } finally {
                if (urlConnection instanceof HttpURLConnection) {
                    ((HttpURLConnection) urlConnection).disconnect();
                }
            }
        } catch (URISyntaxException | InstantiationException | IllegalAccessException e) {
            throw new IOException("Invalid credentialsUrl format", e);
        }
    }
}
