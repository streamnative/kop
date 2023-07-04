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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.Getter;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;

/**
 * The OAuth 2.0 client credential flow.
 */
public class ClientCredentialsFlow implements Closeable {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectReader METADATA_READER = OBJECT_MAPPER.readerFor(Metadata.class);
    private static final ObjectReader TOKEN_RESULT_READER = OBJECT_MAPPER.readerFor(OAuthBearerTokenImpl.class);
    private static final ObjectReader TOKEN_ERROR_READER = OBJECT_MAPPER.readerFor(TokenError.class);

    private final Duration connectTimeout = Duration.ofSeconds(10);
    private final Duration readTimeout = Duration.ofSeconds(30);
    private final ClientConfig clientConfig;
    private final AsyncHttpClient httpClient;

    public ClientCredentialsFlow(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        this.httpClient = new DefaultAsyncHttpClient(new DefaultAsyncHttpClientConfig.Builder()
                .setFollowRedirect(true)
                .setConnectTimeout((int) connectTimeout.toMillis())
                .setReadTimeout((int) readTimeout.toMillis())
                .build());
    }

    @VisibleForTesting
    protected ClientCredentialsFlow(ClientConfig clientConfig, AsyncHttpClient httpClient) {
        this.clientConfig = clientConfig;
        this.httpClient = httpClient;
    }

    public OAuthBearerTokenImpl authenticate() throws IOException {
        final String tokenEndPoint = findAuthorizationServer().getTokenEndPoint();
        final ClientInfo clientInfo = clientConfig.getClientInfo();
        try {
            final String body = buildClientCredentialsBody(clientInfo);
            final Response response = httpClient.preparePost(tokenEndPoint)
                    .setHeader("Accept", "application/json")
                    .setHeader("Content-Type", "application/x-www-form-urlencoded")
                    .setBody(body)
                    .execute()
                    .get();
            switch (response.getStatusCode()) {
                case 200:
                    OAuthBearerTokenImpl token = TOKEN_RESULT_READER.readValue(response.getResponseBodyAsBytes());
                    String tenant = clientInfo.getTenant();
                    // Add tenant for multi-tenant.
                    if (tenant != null) {
                        token.setTenant(tenant);
                    }
                    return token;
                case 400: // Bad request
                case 401: // Unauthorized
                    throw new IOException(OBJECT_MAPPER.writeValueAsString(
                            TOKEN_ERROR_READER.readValue(response.getResponseBodyAsBytes())));
                default:
                    throw new IOException("Failed to perform HTTP request:  "
                            + response.getStatusCode() + " " + response.getStatusText());
            }
        } catch (UnsupportedEncodingException | InterruptedException
                 | ExecutionException | JsonProcessingException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    @VisibleForTesting
    Metadata findAuthorizationServer() throws IOException {
        // See RFC-8414 for this well-known URI
        final URL wellKnownMetadataUrl = URI.create(clientConfig.getIssuerUrl().toExternalForm()
                + "/.well-known/openid-configuration").normalize().toURL();
        final URLConnection connection = wellKnownMetadataUrl.openConnection();
        connection.setConnectTimeout((int) connectTimeout.toMillis());
        connection.setReadTimeout((int) readTimeout.toMillis());
        connection.setRequestProperty("Accept", "application/json");

        try (InputStream inputStream = connection.getInputStream()) {
            return METADATA_READER.readValue(inputStream);
        }
    }


    private static String encode(String s) throws UnsupportedEncodingException {
        return URLEncoder.encode(s, StandardCharsets.UTF_8.name());
    }

    private String buildClientCredentialsBody(ClientInfo clientInfo) throws UnsupportedEncodingException {
        final Map<String, String> bodyMap = new HashMap<>();
        bodyMap.put("grant_type", "client_credentials");
        bodyMap.put("client_id", encode(clientInfo.getId()));
        bodyMap.put("client_secret", encode(clientInfo.getSecret()));
        if (clientConfig.getAudience() != null) {
            bodyMap.put("audience", encode(clientConfig.getAudience()));
        }
        if (clientConfig.getScope() != null) {
            bodyMap.put("scope", encode(clientConfig.getScope()));
        }
        return bodyMap.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining("&"));
    }

    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Metadata {

        @JsonProperty("token_endpoint")
        private String tokenEndPoint;
    }

    @Getter
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TokenError {

        @JsonProperty("error")
        private String error;

        @JsonProperty("error_description")
        private String errorDescription;

        @JsonProperty("error_uri")
        private String errorUri;
    }
}
