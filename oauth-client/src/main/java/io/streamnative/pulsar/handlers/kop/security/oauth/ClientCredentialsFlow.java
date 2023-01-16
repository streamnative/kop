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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

/**
 * The OAuth 2.0 client credential flow.
 */
public class ClientCredentialsFlow implements Closeable {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectReader METADATA_READER = OBJECT_MAPPER.readerFor(Metadata.class);
    private static final ObjectReader CLIENT_INFO_READER = OBJECT_MAPPER.readerFor(ClientInfo.class);
    private static final ObjectReader TOKEN_RESULT_READER = OBJECT_MAPPER.readerFor(OAuthBearerTokenImpl.class);
    private static final ObjectReader TOKEN_ERROR_READER = OBJECT_MAPPER.readerFor(TokenError.class);

    private final Duration connectTimeout = Duration.ofSeconds(10);
    private final Duration readTimeout = Duration.ofSeconds(30);
    private final ClientConfig clientConfig;

    public ClientCredentialsFlow(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    public OAuthBearerTokenImpl authenticate() throws IOException {
        final String tokenEndPoint = findAuthorizationServer().getTokenEndPoint();
        final ClientInfo clientInfo = loadPrivateKey();
        final URL url = new URL(tokenEndPoint);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        try {
            con.setReadTimeout((int) readTimeout.toMillis());
            con.setConnectTimeout((int) connectTimeout.toMillis());
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            con.setRequestProperty("Accept", "application/json");
            final String body = buildClientCredentialsBody(clientInfo);
            try (OutputStream o = con.getOutputStream()) {
                o.write(body.getBytes(StandardCharsets.UTF_8));
            }
            try (InputStream in = con.getInputStream()) {
                return TOKEN_RESULT_READER.readValue(in);
            }
        } catch (IOException err) {
            switch (con.getResponseCode()) {
                case 400: // Bad request
                case 401: { // Unauthorized
                    IOException error;
                    try {
                        error =  new IOException(OBJECT_MAPPER.writeValueAsString(
                                TOKEN_ERROR_READER.readValue(con.getErrorStream())));
                        error.addSuppressed(err);
                    } catch (Exception ignoreJsonError) {
                        err.addSuppressed(ignoreJsonError);
                        throw err;
                    }
                    throw error;
                }
                default:
                    throw new IOException("Failed to perform HTTP request to " + tokenEndPoint
                            + ":" + con.getResponseCode() + " " + con.getResponseMessage(), err);
            }
        } finally {
            con.disconnect();
        }
    }

    @Override
    public void close() throws IOException {
    }

    @VisibleForTesting
    Metadata findAuthorizationServer() throws IOException {
        // See RFC-8414 for this well-known URI
        final URL wellKnownMetadataUrl = URI.create(clientConfig.getIssuerUrl().toExternalForm()
                + "/.well-known/openid-configuration").normalize().toURL();
        final HttpURLConnection connection = (HttpURLConnection) wellKnownMetadataUrl.openConnection();
        try {
            connection.setConnectTimeout((int) connectTimeout.toMillis());
            connection.setReadTimeout((int) readTimeout.toMillis());
            connection.setRequestProperty("Accept", "application/json");

            try (InputStream inputStream = connection.getInputStream()) {
                return METADATA_READER.readValue(inputStream);
            }
        } finally {
            connection.disconnect();
        }
    }

    @VisibleForTesting
    ClientInfo loadPrivateKey() throws IOException {
        final URLConnection connection = clientConfig.getCredentialsUrl().openConnection();
        try (InputStream inputStream = connection.getInputStream()) {
            return CLIENT_INFO_READER.readValue(inputStream);
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
    public static class ClientInfo {

        @JsonProperty("client_id")
        private String id;

        @JsonProperty("client_secret")
        private String secret;
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
