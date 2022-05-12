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
package io.streamnative.pulsar.handlers.kop;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.fusionauth.jwks.domain.JSONWebKey;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.KeyPair;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import sh.ory.hydra.ApiException;
import sh.ory.hydra.api.AdminApi;
import sh.ory.hydra.model.JSONWebKeySet;
import sh.ory.hydra.model.OAuth2Client;

/**
 * The Hydra OAuth utils.
 */
public class HydraOAuthUtils {

    private static final String AUDIENCE = "http://example.com/api/v2/";

    private static final AdminApi hydraAdmin = new AdminApi();

    private static String publicKey;

    static {
        hydraAdmin.setCustomBaseUrl("http://localhost:4445");
    }

    /**
     * Init the Hydra OAuth jwt access token.
     *
     * @return base64 pem public key
     */
    public static String getPublicKeyStr() throws JsonProcessingException, ApiException {
        if (publicKey != null) {
            return publicKey;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        ObjectReader jwkReader = objectMapper.readerFor(sh.ory.hydra.model.JSONWebKey.class);
        KeyPair pair = Keys.keyPairFor(SignatureAlgorithm.RS256);
        pair.getPrivate().getEncoded();
        JSONWebKey jwkPublic = JSONWebKey.build(pair.getPublic());
        JSONWebKey jwkPrivate = JSONWebKey.build(pair.getPrivate());
        jwkPublic.kid = "public:hydra.jwt.access-token";
        jwkPrivate.kid = "private:hydra.jwt.access-token";
        sh.ory.hydra.model.JSONWebKey hydraJwkPublic =
                jwkReader.readValue(objectMapper.writeValueAsString(jwkPublic));
        sh.ory.hydra.model.JSONWebKey hydraJwkPrivate =
                jwkReader.readValue(objectMapper.writeValueAsString(jwkPrivate));
        try {
            hydraAdmin.updateJsonWebKeySet("hydra.jwt.access-token", new JSONWebKeySet()
                    .keys(Arrays.asList(hydraJwkPublic, hydraJwkPrivate)));
        } catch (ApiException e) {
            if (e.getCode() != 409) {
                throw e;
            }
        }
        publicKey = Base64.getEncoder().encodeToString(pair.getPublic().getEncoded());
        return publicKey;
    }

    public static String createOAuthClient(String clientId, String clientSecret) throws ApiException, IOException {
        final OAuth2Client oAuth2Client = new OAuth2Client()
                .audience(Collections.singletonList(AUDIENCE))
                .clientId(clientId)
                .clientSecret(clientSecret)
                .grantTypes(Collections.singletonList("client_credentials"))
                .responseTypes(Collections.singletonList("code"))
                .tokenEndpointAuthMethod("client_secret_post");
        try {
            hydraAdmin.createOAuth2Client(oAuth2Client);
        } catch (ApiException e) {
            if (e.getCode() != 409) {
                throw e;
            }
        }
        return writeCredentialsFile(clientId, clientSecret, clientId + ".json");
    }

    public static String writeCredentialsFile(String clientId,
                                               String clientSecret,
                                               String basename) throws IOException {
        final String content = "{\n"
                + "    \"client_id\": \"" + clientId + "\",\n"
                + "    \"client_secret\": \"" + clientSecret + "\"\n"
                + "}\n";

        File file = File.createTempFile("oauth-credentials-", basename);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(content);
        }
        return "file://" + file.getAbsolutePath();
    }
}
