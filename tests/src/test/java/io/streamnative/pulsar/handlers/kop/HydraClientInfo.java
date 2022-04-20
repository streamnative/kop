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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;

@NoArgsConstructor
@Getter
@Setter
class HydraClientInfo {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @JsonProperty("client_id")
    private String id;

    @JsonProperty("client_secret")
    private String secret;

    @JsonProperty("audience")
    private String[] audience;

    @JsonProperty("response_types")
    private String[] responseTypes = { "code" };

    @JsonProperty("grant_types")
    private String[] grantTypes = { "client_credentials" };

    // NOTE: Currently KoP only supports "client_secret_post" auth method
    @JsonProperty("token_endpoint_auth_method")
    private String tokenEndpointAuthMethod = "client_secret_post";

    @SneakyThrows
    @Override
    public String toString() {
        return objectMapper.writeValueAsString(this);
    }

    /**
     * Write a credential file to the path of the resource directory.
     *
     * The content of the file should be a JSON like:
     *
     * ```json
     * {
     *     "client_id": "xxx",
     *     "client_secret": "xxx"
     * }
     * ```
     *
     * @param filename the basename of the file
     * @return the absolute path of the credential file with a "file://" prefix, which can be configured by Kafka
     *   clients for OAuth2 authentication
     * @throws IOException
     */
    public String writeCredentials(String filename) throws IOException {
        final String content = "{\n"
                + "    \"client_id\": \"" + id + "\",\n"
                + "    \"client_secret\": \"" + secret + "\"\n"
                + "}\n";
        File file = new File(HydraClientInfo.class.getResource("/").getFile() + "/" + filename);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(content);
        }
        return "file://" + file.getAbsolutePath();
    }
}
