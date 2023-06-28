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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.util.Base64;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExtensionTokenData {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ObjectWriter EXTENSION_WRITER = OBJECT_MAPPER.writer();
    private static final ObjectReader EXTENSION_READER = OBJECT_MAPPER.readerFor(ExtensionTokenData.class);

    @JsonProperty("tenant")
    private String tenant;

    @JsonProperty("groupId")
    private String groupId;

    public static ExtensionTokenData decode(String extensionData) throws IOException {
        return EXTENSION_READER.readValue(Base64.getDecoder().decode(extensionData));
    }

    public boolean hasExtensionData() {
        return tenant != null || groupId != null;
    }

    public String encode() throws JsonProcessingException {
        return Base64.getEncoder().encodeToString(EXTENSION_WRITER.writeValueAsBytes(this));
    }
}
