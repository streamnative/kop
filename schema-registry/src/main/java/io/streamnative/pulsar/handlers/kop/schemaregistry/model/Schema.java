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
package io.streamnative.pulsar.handlers.kop.schemaregistry.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

// This class is migrated from io.confluent.kafka.schemaregistry.client.rest.entities.Schema
@Data
@AllArgsConstructor
@Builder
@EqualsAndHashCode
@NoArgsConstructor
@ToString
public final class Schema {

    public static final String TYPE_AVRO = "AVRO";
    public static final String TYPE_JSON = "JSON";
    public static final String TYPE_PROTOBUF = "PROTOBUF";
    private static final List<String> ALL_TYPES =
            Collections.unmodifiableList(Arrays.asList(TYPE_AVRO, TYPE_JSON, TYPE_PROTOBUF));

    private String tenant;
    @JsonProperty("id")
    private int id;
    @JsonProperty("version")
    private int version;
    @JsonProperty("schema")
    private String schemaDefinition;
    @JsonProperty("subject")
    private String subject;
    @JsonProperty("type")
    private String type;

    public static List<String> getAllTypes() {
        return ALL_TYPES;
    }
}
