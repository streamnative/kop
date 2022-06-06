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
package io.streamnative.pulsar.handlers.kop.schemaregistry.resources;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.netty.handler.codec.http.FullHttpRequest;
import io.streamnative.pulsar.handlers.kop.schemaregistry.HttpJsonRequestProcessor;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryHandler;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryRequestAuthenticator;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.CompatibilityChecker;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.Schema;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorage;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorageAccessor;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;

public class CompatibilityResource extends AbstractResource {

    public CompatibilityResource(SchemaStorageAccessor schemaStorageAccessor,
                                 SchemaRegistryRequestAuthenticator schemaRegistryRequestAuthenticator) {
        super(schemaStorageAccessor, schemaRegistryRequestAuthenticator);
    }

    @Override
    public void register(SchemaRegistryHandler schemaRegistryHandler) {
        schemaRegistryHandler.addProcessor(new CheckCompatibilityLatestSchema());
    }

    @Data
    public static final class CheckCompatibilityLatestSchemaRequest {
        private String schema;
    }

    @AllArgsConstructor
    public static final class CheckCompatibilityLatestSchemaResponse {

        private final boolean isCompatible;

        @JsonProperty("is_compatible")
        public boolean getIsCompatible() {
            return isCompatible;
        }
    }

    // GET /compatibility/subjects/${subject}/versions/latest
    public class CheckCompatibilityLatestSchema extends HttpJsonRequestProcessor<CheckCompatibilityLatestSchemaRequest,
            CheckCompatibilityLatestSchemaResponse> {

        public CheckCompatibilityLatestSchema() {
            super(CheckCompatibilityLatestSchemaRequest.class,
                    "/compatibility/subjects/" + STRING_PATTERN + "/versions/latest", POST);
        }

        @Override
        protected CompletableFuture<CheckCompatibilityLatestSchemaResponse> processRequest(
                CheckCompatibilityLatestSchemaRequest payload, List<String> patternGroups, FullHttpRequest request)
                throws Exception {
            SchemaStorage schemaStorage = getSchemaStorage(request);
            String subject = patternGroups.get(0);
            Schema dummy = Schema.builder()
                    .schemaDefinition(payload.schema)
                    .type(Schema.TYPE_AVRO)
                    .build();
            return CompatibilityChecker.verify(dummy, subject, schemaStorage)
                    .thenApply(CheckCompatibilityLatestSchemaResponse::new);
        }

    }

}
