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

import io.netty.handler.codec.http.FullHttpRequest;
import io.streamnative.pulsar.handlers.kop.schemaregistry.HttpJsonRequestProcessor;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryHandler;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryRequestAuthenticator;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.Schema;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorage;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorageAccessor;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class SchemaResource extends AbstractResource {

    public SchemaResource(SchemaStorageAccessor schemaStorageAccessor,
                          SchemaRegistryRequestAuthenticator schemaRegistryRequestAuthenticator) {
        super(schemaStorageAccessor, schemaRegistryRequestAuthenticator);
    }

    /**
     * Register all processors.
     * @param schemaRegistryHandler
     */
    public void register(SchemaRegistryHandler schemaRegistryHandler) {
        schemaRegistryHandler.addProcessor(new GetSchemaById());
        schemaRegistryHandler.addProcessor(new GetSchemaTypes());
        schemaRegistryHandler.addProcessor(new GetSchemaAliases());
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class GetSchemaResponse {
        private String schema;
    }

    // GET /schemas/ids/{int: id}
    public class GetSchemaById extends HttpJsonRequestProcessor<Void, GetSchemaResponse> {

        public GetSchemaById() {
            super(Void.class, "/schemas/ids/" + INT_PATTERN, GET);
        }

        @Override
        protected GetSchemaResponse processRequest(Void payload, List<String> patternGroups, FullHttpRequest request)
                                    throws Exception{
            int id = getInt(0, patternGroups);
            SchemaStorage schemaStorage = getSchemaStorage(request);
            Schema schemaById = schemaStorage.findSchemaById(id);
            if (schemaById == null) {
                return null;
            }
            return new GetSchemaResponse(schemaById.getSchemaDefinition());
        }

    }


    // /schemas/types
    public static class GetSchemaTypes extends HttpJsonRequestProcessor<Void, List<String>> {

        public GetSchemaTypes() {
            super(Void.class, "/schemas/types", GET);
        }

        @Override
        protected List<String> processRequest(Void payload, List<String> patternGroups, FullHttpRequest request) {
           return Schema.getAllTypes();
        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
     public static final class SubjectVersionPair {
        private String subject;
        private int version;
    }


    // /schemas/ids/{int: id}/versions
    public class GetSchemaAliases extends HttpJsonRequestProcessor<Void, List<SubjectVersionPair>> {

        public GetSchemaAliases() {
            super(Void.class, "/schemas/ids/" + INT_PATTERN + "/versions", GET);
        }

        @Override
        protected List<SubjectVersionPair> processRequest(Void payload,
                                                          List<String> patternGroups, FullHttpRequest request)
                                throws Exception {
            SchemaStorage schemaStorage = getSchemaStorage(request);
            int id = getInt(0, patternGroups);
            Schema schema =  schemaStorage.findSchemaById(id);
            if (schema == null) {
                return null;
            }
            List<Schema> schemaAliases = schemaStorage.findSchemaByDefinition(schema.getSchemaDefinition());
            return schemaAliases
                    .stream()
                    .map(s -> new SubjectVersionPair(schema.getSubject(), schema.getVersion()))
                    .collect(Collectors.toList());

        }

    }


}
