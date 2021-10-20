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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

public class SubjectResource extends AbstractResource {

    public SubjectResource(SchemaStorageAccessor schemaStorageAccessor,
                           SchemaRegistryRequestAuthenticator schemaRegistryRequestAuthenticator) {
        super(schemaStorageAccessor, schemaRegistryRequestAuthenticator);
    }

    @Override
    public void register(SchemaRegistryHandler schemaRegistryHandler) {
        schemaRegistryHandler.addProcessor(new CreateNewSchema());
        schemaRegistryHandler.addProcessor(new GetAllSubjects());
        schemaRegistryHandler.addProcessor(new GetAllVersions());
        schemaRegistryHandler.addProcessor(new DeleteSubject());
        schemaRegistryHandler.addProcessor(new GetSchemaBySubjectAndVersion());
        schemaRegistryHandler.addProcessor(new GetRawSchemaBySubjectAndVersion());
        schemaRegistryHandler.addProcessor(new CreateOrUpdateSchema());
    }

    // GET /subjects
    public class GetAllSubjects extends HttpJsonRequestProcessor<Void, List<String>> {

        public GetAllSubjects() {
            super(Void.class, "/subjects", GET);
        }

        @Override
        protected CompletableFuture<List<String>> processRequest(Void payload, List<String> patternGroups, FullHttpRequest request)
                throws Exception {
            SchemaStorage schemaStorage = getSchemaStorage(request);
            CompletableFuture<List<String>> subjects = schemaStorage.getAllSubjects();
            return subjects;
        }

    }

    // GET /subjects/test/versions
    public class GetAllVersions extends HttpJsonRequestProcessor<Void, List<Integer>> {

        public GetAllVersions() {
            super(Void.class, "/subjects/" + STRING_PATTERN + "/versions", GET);
        }

        @Override
        protected CompletableFuture<List<Integer>> processRequest(Void payload, List<String> patternGroups, FullHttpRequest request)
                throws Exception {
            SchemaStorage schemaStorage = getSchemaStorage(request);
            String subject = getString(0, patternGroups);
            CompletableFuture<List<Integer>> versions = schemaStorage.getAllVersionsForSubject(subject);
            return versions.thenApply(v -> {
                if (v.isEmpty()) {
                    return null;
                }
                return v;
            });
        }

    }

    // DELETE /subjects/(string: subject)
    public class DeleteSubject extends HttpJsonRequestProcessor<Void, List<Integer>> {

        public DeleteSubject() {
            super(Void.class, "/subjects/" + STRING_PATTERN, DELETE);
        }

        @Override
        protected CompletableFuture<List<Integer>> processRequest(Void payload, List<String> patternGroups, FullHttpRequest request)
                throws Exception {
            SchemaStorage schemaStorage = getSchemaStorage(request);
            String subject = getString(0, patternGroups);

            CompletableFuture<List<Integer>> versions = schemaStorage.deleteSubject(subject);
            return versions.thenApply(v -> {
                if (v.isEmpty()) {
                    return null;
                }
                return v;
            });
        }

    }

    @AllArgsConstructor
    @Getter
    public static class GetSchemaBySubjectAndVersionResponse {
        private String name;
        private int version;
        private String schema;
    }

    // GET /subjects/(string: subject)/versions/(versionId: version)
    public class GetSchemaBySubjectAndVersion
            extends HttpJsonRequestProcessor<Void, GetSchemaBySubjectAndVersionResponse> {

        public GetSchemaBySubjectAndVersion() {
            super(Void.class, "/subjects/" + STRING_PATTERN + "/versions/" + INT_PATTERN, GET);
        }

        @Override
        protected CompletableFuture<GetSchemaBySubjectAndVersionResponse> processRequest(Void payload,
                                                                      List<String> patternGroups,
                                                                      FullHttpRequest request)
                throws Exception {
            String subject = getString(0, patternGroups);
            int version = getInt(1, patternGroups);
            SchemaStorage schemaStorage = getSchemaStorage(request);
            CompletableFuture<Schema> schema = schemaStorage.findSchemaBySubjectAndVersion(subject, version);
            return schema.thenApply(s -> {
                if (s == null) {
                    return null;
                }
                return new GetSchemaBySubjectAndVersionResponse(s.getSubject(),
                        s.getVersion(), s.getSchemaDefinition());
            });
        }

    }

    // GET /subjects/(string: subject)/versions/(versionId: version)
    public class GetRawSchemaBySubjectAndVersion extends HttpJsonRequestProcessor<Void, String> {

        public GetRawSchemaBySubjectAndVersion() {
            super(Void.class, "/subjects/" + STRING_PATTERN + "/versions/" + INT_PATTERN + "/schema", GET);
        }

        @Override
        protected CompletableFuture<String> processRequest(Void payload, List<String> patternGroups, FullHttpRequest request)
                throws Exception {
            String subject = getString(0, patternGroups);
            int version = getInt(1, patternGroups);
            SchemaStorage schemaStorage = getSchemaStorage(request);
            CompletableFuture<Schema> schema = schemaStorage.findSchemaBySubjectAndVersion(subject, version);
            return schema.thenApply(s -> {
                if (s == null) {
                    return null;
                }
                return s.getSchemaDefinition();
            });
        }

    }



    @Data
    public static final class CreateSchemaRequest {
        String schema;
        String schemaType = "AVRO";
        List<SchemaReference> references = new ArrayList<>();

        @Data
        public static final class SchemaReference {
            String name;
            String subject;
            String version;
        }
    }

    @Data
    @AllArgsConstructor
    public static final class CreateSchemaResponse {
        int id;
    }


    // POST /subjects/(string: subject)/versions
    public class CreateNewSchema extends HttpJsonRequestProcessor<CreateSchemaRequest, CreateSchemaResponse> {

        public CreateNewSchema() {
            super(CreateSchemaRequest.class, "/subjects/" + STRING_PATTERN + "/versions", POST);
        }

        @Override
        protected CompletableFuture<CreateSchemaResponse> processRequest(CreateSchemaRequest payload, List<String> patternGroups,
                                                      FullHttpRequest request)
                throws Exception {
            String subject = getString(0, patternGroups);
            SchemaStorage schemaStorage = getSchemaStorage(request);
            CompletableFuture<Schema> schema = schemaStorage.createSchemaVersion(subject,
                    payload.schemaType, payload.schema, true);
            return schema.thenApply(s -> new CreateSchemaResponse(s.getId()));
        }

    }


    // POST /subjects/(string: subject)
    public class CreateOrUpdateSchema extends HttpJsonRequestProcessor<CreateSchemaRequest, CreateSchemaResponse> {

        public CreateOrUpdateSchema() {
            super(CreateSchemaRequest.class, "/subjects/" + STRING_PATTERN, POST);
        }

        @Override
        protected CompletableFuture<CreateSchemaResponse> processRequest(CreateSchemaRequest payload, List<String> patternGroups,
                                                      FullHttpRequest request)
                throws Exception {
            String subject = getString(0, patternGroups);
            SchemaStorage schemaStorage = getSchemaStorage(request);
            CompletableFuture<Schema> schema = schemaStorage.createSchemaVersion(subject,
                    payload.schemaType, payload.schema, false);
            return schema.thenApply(s -> new CreateSchemaResponse(s.getId()));
        }

    }
}
