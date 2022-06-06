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
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.CompatibilityChecker;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorage;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorageAccessor;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Data;

public class ConfigResource extends AbstractResource {

    public ConfigResource(SchemaStorageAccessor schemaStorageAccessor,
                          SchemaRegistryRequestAuthenticator schemaRegistryRequestAuthenticator) {
        super(schemaStorageAccessor, schemaRegistryRequestAuthenticator);
    }

    @Override
    public void register(SchemaRegistryHandler schemaRegistryHandler) {
        schemaRegistryHandler.addProcessor(new PutConfig());
        schemaRegistryHandler.addProcessor(new GetSubjectConfig());
        schemaRegistryHandler.addProcessor(new GetConfig());
        schemaRegistryHandler.addProcessor(new GetMode());
    }

    @Data
    @AllArgsConstructor
    public static final class GetConfigResponse {
        private String compatibilityLevel;
    }

    // GET /config
    public static class GetConfig extends HttpJsonRequestProcessor<Void, GetConfigResponse> {

        public GetConfig() {
            super(Void.class, "/config", GET);
        }

        @Override
        protected CompletableFuture<GetConfigResponse> processRequest(Void payload, List<String> patternGroups,
                                                                      FullHttpRequest request)
                throws Exception {
            return CompletableFuture.completedFuture(new GetConfigResponse(CompatibilityChecker.Mode.NONE.name()));
        }

    }

    @Data
    @AllArgsConstructor
    public static final class GetModeResponse {
        private String mode;
    }

    // GET /mode
    public static class GetMode extends HttpJsonRequestProcessor<Void, GetModeResponse> {

        public GetMode() {
            super(Void.class, "/mode", GET);
        }

        @Override
        protected CompletableFuture<GetModeResponse> processRequest(Void payload, List<String> patternGroups,
                                                                    FullHttpRequest request)
                throws Exception {
            return CompletableFuture.completedFuture(new GetModeResponse("READWRITE"));
        }

    }

    @Data
    public static final class PutConfigRequest {
        private String compatibility;
    }

    // GET /config/${subject}
    public class GetSubjectConfig extends HttpJsonRequestProcessor<Void, GetConfigResponse> {

        public GetSubjectConfig() {
            super(Void.class, "/config/" + STRING_PATTERN, GET);
        }

        @Override
        protected CompletableFuture<GetConfigResponse> processRequest(Void payload, List<String> patternGroups,
                                                                      FullHttpRequest request)
                throws Exception {
            SchemaStorage schemaStorage = getSchemaStorage(request);
            String subject = patternGroups.get(0);
            return schemaStorage.getCompatibilityMode(subject).thenApply(r -> new GetConfigResponse(r.name()));
        }

    }

    // PUT /config/${subject}
    public class PutConfig extends HttpJsonRequestProcessor<PutConfigRequest, GetConfigResponse> {

        public PutConfig() {
            super(PutConfigRequest.class, "/config/" + STRING_PATTERN, PUT);
        }

        @Override
        protected CompletableFuture<GetConfigResponse> processRequest(PutConfigRequest payload,
                                                                      List<String> patternGroups,
                                                                      FullHttpRequest request)
                throws Exception {
            SchemaStorage schemaStorage = getSchemaStorage(request);
            String subject = patternGroups.get(0);
            CompatibilityChecker.Mode mode = CompatibilityChecker.Mode.valueOf(payload.compatibility);
            return schemaStorage.setCompatibilityMode(subject, mode)
                    .thenApply(r -> new GetConfigResponse(payload.compatibility));
        }

    }

}
