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
import io.netty.handler.codec.http.HttpResponseStatus;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryHandler;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryRequestAuthenticator;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorage;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorageAccessor;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl.SchemaStorageException;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class AbstractResource {

    public static final String INT_PATTERN = "([0-9]+)";
    public static final String STRING_PATTERN = "(.+)";

    public static final String GET = "GET";
    public static final String POST = "POST";
    public static final String DELETE = "DELETE";

    public static final String DEFAULT_TENANT = "public";

    protected final SchemaStorageAccessor schemaStorageAccessor;
    protected final SchemaRegistryRequestAuthenticator schemaRegistryRequestAuthenticator;

    protected String getCurrentTenant(FullHttpRequest request) throws Exception {
        return schemaRegistryRequestAuthenticator.authenticate(request);
    }

    protected SchemaStorage getSchemaStorage(FullHttpRequest request) throws SchemaStorageException {
        String currentTenant;
        try {
            currentTenant = getCurrentTenant(request);
        } catch (SchemaStorageException e) {
            throw e;
        }  catch (Exception e) {
            throw new SchemaStorageException(e);
        }
        if (currentTenant == null) {
            throw new SchemaStorageException("Missing or failed authentication",
                    HttpResponseStatus.UNAUTHORIZED.code());
        }
        return schemaStorageAccessor.getSchemaStorageForTenant(currentTenant);
    }

    /**
     * Register all processors.
     * @param schemaRegistryHandler
     */
    public abstract void register(SchemaRegistryHandler schemaRegistryHandler);
}
