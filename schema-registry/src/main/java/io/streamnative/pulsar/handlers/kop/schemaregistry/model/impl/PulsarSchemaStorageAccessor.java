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
package io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl;

import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorageAccessor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;

@AllArgsConstructor
@Slf4j
public class PulsarSchemaStorageAccessor implements SchemaStorageAccessor {
    private final ConcurrentHashMap<String, PulsarSchemaStorage> tenants = new ConcurrentHashMap<>();
    private final Function<String, PulsarClient> authenticatedClientBuilder;
    private final String namespaceName;
    private final String topicName;

    @Override
    public PulsarSchemaStorage getSchemaStorageForTenant(String tenant) throws SchemaStorageException {
        if (tenant == null) {
            throw new SchemaStorageException("Invalid Tenant null");
        }
        return tenants.computeIfAbsent(tenant, t -> {
            String fullTopicName = "persistent://" + t + "/" + namespaceName + "/" + topicName;
            log.info("Building Pulsar Client for Schema Registry for Tenant {}, data topic {}", tenant, fullTopicName);
            PulsarClient pulsarClient = authenticatedClientBuilder.apply(t);
            return new PulsarSchemaStorage(t, pulsarClient, fullTopicName);
        });
    }

    public void clear() {
        tenants.clear();
    }

    public void close() {
        tenants.clear();
    }
}
