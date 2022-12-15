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
package io.streamnative.pulsar.handlers.kop.format;

import io.streamnative.pulsar.handlers.kop.schemaregistry.model.Schema;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorage;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorageAccessor;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

@AllArgsConstructor
@Slf4j
public class PulsarAdminSchemaManager implements SchemaManager{

    private static final KeyValueSchemaIdsImpl NO_SCHEMA = new KeyValueSchemaIdsImpl(-1, -1);

    private final String tenant;
    private final PulsarAdmin pulsarAdmin;
    private final SchemaStorageAccessor kafkaSchemaRegistry;
    private final ConcurrentHashMap<String, KeyValueSchemaIds> cache = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<KeyValueSchemaIds> getSchemaIds(String topic, BytesSchemaVersion schemaVersion) {
        if (schemaVersion == null) {
            return CompletableFuture.completedFuture(NO_SCHEMA);
        }
        String subject = topic;
        long version = ByteBuffer.wrap(schemaVersion.get()).getLong();
        String cacheKey = subject + "__" + version;
        KeyValueSchemaIds cachedValue  = cache.get(cacheKey);
        if (cachedValue != null) {
            return CompletableFuture.completedFuture(cachedValue);
        }
        SchemaStorage schemaStorageForTenant = kafkaSchemaRegistry.getSchemaStorageForTenant(tenant);
        return pulsarAdmin.schemas().getSchemaInfoAsync(topic, version)
                    .thenCompose((SchemaInfo schemaInfo) -> {
            if (schemaInfo.getType() == SchemaType.KEY_VALUE) {
                String schemaDefinition = schemaInfo.getSchemaDefinition();
                log.info("lookup schema for {} - {}", subject, schemaDefinition);
                KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
                KeyValueEncodingType keyValueEncodingType = KeyValueSchemaInfo.decodeKeyValueEncodingType(schemaInfo);
                if (keyValueEncodingType != KeyValueEncodingType.SEPARATED) {
                    cache.put(cacheKey, NO_SCHEMA);
                    return CompletableFuture.completedFuture(NO_SCHEMA);
                }
                SchemaInfo keySchema = kvSchemaInfo.getKey();
                SchemaInfo valueSchema = kvSchemaInfo.getValue();
                CompletableFuture<Integer> keySchemaIdFuture;
                if (keySchema.getType() == SchemaType.AVRO) {
                    keySchemaIdFuture = schemaStorageForTenant
                            .createSchemaVersion(subject + "-key", Schema.TYPE_AVRO,
                                    keySchema.getSchemaDefinition(), false)
                            .thenApply(Schema::getId);
                } else {
                    keySchemaIdFuture = CompletableFuture.completedFuture(-1);
                }
                return keySchemaIdFuture.thenCompose((Integer keySchemaId) -> {
                    log.info("keySchemaId {}", keySchemaId);
                    CompletableFuture<Integer> valueSchemaIdFuture;
                    if (valueSchema.getType() == SchemaType.AVRO) {
                        valueSchemaIdFuture = schemaStorageForTenant
                                .createSchemaVersion(subject, Schema.TYPE_AVRO,
                                        valueSchema.getSchemaDefinition(), false)
                                .thenApply(Schema::getId);
                    } else {
                        valueSchemaIdFuture = CompletableFuture.completedFuture(-1);
                    }
                    return valueSchemaIdFuture.thenApply((Integer valueSchemaId) -> {
                        log.info("valueSchemaId {}", valueSchemaId);
                        KeyValueSchemaIdsImpl res = new KeyValueSchemaIdsImpl(keySchemaId, valueSchemaId);
                        cache.put(cacheKey, res);
                        return res;
                    });
                });
            } else if (schemaInfo.getType() == SchemaType.AVRO) {
                String schemaDefinition = schemaInfo.getSchemaDefinition();
                log.info("lookup schema for {} - {}", subject, schemaDefinition);
                return schemaStorageForTenant
                        .createSchemaVersion(subject, Schema.TYPE_AVRO, schemaDefinition, false)
                        .thenApply((Schema kafkaSchema) -> {
                            log.info("lookup schema for {} -> id {}", subject, kafkaSchema.getId());
                            KeyValueSchemaIdsImpl res = new KeyValueSchemaIdsImpl(-1, kafkaSchema.getId());
                            cache.put(cacheKey, res);
                            return res;
                        });
            } else {
                cache.put(cacheKey, NO_SCHEMA);
                return CompletableFuture.completedFuture(NO_SCHEMA);
            }
        });

    }

    @AllArgsConstructor
    private static final class KeyValueSchemaIdsImpl implements  KeyValueSchemaIds {
        private final int keySchemaId;
        private final int valueSchemaId;

        @Override
        public int getKeySchemaId() {
            return keySchemaId;
        }

        @Override
        public int getValueSchemaId() {
            return valueSchemaId;
        }
    }
}
