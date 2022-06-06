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

import io.streamnative.pulsar.handlers.kop.schemaregistry.model.CompatibilityChecker;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.Schema;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorage;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class MemorySchemaStorage implements SchemaStorage {
    private final ConcurrentHashMap<Integer, Schema> schemas = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompatibilityChecker.Mode> compatibility = new ConcurrentHashMap<>();
    private final AtomicInteger schemaIdGenerator = new AtomicInteger();
    private final String tenant;


    public MemorySchemaStorage(String tenant) {
        this.tenant = tenant;
    }

    @Override
    public String getTenant() {
        return tenant;
    }

    @Override
    public CompletableFuture<Schema> findSchemaById(int id) {
        return CompletableFuture.completedFuture(schemas.get(id));
    }

    @Override
    public CompletableFuture<Schema> findSchemaBySubjectAndVersion(String subject, int version) {
        return CompletableFuture.completedFuture(schemas
                .values()
                .stream()
                .filter(s -> s.getSubject().equals(subject)
                        && s.getVersion() == version)
                .findAny()
                .orElse(null));
    }

    @Override
    public CompletableFuture<List<Schema>> findSchemaByDefinition(String schemaDefinition) {
        return CompletableFuture.completedFuture(schemas
                .values()
                .stream()
                .filter(s -> s.getSchemaDefinition().equals(schemaDefinition))
                .sorted(Comparator.comparing(Schema::getId)) // this is good for unit tests
                .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<List<String>> getAllSubjects() {
        return CompletableFuture.completedFuture(schemas
                .values()
                .stream()
                .map(Schema::getSubject)
                .distinct()
                .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<List<Integer>> getAllVersionsForSubject(String subject) {
        return CompletableFuture.completedFuture(schemas
                .values()
                .stream()
                .filter(s -> s.getSubject().equals(subject))
                .map(Schema::getVersion)
                .sorted() // this is goodfor unit tests
                .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<List<Integer>> deleteSubject(String subject) {
        // please note that this implementation is not meant to be fully
        // thread safe, it is only for tests
        List<Integer> keys = schemas
                .values()
                .stream()
                .filter(s -> s.getSubject().equals(subject))
                .map(s -> s.getId())
                .collect(Collectors.toList());
        List<Integer> versions = new ArrayList<>(keys.size());
        keys.forEach(key -> {
            Schema remove = schemas.remove(key);
            if (remove != null) {
                versions.add(remove.getVersion());
            }
        });
        return CompletableFuture.completedFuture(versions);
    }

    @Override
    public CompletableFuture<Schema> createSchemaVersion(String subject, String schemaType,
                                                         String schemaDefinition, boolean forceCreate) {
        // please note that this implementation is not meant to be fully
        // thread safe, it is only for tests

        if (!forceCreate) {
            Schema found = schemas
                    .values()
                    .stream()
                    .filter(s -> s.getSubject().equals(subject)
                            && s.getSchemaDefinition().equals(schemaDefinition))
                    .sorted(Comparator.comparing(Schema::getVersion).reversed())
                    .findFirst()
                    .orElse(null);
            if (found != null) {
                return CompletableFuture.completedFuture(found);
            }
        }

        final CompatibilityChecker.Mode compatibilityMode = compatibility.getOrDefault(subject,
                CompatibilityChecker.Mode.NONE);
        if (compatibilityMode != CompatibilityChecker.Mode.NONE) {

            // we can extract all the versions
            // we already have them in memory
            List<Schema> allSchemas = schemas
                    .values()
                    .stream()
                    .filter(s -> s.getSubject().equals(subject))
                    .sorted(Comparator.comparing(Schema::getId))
                    .collect(Collectors.toList());

            boolean result = CompatibilityChecker.verify(schemaDefinition, schemaType, compatibilityMode, allSchemas);
            log.info("schema verification result: {}", result);
            if (!result) {
                return FutureUtil.failedFuture(new CompatibilityChecker
                        .IncompatibleSchemaChangeException("Schema is not compatible according to " + compatibilityMode
                        + " compatibility mode"));
            }
        }

        int newId = schemaIdGenerator.incrementAndGet();
        int newVersion = schemas
                .values()
                .stream()
                .filter(s -> s.getSubject().equals(subject))
                .map(Schema::getVersion)
                .sorted(Comparator.reverseOrder())
                .findFirst()
                .orElse(0) + 1;
        Schema newSchema = Schema
                .builder()
                .id(newId)
                .schemaDefinition(schemaDefinition)
                .type(schemaType)
                .subject(subject)
                .version(newVersion)
                .tenant(getTenant())
                .build();
        schemas.put(newSchema.getId(), newSchema);
        return CompletableFuture.completedFuture(newSchema);
    }

    public void storeSchema(Schema schema) {
        schemas.put(schema.getId(), schema);
    }

    public void clear() {
        schemas.clear();
    }

    @Override
    public CompletableFuture<CompatibilityChecker.Mode> getCompatibilityMode(String subject) {
        return CompletableFuture.completedFuture(compatibility.getOrDefault(subject, CompatibilityChecker.Mode.NONE));
    }

    @Override
    public CompletableFuture<Void> setCompatibilityMode(String subject, CompatibilityChecker.Mode mode) {
        compatibility.put(subject, mode);
        return CompletableFuture.completedFuture(null);
    }
}
