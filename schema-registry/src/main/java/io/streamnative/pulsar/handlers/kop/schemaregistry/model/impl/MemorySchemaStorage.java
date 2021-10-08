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

import io.streamnative.pulsar.handlers.kop.schemaregistry.model.Schema;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorage;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MemorySchemaStorage implements SchemaStorage {
    private final ConcurrentHashMap<Integer, Schema> schemas = new ConcurrentHashMap<>();
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
    public Schema findSchemaById(int id) throws SchemaStorageException {
        return schemas.get(id);
    }

    @Override
    public Schema findSchemaBySubjectAndVersion(String subject, int version) {
        return schemas
                .values()
                .stream()
                .filter(s -> s.getSubject().equals(subject)
                        && s.getVersion() == version)
                .findAny()
                .orElse(null);
    }

    @Override
    public List<Schema> findSchemaByDefinition(String schemaDefinition) {
        return schemas
                .values()
                .stream()
                .filter(s -> s.getSchemaDefinition().equals(schemaDefinition))
                .sorted(Comparator.comparing(Schema::getId)) // this is good for unit tests
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getAllSubjects() {
        return schemas
                .values()
                .stream()
                .map(Schema::getSubject)
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public List<Integer> getAllVersionsForSubject(String subject) {
        return schemas
                .values()
                .stream()
                .filter(s -> s.getSubject().equals(subject))
                .map(Schema::getVersion)
                .sorted() // this is goodfor unit tests
                .collect(Collectors.toList());
    }

    @Override
    public List<Integer> deleteSubject(String subject) {
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
        return versions;
    }

    @Override
    public Schema createSchemaVersion(String subject, String schemaType,
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
                return found;
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
        return newSchema;
    }

    public void storeSchema(Schema schema) {
        schemas.put(schema.getId(), schema);
    }

    public void clear() {
        schemas.clear();
    }
}
