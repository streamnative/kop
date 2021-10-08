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
import java.io.Closeable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;

@Slf4j
public class PulsarSchemaStorage implements SchemaStorage, Closeable {

    // Pulsar Schema instances are stateful, you cannot
    // use them as constants
    private final org.apache.pulsar.client.api.Schema<Op> avroSchema =
                            org.apache.pulsar.client.api.Schema.AVRO(Op.class);

    private final ConcurrentHashMap<Integer, SchemaEntry> schemas = new ConcurrentHashMap<>();
    private final PulsarClient pulsarClient;
    private final String topic;
    private final String tenant;
    private Reader<Op> reader;

    private enum SchemaStatus {
        ACTIVE,
        DELETED
    }

    @Data
    @Builder
    private static final class SchemaEntry {
        private int id;
        private SchemaStatus status;
        private String tenant;
        private int version;
        private String subject;
        private String schemaDefinition;
        private String type;
    }

    @Builder
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class Op {
        int schemaId;
        int schemaVersion;
        String subject;
        String tenant;
        String schemaDefinition;
        SchemaStatus status;
        String type;


        SchemaEntry toSchemaEntry() {
            return SchemaEntry
                    .builder()
                    .id(schemaId)
                    .version(schemaVersion)
                    .subject(subject)
                    .tenant(tenant)
                    .schemaDefinition(schemaDefinition)
                    .status(status)
                    .type(type)
                    .build();
        }
    }

    PulsarSchemaStorage(String tenant, PulsarClient client, String topic) {
        this.tenant = tenant;
        this.pulsarClient = client;
        this.topic = topic;
    }

    @Override
    public String getTenant() {
        return tenant;
    }

    private synchronized Reader<Op> getReader() throws SchemaStorageException {
        try {
            if (reader == null) {
                reader = pulsarClient.newReader(avroSchema)
                        .topic(topic)
                        .startMessageId(MessageId.earliest)
                        .subscriptionRolePrefix("kafak-schema-registry-")
                        .create();
            }
            return reader;
        } catch (PulsarClientException err) {
            throw new SchemaStorageException(err);
        }
    }

    private synchronized void ensureLatestData() throws SchemaStorageException {
        Reader<Op> reader = getReader();
        try {
            while (reader.hasMessageAvailable()) {
                Message<Op> opMessage = reader.readNext(1, TimeUnit.SECONDS);
                if (opMessage != null) {
                    Op value = opMessage.getValue();
                    log.info("read {} from pulsar", value);
                    SchemaEntry schemaEntry = value.toSchemaEntry();
                    schemas.put(schemaEntry.id, schemaEntry);
                }
            }
        } catch (PulsarClientException err) {
            throw new SchemaStorageException(err);
        }
    }

    @Override
    public Schema findSchemaById(int id) throws SchemaStorageException {
        return getSchemaFromSchemaEntry(fetchSchemaEntry(() -> schemas.get(id)));
    }

    private static Schema getSchemaFromSchemaEntry(SchemaEntry res) {
        if (res == null) {
            return null;
        }
        return Schema
                .builder()
                .tenant(res.tenant)
                .id(res.id)
                .version(res.version)
                .subject(res.subject)
                .schemaDefinition(res.schemaDefinition)
                .type(res.type)
                .build();
    }

    @Override
    public Schema findSchemaBySubjectAndVersion(String subject, int version)
                            throws SchemaStorageException {
        return getSchemaFromSchemaEntry(fetchSchemaEntry(() ->schemas
                .values()
                .stream()
                .filter(s-> s.getSubject().equals(subject)
                        && s.getVersion() == version)
                .findAny()
                .orElse(null)));
    }

    private SchemaEntry fetchSchemaEntry(Supplier<SchemaEntry> procedure)
            throws SchemaStorageException {
        return fetch(procedure,
                (schemaEntry) -> schemaEntry != null && schemaEntry.status == SchemaStatus.DELETED,
                schemaEntry ->  schemaEntry == null);
    }

    private <T> T fetch(Supplier<T> procedure,
                        Function<T, Boolean> isDeleted,
                        Function<T, Boolean> requiresFetch)
            throws SchemaStorageException {
        T res = procedure.get();
        if (isDeleted.apply(res)) {
            return null;
        }
        if (requiresFetch.apply(res)) {
            // ensure we are in sync with the latest write
            ensureLatestData();
            res = procedure.get();
        }
        if (isDeleted.apply(res)) {
            return null;
        }
        return res;
    }

    @Override
    public List<Schema> findSchemaByDefinition(String schemaDefinition) throws SchemaStorageException {
        List<SchemaEntry> list = fetch(
                () ->  schemas
                    .values()
                    .stream()
                    .filter(s -> s.getSchemaDefinition().equals(schemaDefinition))
                    .sorted(Comparator.comparing(SchemaEntry::getId)) // this is good for unit tests
                    .collect(Collectors.toList())
                , (res) -> false  // not applicable
                , (res) -> res.isEmpty()); // fetch again if nothing found, useful for demos/testing
        return list
                .stream()
                .map(PulsarSchemaStorage::getSchemaFromSchemaEntry)
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getAllSubjects() throws SchemaStorageException {
        List<String> list = fetch(
                () -> schemas
                        .values()
                        .stream()
                        .map(SchemaEntry::getSubject)
                        .distinct()
                        .collect(Collectors.toList()),
                (res) -> false, // not applicable
                (res) -> res.isEmpty()); // fetch again if nothing found, useful for demos/testing
        return list;
    }

    @Override
    public List<Integer> getAllVersionsForSubject(String subject) throws SchemaStorageException {
        List<Integer> list = fetch(
                () -> schemas
                .values()
                .stream()
                .filter(s -> s.getSubject().equals(subject) && s.status != SchemaStatus.DELETED)
                .map(SchemaEntry::getVersion)
                .sorted() // this is goodfor unit tests
                .collect(Collectors.toList()),
                (res) -> false,  // not applicable
                (res) -> res.isEmpty()); // fetch again if nothing found, useful for demos/testing
        return list;
    }

    private synchronized <T> List<T> executeWriteOp(Supplier<List<Map.Entry<Op, T>>> opBuilder)
            throws SchemaStorageException {
        try (Producer<Op> opProducer = pulsarClient.newProducer(avroSchema)
                .enableBatching(false)
                .topic(topic)
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .blockIfQueueFull(true)
                .create();) {
            // nobody can write now to the topic
            // wait for local cache to be up-to-date
            ensureLatestData();

            // build the Op, this will usually use the contents of the local cache
            List<Map.Entry<Op, T>> ops = opBuilder.get();
            List<T> res = new ArrayList<>();
            // write to Pulsar
            // if the write fails we lost the lock
            for (Map.Entry<Op, T> action : ops) {
                Op op = action.getKey();
                // if "op" is null, then we do not have to write to Pulsar
                if (op != null) {
                    if (!op.tenant.equals(getTenant())) {
                        throw new SchemaStorageException("Invalid tenant " + op.tenant + ", expected " + tenant);
                    }
                    opProducer.send(op);
                    // write to local memory
                    SchemaEntry schemaEntry = op.toSchemaEntry();
                    schemas.put(schemaEntry.id, schemaEntry);
                }
                res.add(action.getValue());
            }

            return res;
        } catch (PulsarClientException err) {
            throw new SchemaStorageException(err);
        }
    }

    @Override
    public List<Integer> deleteSubject(String subject) throws SchemaStorageException {
        List<Integer> versionsRes = executeWriteOp(() -> {
            List<SchemaEntry> entriesToDelete = schemas
                    .values()
                    .stream()
                    .filter(s->s.getTenant().equals(tenant) && s.getSubject().equals(subject))
                    .collect(Collectors.toList());

                List<Map.Entry<Op, Integer>> operationsAndResults =
                        entriesToDelete
                                .stream()
                                .map(schemaEntry -> {
                                    return new AbstractMap.SimpleImmutableEntry<>(
                                            Op.builder()
                                                    .schemaId(schemaEntry.id)
                                                    .subject(schemaEntry.subject)
                                                    .schemaDefinition(null)
                                                    .schemaVersion(schemaEntry.version)
                                                    .status(SchemaStatus.DELETED)
                                                    .tenant(schemaEntry.tenant)
                                                    .build(),
                                            schemaEntry.getVersion()
                                    );
                                })
                                .collect(Collectors.toList());

                return operationsAndResults;
        });
        return versionsRes;
    }

    @Override
    public Schema createSchemaVersion(String subject, String schemaType, String schemaDefinition,
                                      boolean forceCreate) throws SchemaStorageException {
        if (!forceCreate) {
            // read from cache, this is the most common case
            SchemaEntry found = fetchSchemaEntry(() -> schemas
                    .values()
                    .stream()
                    .filter(s -> s.getTenant().equals(tenant)
                            && s.getSubject().equals(subject)
                            && s.getSchemaDefinition().equals(schemaDefinition))
                    .sorted(Comparator.comparing(SchemaEntry::getVersion).reversed())
                    .findFirst()
                    .orElse(null));
            if (found != null) {
                return getSchemaFromSchemaEntry(found);
            }
        }

        // enter the lock
        List<SchemaEntry> schemaRes = executeWriteOp(buildWriteSchemaOp(subject,
                schemaType, schemaDefinition, forceCreate));

        // this function will always return something
        return getSchemaFromSchemaEntry(schemaRes.get(0));
    }

    private Supplier<List<Map.Entry<Op, SchemaEntry>>> buildWriteSchemaOp(String subject,
                                                                          String schemaType,
                                                                          String schemaDefinition,
                                                                          boolean forceCreate) {
        return () -> {

            if (!forceCreate) {
                SchemaEntry found = schemas
                        .values()
                        .stream()
                        .filter(s -> s.getSubject().equals(subject)
                                && s.getSchemaDefinition().equals(schemaDefinition))
                        .sorted(Comparator.comparing(SchemaEntry::getVersion).reversed())
                        .findFirst()
                        .orElse(null);

                if (found != null) {
                    List<Map.Entry<Op, SchemaEntry>> cachedRes =
                     Arrays.asList(new AbstractMap.SimpleImmutableEntry<>((Op) null, found));
                    return cachedRes;
                }
            }

            // select new id, we are inside the write lock
            // also we are sure that the local cache is up-to-date
            int newId = schemas
                    .keySet()
                    .stream()
                    .mapToInt(s -> s)
                    .max()
                    .orElse(0) + 1;
            int newVersion = schemas
                    .values()
                    .stream()
                    .filter(s -> s.getSubject().equals(subject))
                    .map(SchemaEntry::getVersion)
                    .sorted(Comparator.reverseOrder())
                    .findFirst()
                    .orElse(0) + 1;
            Op newSchema = Op
                    .builder()
                    .schemaId(newId)
                    .schemaDefinition(schemaDefinition)
                    .type(schemaType)
                    .subject(subject)
                    .schemaVersion(newVersion)
                    .tenant(tenant)
                    .build();

            return Arrays.asList(new AbstractMap.SimpleImmutableEntry<>(newSchema, newSchema.toSchemaEntry()));
        };
    }

    public void close() {
        // we are not owning the PulsarClient
    }
}
