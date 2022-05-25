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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.CompatibilityChecker;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.Schema;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorage;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class PulsarSchemaStorage implements SchemaStorage, Closeable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ConcurrentHashMap<String, CompatibilityChecker.Mode> compatibility = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, SchemaEntry> schemas = new ConcurrentHashMap<>();
    private final PulsarClient pulsarClient;
    private final String topic;
    private final String tenant;
    private CompletableFuture<Reader<byte[]>> reader;
    private CompletableFuture<?> currentReadHandle;

    PulsarSchemaStorage(String tenant, PulsarClient client, String topic) {
        this.tenant = tenant;
        this.pulsarClient = client;
        this.topic = topic;
    }

    private static CompletableFuture<Schema> getSchemaFromSchemaEntry(CompletableFuture<SchemaEntry> res) {
        return res.thenApply(PulsarSchemaStorage::getSchemaFromSchemaEntry);
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
    public String getTenant() {
        return tenant;
    }

    private synchronized CompletableFuture<Reader<byte[]>> getReaderHandle() {
        return reader;
    }

    private synchronized CompletableFuture<Reader<byte[]>> ensureReaderHandle() {
        if (reader == null) {
            reader = pulsarClient.newReader()
                    .topic(topic)
                    .startMessageId(MessageId.earliest)
                    .startMessageIdInclusive()
                    .subscriptionRolePrefix("kafka-schema-registry")
                    .createAsync();
        }
        return reader;
    }

    private CompletableFuture<?> readNextMessageIfAvailable(Reader<byte[]> reader) {
        return reader
                .hasMessageAvailableAsync()
                .thenCompose(hasMessageAvailable -> {
                    if (hasMessageAvailable == null
                            || !hasMessageAvailable) {
                        return CompletableFuture.completedFuture(null);
                    } else {
                        CompletableFuture<Message<byte[]>> opMessage = reader.readNextAsync();
                        return opMessage.thenCompose(msg -> {
                            byte[] value = msg.getValue();
                            applyOpToLocalMemory(value);
                            return readNextMessageIfAvailable(reader);
                        });
                    }
                });
    }

    // visible for testing
    synchronized CompletableFuture<?> ensureLatestData() {
        return ensureLatestData(false);
    }

    synchronized CompletableFuture<?> ensureLatestData(boolean beforeWrite) {
        if (currentReadHandle != null) {
            if (beforeWrite) {
                // we are inside a write loop, so
                // we must ensure that we start to read now
                // otherwise the write would use non up-to-date data
                // so let's finish the current loop
                log.info("A read was already pending, starting a new one in order to ensure consistency");
                return currentReadHandle
                        .thenCompose(___ -> ensureLatestData(false));
            }
            // if there is an ongoing read operation then complete it
            return currentReadHandle;
        }
        // please note that the read operation is async,
        // and it is not execute inside this synchronized block
        CompletableFuture<Reader<byte[]>> readerHandle = ensureReaderHandle();
        final CompletableFuture<?> newReadHandle =
                readerHandle.thenCompose(this::readNextMessageIfAvailable);
        currentReadHandle = newReadHandle;
        return newReadHandle.whenComplete((a, b) -> {
            endReadLoop(newReadHandle);
            if (b != null) {
                throw new CompletionException(b);
            }
        });
    }

    private synchronized void endReadLoop(CompletableFuture<?> handle) {
        if (handle == currentReadHandle) {
            currentReadHandle = null;
        }
    }

    @Override
    public CompletableFuture<Schema> findSchemaById(int id) {
        return getSchemaFromSchemaEntry(fetchSchemaEntry(() -> schemas.get(id)));
    }

    @Override
    public CompletableFuture<Schema> findSchemaBySubjectAndVersion(String subject, int version) {
        return getSchemaFromSchemaEntry(fetchSchemaEntry(() -> schemas
                .values()
                .stream()
                .filter(s -> s.getSubject().equals(subject)
                        && s.getVersion() == version)
                .findAny()
                .orElse(null)));
    }

    private CompletableFuture<SchemaEntry> fetchSchemaEntry(Supplier<SchemaEntry> procedure) {
        return fetch(procedure,
                (schemaEntry) -> schemaEntry != null && schemaEntry.status == SchemaStatus.DELETED,
                schemaEntry -> schemaEntry == null);
    }

    private <T> CompletableFuture<T> fetch(Supplier<T> procedure,
                                           Function<T, Boolean> isDeleted,
                                           Function<T, Boolean> requiresFetch) {
        T res = procedure.get();
        if (isDeleted.apply(res)) {
            return CompletableFuture.completedFuture(null);
        }
        if (requiresFetch.apply(res)) {
            // ensure we are in sync with the latest write
            return ensureLatestData().thenApply(___ -> {
                T res2 = procedure.get();
                if (isDeleted.apply(res2)) {
                    return null;
                }
                return res2;
            });
        } else {
            // we are happy with the result, so return it to the caller
            if (isDeleted.apply(res)) {
                return CompletableFuture.completedFuture(null);
            }
            return CompletableFuture.completedFuture(res);
        }

    }

    @Override
    public CompletableFuture<List<Schema>> findSchemaByDefinition(String schemaDefinition) {
        return fetch(
                () -> schemas
                        .values()
                        .stream()
                        .filter(s -> s.getSchemaDefinition().equals(schemaDefinition))
                        .sorted(Comparator.comparing(SchemaEntry::getId)) // this is good for unit tests
                        .collect(Collectors.toList())
                , (res) -> false  // not applicable
                , (res) -> res.isEmpty())  // fetch again if nothing found, useful for demos/testing
                .thenApply(l -> {
                    return l
                            .stream()
                            .map(PulsarSchemaStorage::getSchemaFromSchemaEntry)
                            .collect(Collectors.toList());
                });
    }

    @Override
    public CompletableFuture<List<String>> getAllSubjects() {
        return ensureLatestData().thenApply(___ ->
                schemas
                        .values()
                        .stream()
                        .map(SchemaEntry::getSubject)
                        .distinct()
                        .sorted() // this is good for unit tests
                        .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<List<Integer>> getAllVersionsForSubject(String subject) {
        return ensureLatestData().thenApply(___ -> schemas
                .values()
                .stream()
                .filter(s -> s.getSubject().equals(subject) && s.status != SchemaStatus.DELETED)
                .map(SchemaEntry::getVersion)
                .sorted() // this is good for unit tests
                .collect(Collectors.toList()));
    }

    private synchronized <T> CompletableFuture<List<T>> executeWriteOp(Supplier<List<Map.Entry<Op, T>>> opBuilder) {
        log.info("opening exclusive producer to {}", topic);
        CompletableFuture<Producer<byte[]>> producerHandle = pulsarClient.newProducer()
                .enableBatching(false)
                .topic(topic)
                .accessMode(ProducerAccessMode.WaitForExclusive)
                .blockIfQueueFull(true)
                .createAsync();
        return producerHandle.thenCompose(opProducer -> {
            // nobody can write now to the topic
            // wait for local cache to be up-to-date
            CompletableFuture<List<T>> dummy = ensureLatestData(true)
                    .thenCompose((___) -> {
                        // build the Op, this will usually use the contents of the local cache
                        List<Map.Entry<Op, T>> ops = opBuilder.get();
                        List<T> res = new ArrayList<>();
                        List<CompletableFuture<?>> sendHandles = new ArrayList<>();
                        // write to Pulsar
                        // if the write fails we lost the lock
                        for (Map.Entry<Op, T> action : ops) {
                            Op op = action.getKey();
                            // if "op" is null, then we do not have to write to Pulsar
                            if (op != null) {
                                log.info("writing {} to Pulsar", op);
                                if (!op.tenant.equals(getTenant())) {
                                    sendHandles.add(FutureUtil.failedFuture(new SchemaStorageException(
                                            "Invalid tenant " + op.tenant + ", expected " + tenant)));
                                } else {
                                    byte[] serialized = serializeOp(op);
                                    String key = op.buildMessageKey();
                                    sendHandles.add(opProducer
                                            .newMessage()
                                            .key(key)
                                            .value(serialized)
                                            .sendAsync().thenAccept((msgId) -> {
                                        log.info("written {} as {} with key {} to Pulsar", op, msgId, key);
                                        // write to local memory
                                        applyOpToLocalMemory(op);
                                    }));
                                }
                            }
                            res.add(action.getValue());
                        }

                        return CompletableFuture
                                .allOf(sendHandles.toArray(new CompletableFuture[0]))
                                .thenApply(____ -> res);

                    });
            // ensure that we release the exclusive producer in any case
            dummy.whenComplete((___, err) -> {
                opProducer.closeAsync();
            });
            return dummy;
        });


    }

    private byte[] serializeOp(Op op) {
        try {
            return MAPPER.writeValueAsBytes(op);
        } catch (JsonProcessingException err) {
            throw new RuntimeException(err);
        }
    }

    private void applyOpToLocalMemory(byte[] serialized) {
        try {
            Op op = MAPPER.readValue(serialized, Op.class);
            applyOpToLocalMemory(op);
        } catch (IOException err) {
            log.error("Ignoring malformed entry {}", new String(serialized, StandardCharsets.UTF_8));
        }
    }

    private void applyOpToLocalMemory(Op op) {
        if (op.isCompatibilityModeChange()) {
            try {
                compatibility.put(op.subject, CompatibilityChecker.Mode.valueOf(op.compatibilityMode));
            } catch (IllegalArgumentException err) {
                log.error("Unrecognized mode, skip op", op);
            }
        } else {
            SchemaEntry schemaEntry = op.toSchemaEntry();
            schemas.put(schemaEntry.id, schemaEntry);
        }
    }

    @Override
    public CompletableFuture<List<Integer>> deleteSubject(String subject) {
        return executeWriteOp(() -> {
            List<SchemaEntry> entriesToDelete = schemas
                    .values()
                    .stream()
                    .filter(s -> s.getTenant().equals(tenant) && s.getSubject().equals(subject))
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
    }

    @Override
    public CompletableFuture<Schema> createSchemaVersion(String subject, String schemaType, String schemaDefinition,
                                                         boolean forceCreate) {
        if (!forceCreate) {
            // read from cache, this is the most common case
            CompletableFuture<SchemaEntry> found = fetchSchemaEntry(() -> schemas
                    .values()
                    .stream()
                    .filter(s -> s.getTenant().equals(tenant)
                            && s.getSubject().equals(subject)
                            && s.getSchemaDefinition().equals(schemaDefinition))
                    .sorted(Comparator.comparing(SchemaEntry::getVersion).reversed())
                    .findFirst()
                    .orElse(null));
            return found.thenCompose(schemaEntry -> {
                if (schemaEntry != null) {
                    return CompletableFuture.completedFuture(getSchemaFromSchemaEntry(schemaEntry));
                } else {
                    // execute the operation, in write lock
                    return executeWriteOp(buildWriteSchemaOp(subject,
                            schemaType, schemaDefinition, forceCreate))
                            // this function will always return something
                            .thenApply(sr -> {
                                return getSchemaFromSchemaEntry(sr.get(0));
                            });
                }
            });
        } else {
            // execute the operation, in write lock
            return executeWriteOp(buildWriteSchemaOp(subject,
                    schemaType, schemaDefinition, forceCreate))
                    // this function will always return something
                    .thenApply(sr -> {
                        return getSchemaFromSchemaEntry(sr.get(0));
                    });
        }
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

            // we are inside the lock, so we know that every local variable is up-to-date
            final CompatibilityChecker.Mode compatibilityMode = compatibility.getOrDefault(subject,
                    CompatibilityChecker.Mode.NONE);
            if (compatibilityMode != CompatibilityChecker.Mode.NONE) {

                // we can extract all the versions
                // we already have them in memory
                List<Schema> allSchemas = schemas
                        .values()
                        .stream()
                        .filter(s -> s.getSubject().equals(subject))
                        .sorted(Comparator.comparing(SchemaEntry::getId))
                        .map(PulsarSchemaStorage::getSchemaFromSchemaEntry)
                        .collect(Collectors.toList());

                boolean result =
                        CompatibilityChecker.verify(schemaDefinition, schemaType, compatibilityMode, allSchemas);
                log.info("schema verification result: {}", result);
                if (!result) {
                    throw new CompatibilityChecker
                            .IncompatibleSchemaChangeException(
                            "Schema is not compatible according to " + compatibilityMode
                                    + " compatibility mode");
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

    @Override
    public void close() {
        CompletableFuture<Reader<byte[]>> currentReadHandle = getReaderHandle();
        if (currentReadHandle != null) {
            currentReadHandle.thenAccept(reader -> {
                try {
                    reader.close();
                } catch (Exception err) {
                    // ignore
                }
            });
        }
    }

    @Override
    public CompletableFuture<CompatibilityChecker.Mode> getCompatibilityMode(String subject) {
        return ensureLatestData()
                .thenApply(___ -> {
                    return compatibility.getOrDefault(subject, CompatibilityChecker.Mode.NONE);
                });
    }

    @Override
    public CompletableFuture<Void> setCompatibilityMode(String subject, CompatibilityChecker.Mode mode) {
        return executeWriteOp(() -> {
            return Arrays.asList(
                    new AbstractMap.SimpleImmutableEntry<>(
                            Op.builder()
                                    .subject(subject)
                                    .tenant(tenant)
                                    .compatibilityMode(mode.name())
                                    .build(),
                            null
                    ));
        }).thenApply(___ -> null);
    }

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
        String compatibilityMode;

        boolean isCompatibilityModeChange() {
            return compatibilityMode != null;
        }

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

        String buildMessageKey() {
            if (compatibilityMode != null) {
                return tenant + "_" + subject + "_" + "compatibilityMode";
            } else {
                return tenant + "_" + subject + "_v" + schemaVersion;
            }
        }
    }
}
