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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import io.streamnative.pulsar.handlers.kop.schemaregistry.model.CompatibilityChecker;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.Schema;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorage;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorageAccessor;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.AllArgsConstructor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@AllArgsConstructor
public class SchemaStorageTestsBase {
    private SchemaStorageAccessor storageAccessor;

    private static final String AVRO_SCHEMA = "{\"namespace\": \"example.avro\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"user\",\n"
            + " \"fields\": [\n"
            + "     {\"name\": \"name\", \"type\": \"string\"},\n"
            + "     {\"name\": \"favorite_number\",  \"type\": \"int\"}"
            + " ]\n"
            + "}";

    private static final String AVRO_SCHEMA_ADDED_FIELD_WITHOUT_DEFAULT = "{\"namespace\": \"example.avro\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"user\",\n"
            + " \"fields\": [\n"
            + "     {\"name\": \"name\", \"type\": \"string\"},\n"
            + "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n"
            + "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n"
            + " ]\n"
            + "}";

    @Test
    public void testWriteGet() throws Exception {
        SchemaStorage storage = storageAccessor.getSchemaStorageForTenant("test-tenant");
        String subject1 = "aa";
        Schema schemaVersion = storage.createSchemaVersion(subject1, Schema.TYPE_AVRO, "{test}", true).get();
        Schema lookup = storage.findSchemaById(schemaVersion.getId()).get();
        assertEquals(schemaVersion, lookup);
        List<Integer> versions = storage.deleteSubject(subject1).get();
        assertEquals(1, versions.size());
        lookup = storage.findSchemaById(schemaVersion.getId()).get();
        assertNull(lookup);

        String subject2 = "bb";
        Schema schemaVersion2 = storage.createSchemaVersion(subject2, Schema.TYPE_AVRO, "{test}", true).get();
        Schema lookup2 = storage.findSchemaById(schemaVersion2.getId()).get();
        assertEquals(schemaVersion2, lookup2);


        Schema schemaVersion3 = storage.createSchemaVersion(subject2, Schema.TYPE_AVRO, "{test}", false).get();
        Schema lookup3 = storage.findSchemaById(schemaVersion3.getId()).get();
        assertEquals(schemaVersion3, lookup3);

        // we must have received the same schema id
        assertEquals(lookup2, lookup3);

        Schema schemaVersion4 = storage.createSchemaVersion(subject2, Schema.TYPE_AVRO, "{test}", true).get();
        Schema lookup4 = storage.findSchemaById(schemaVersion4.getId()).get();
        assertEquals(schemaVersion4, lookup4);

        assertNotEquals(lookup3.getId(), lookup4.getId());

        Schema lookupNonExistingSchema = storage.findSchemaById(-10).get();
        assertNull(lookupNonExistingSchema);


        assertEquals(CompatibilityChecker.Mode.NONE, storage.getCompatibilityMode(subject1).get());

        for (CompatibilityChecker.Mode mode : CompatibilityChecker.Mode.values()) {
            storage.setCompatibilityMode(subject1, mode).get();
            assertEquals(mode, storage.getCompatibilityMode(subject1).get());
        }

        String subject3 = "cc";
        storage.setCompatibilityMode(subject3, CompatibilityChecker.Mode.FULL_TRANSITIVE).get();
        storage.createSchemaVersion(subject3, Schema.TYPE_AVRO, AVRO_SCHEMA, true).get();

        ExecutionException executionException = expectThrows(ExecutionException.class, () -> {
            storage.createSchemaVersion(subject3, Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_WITHOUT_DEFAULT, true)
                    .get();
        });
        assertTrue(
                executionException.getCause() instanceof CompatibilityChecker.IncompatibleSchemaChangeException,
                "Unexcepted exception " + executionException.getCause().getClass());


    }

    @AfterClass
    public void stopAll() {
        storageAccessor.close();
    }
}
