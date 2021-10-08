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

import io.streamnative.pulsar.handlers.kop.schemaregistry.model.Schema;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorage;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.SchemaStorageAccessor;
import java.util.List;
import lombok.AllArgsConstructor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@AllArgsConstructor
public class SchemaStorageTestsBase {
    private SchemaStorageAccessor storageAccessor;

    @Test
    public void testWriteGet() throws SchemaStorageException {
        SchemaStorage storage = storageAccessor.getSchemaStorageForTenant("test-tenant");
        String subject1 = "aa";
        Schema schemaVersion = storage.createSchemaVersion(subject1, Schema.TYPE_AVRO, "{test}", true);
        Schema lookup = storage.findSchemaById(schemaVersion.getId());
        assertEquals(schemaVersion, lookup);
        List<Integer> versions = storage.deleteSubject(subject1);
        assertEquals(1, versions.size());
        lookup = storage.findSchemaById(schemaVersion.getId());
        assertNull(lookup);

        String subject2 = "bb";
        Schema schemaVersion2 = storage.createSchemaVersion(subject2, Schema.TYPE_AVRO, "{test}", true);
        Schema lookup2 = storage.findSchemaById(schemaVersion2.getId());
        assertEquals(schemaVersion2, lookup2);


        Schema schemaVersion3 = storage.createSchemaVersion(subject2, Schema.TYPE_AVRO, "{test}", false);
        Schema lookup3 = storage.findSchemaById(schemaVersion3.getId());
        assertEquals(schemaVersion3, lookup3);

        // we must have received the same schema id
        assertEquals(lookup2, lookup3);

        Schema schemaVersion4 = storage.createSchemaVersion(subject2, Schema.TYPE_AVRO, "{test}", true);
        Schema lookup4 = storage.findSchemaById(schemaVersion4.getId());
        assertEquals(schemaVersion4, lookup4);

        assertNotEquals(lookup3.getId(), lookup4.getId());

        Schema lookupNonExistingSchema = storage.findSchemaById(-10);
        assertNull(lookupNonExistingSchema);

    }

    @AfterClass
    public void stopAll() {
        storageAccessor.close();
    }
}
