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
package io.streamnative.pulsar.handlers.kop.schemaregistry.model;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl.MemorySchemaStorage;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;

public class CompatibilityCheckerTest {

    private static final String SUBJECT = "ssss";

    private static final String AVRO_SCHEMA = "{\"namespace\": \"example.avro\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"user\",\n"
            + " \"fields\": [\n"
            + "     {\"name\": \"name\", \"type\": \"string\"},\n"
            + "     {\"name\": \"favorite_number\",  \"type\": \"int\"}"
            + " ]\n"
            + "}";

    private static final String PROTOBUF_SCHEMA = "syntax = \"proto3\";\n"
            + "package com.acme;\n"
            + "\n"
            + "import \"other.proto\";\n"
            + "\n"
            + "message MyRecord {\n"
            + "  string f1 = 1;\n"
            + "}";

    private static final String JSON_SCHEMA = "{\"type\":\"object\",\"properties\":{\"f1\":{\"type\":\"string\"}}}";

    private static final String AVRO_SCHEMA_ADDED_FIELD_WITH_DEFAULT = "{\"namespace\": \"example.avro\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"user\",\n"
            + " \"fields\": [\n"
            + "     {\"name\": \"name\", \"type\": \"string\"},\n"
            + "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n"
            + "     {\"name\": \"favorite_color\", \"type\": \"string\", \"default\": \"green\"}\n"
            + " ]\n"
            + "}";

    private static final String AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT = "{\"namespace\": \"example.avro\",\n"
            + " \"type\": \"record\",\n"
            + " \"name\": \"user\",\n"
            + " \"fields\": [\n"
            + "     {\"name\": \"name\", \"type\": \"string\"},\n"
            + "     {\"name\": \"favorite_number\",  \"type\": \"int\"},\n"
            + "     {\"name\": \"favorite_color\", \"type\": \"string\"}\n"
            + " ]\n"
            + "}";

    @Test
    public void testNoOtherVersions() throws Exception {
        for (CompatibilityChecker.Mode mode : CompatibilityChecker.Mode.values()) {
            assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, mode));
            assertTrue(executeTest(Schema.TYPE_PROTOBUF, PROTOBUF_SCHEMA, mode));
        }
    }

    @Test
    public void testSame() throws Exception {
        for (CompatibilityChecker.Mode mode : CompatibilityChecker.Mode.values()) {
            assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, mode, AVRO_SCHEMA));
        }
        for (CompatibilityChecker.Mode mode : CompatibilityChecker.Mode.values()) {
            assertTrue(executeTest(Schema.TYPE_JSON, JSON_SCHEMA, mode, JSON_SCHEMA));
        }
        for (CompatibilityChecker.Mode mode : CompatibilityChecker.Mode.SUPPORTED_FOR_PROTOBUF) {
            assertTrue(executeTest(Schema.TYPE_PROTOBUF, PROTOBUF_SCHEMA, mode, PROTOBUF_SCHEMA));
        }
    }

    @Test
    public void testAddField() throws Exception {
        // add optional field
        assertTrue(
                executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_WITH_DEFAULT, CompatibilityChecker.Mode.BACKWARD,
                        AVRO_SCHEMA));
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_WITH_DEFAULT,
                CompatibilityChecker.Mode.BACKWARD_TRANSITIVE,
                AVRO_SCHEMA));
        assertTrue(
                executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_WITH_DEFAULT, CompatibilityChecker.Mode.FORWARD,
                        AVRO_SCHEMA));
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_WITH_DEFAULT,
                CompatibilityChecker.Mode.FORWARD_TRANSITIVE,
                AVRO_SCHEMA));
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_WITH_DEFAULT, CompatibilityChecker.Mode.FULL,
                AVRO_SCHEMA));
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_WITH_DEFAULT,
                CompatibilityChecker.Mode.FULL_TRANSITIVE,
                AVRO_SCHEMA));


        // add non-optional field
        assertFalse(
                executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT, CompatibilityChecker.Mode.BACKWARD,
                        AVRO_SCHEMA));
        // more schemas, but we check only against the latest version with BACKWARD
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT, CompatibilityChecker.Mode.BACKWARD,
                AVRO_SCHEMA, AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT));
        // more schemas, we are checking against all previous versions with BACKWARD_TRANSITIVE
        assertFalse(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT,
                CompatibilityChecker.Mode.BACKWARD_TRANSITIVE,
                AVRO_SCHEMA, AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT));

        assertFalse(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT,
                CompatibilityChecker.Mode.BACKWARD_TRANSITIVE,
                AVRO_SCHEMA));
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT, CompatibilityChecker.Mode.FORWARD,
                AVRO_SCHEMA));
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT,
                CompatibilityChecker.Mode.FORWARD_TRANSITIVE,
                AVRO_SCHEMA));
        assertFalse(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT, CompatibilityChecker.Mode.FULL,
                AVRO_SCHEMA));
        assertFalse(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT,
                CompatibilityChecker.Mode.FULL_TRANSITIVE,
                AVRO_SCHEMA));
    }

    @Test
    public void testDeleteField() throws Exception {
        // delete optional field
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.BACKWARD,
                AVRO_SCHEMA_ADDED_FIELD_WITH_DEFAULT));
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.BACKWARD_TRANSITIVE,
                AVRO_SCHEMA_ADDED_FIELD_WITH_DEFAULT));
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.FORWARD,
                AVRO_SCHEMA_ADDED_FIELD_WITH_DEFAULT));
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.FORWARD_TRANSITIVE,
                AVRO_SCHEMA_ADDED_FIELD_WITH_DEFAULT));
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.FULL,
                AVRO_SCHEMA_ADDED_FIELD_WITH_DEFAULT));
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.FULL_TRANSITIVE,
                AVRO_SCHEMA_ADDED_FIELD_WITH_DEFAULT));

        // delete non optional field
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.BACKWARD,
                AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT));
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.BACKWARD_TRANSITIVE,
                AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT));
        assertFalse(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.FORWARD,
                AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT));
        // we are checking only against the latest version with FORWARD
        assertTrue(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.FORWARD,
                AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT, AVRO_SCHEMA));
        // we are checking against all the versions with FORWARD_TRANSITIVE
        assertFalse(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.FORWARD_TRANSITIVE,
                AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT));

        assertFalse(
                executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.FORWARD_TRANSITIVE, AVRO_SCHEMA,
                        AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT));
        assertFalse(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.FULL,
                AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT));
        assertFalse(executeTest(Schema.TYPE_AVRO, AVRO_SCHEMA, CompatibilityChecker.Mode.FULL_TRANSITIVE,
                AVRO_SCHEMA_ADDED_FIELD_NO_DEFAULT));
    }

    private boolean executeTest(String type, String schemaDefinition, CompatibilityChecker.Mode mode,
                                String... versions) throws InterruptedException, ExecutionException {
        SchemaStorage schemaStorage = new MemorySchemaStorage("test");
        // ensure that we can write everything to the SchemaRegistry
        schemaStorage.setCompatibilityMode(SUBJECT, CompatibilityChecker.Mode.NONE);
        Set<Integer> ids = new HashSet<>();
        for (String version : versions) {
            Schema schema = schemaStorage.createSchemaVersion(SUBJECT, type, version, true).get();
            // ensure that we really created a version
            assertTrue(ids.add(schema.getId()));
        }

        schemaStorage.setCompatibilityMode(SUBJECT, mode);
        Schema schema = Schema
                .builder()
                .type(type)
                .schemaDefinition(schemaDefinition)
                .build();
        return CompatibilityChecker.verify(schema, SUBJECT, schemaStorage).get();

    }


}
