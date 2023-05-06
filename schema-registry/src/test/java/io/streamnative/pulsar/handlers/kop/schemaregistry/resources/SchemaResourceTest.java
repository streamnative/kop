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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.pulsar.handlers.kop.schemaregistry.DummyOptionsCORSProcessor;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryHandler;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryRequestAuthenticator;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SimpleAPIServer;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.CompatibilityChecker;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.Schema;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl.MemorySchemaStorageAccessor;
import java.io.FileNotFoundException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Slf4j
public class SchemaResourceTest {

    private static final String TEST_SCHEMA = "{\n"
            + "  \"schema\":"
            + "    \"{"
            + "       \\\"type\\\": \\\"record\\\","
            + "       \\\"name\\\": \\\"test\\\","
            + "       \\\"fields\\\":"
            + "         ["
            + "           {"
            + "             \\\"type\\\": \\\"string\\\","
            + "             \\\"name\\\": \\\"field1\\\""
            + "           }"
            + "          ]"
            + "     }\",\n"
            + "  \"schemaType\": \"AVRO\"\n"
            + "}";

    private static final String TEST_SCHEMA_WITH_ADDED_NON_DEFAULT_FIELD = "{\n"
            + "  \"schema\":"
            + "    \"{"
            + "       \\\"type\\\": \\\"record\\\","
            + "       \\\"name\\\": \\\"test\\\","
            + "       \\\"fields\\\":"
            + "         ["
            + "           {"
            + "             \\\"type\\\": \\\"string\\\","
            + "             \\\"name\\\": \\\"field1\\\""
            + "           },"
            + "           {"
            + "             \\\"type\\\": \\\"string\\\","
            + "             \\\"name\\\": \\\"fieldAddedWithoutDefault\\\""
            + "           }"
            + "          ]"
            + "     }\",\n"
            + "  \"schemaType\": \"AVRO\"\n"
            + "}";

    private SimpleAPIServer server;
    private final MemorySchemaStorageAccessor schemaStorage = new MemorySchemaStorageAccessor();
    private final SchemaRegistryRequestAuthenticator schemaRegistryRequestAuthenticator =
            (r) -> SchemaResource.DEFAULT_TENANT;

    @BeforeClass(alwaysRun = true)
    public void startServer() throws Exception {

        SchemaResource schemaResource = new SchemaResource(schemaStorage, schemaRegistryRequestAuthenticator);
        SubjectResource subjectResource = new SubjectResource(schemaStorage, schemaRegistryRequestAuthenticator);
        ConfigResource configResource = new ConfigResource(schemaStorage, schemaRegistryRequestAuthenticator);
        CompatibilityResource compatibilityResource = new CompatibilityResource(schemaStorage,
                schemaRegistryRequestAuthenticator);
        SchemaRegistryHandler schemaRegistryHandler = new SchemaRegistryHandler();
        schemaResource.register(schemaRegistryHandler);
        subjectResource.register(schemaRegistryHandler);
        configResource.register(schemaRegistryHandler);
        compatibilityResource.register(schemaRegistryHandler);
        schemaRegistryHandler.addProcessor(new DummyOptionsCORSProcessor());
        server = new SimpleAPIServer(schemaRegistryHandler);
        server.startServer();
    }

    @AfterClass(alwaysRun = true)
    public void stopServer() throws Exception {
        if (server != null) {
            server.stopServer();
        }
    }

    @BeforeMethod(alwaysRun = true)
    public void reset() {
        schemaStorage.clear();
    }

    @Test
    public void getSchemaByIdTest() throws Exception {
        putSchema(1, "{SCHEMA-1}");
        String result = server.executeGet("/schemas/ids/1");
        log.info("result {}", result);
        assertEquals(result, "{\n"
                + "  \"schema\" : \"{SCHEMA-1}\"\n"
                + "}");
    }

    @Test
    public void getSchemaByIdWithQueryStringTest() throws Exception {
        putSchema(1, "{SCHEMA-1}");
        String result = server.executeGet("/schemas/ids/1?fetchMaxId=false");
        log.info("result {}", result);
        assertEquals(result, "{\n"
                + "  \"schema\" : \"{SCHEMA-1}\"\n"
                + "}");
    }

    @Test(expectedExceptions = FileNotFoundException.class)
    public void getSchemaByIdNotFoundTest() throws Exception {
        server.executeGet("/schemas/ids/1");
    }

    @Test
    public void getSchemaBySubjectAndVersion() throws Exception {
        putSchema(1, "aaa", "{SCHEMA-1}", 17);
        String result = server.executeGet("/subjects/aaa/versions/17");

        assertEquals(result, "{\n"
                + "  \"id\" : 1,\n"
                + "  \"schema\" : \"{SCHEMA-1}\",\n"
                + "  \"subject\" : \"aaa\",\n"
                + "  \"version\" : 17\n"
                + "}");
    }

    @Test(expectedExceptions = FileNotFoundException.class)
    public void getSchemaBySubjectAndVersionNotFound() throws Exception {
        server.executeGet("/subjects/aaa/versions/1");
    }

    @Test
    public void getRawSchemaBySubjectAndVersion() throws Exception {
        putSchema(1, "aaa", "{SCHEMA-1}", 17);
        String result = server.executeGet("/subjects/aaa/versions/17/schema");
        log.info("result {}", result);
        assertEquals(result, "{SCHEMA-1}");
    }

    @Test(expectedExceptions = FileNotFoundException.class)
    public void getRawSchemaBySubjectAndVersionNotFound() throws Exception {
        server.executeGet("/subjects/aaa/versions/1/schema");
    }

    @Test
    public void getAllSchemaTypesTest() throws Exception {
        String result = server.executeGet("/schemas/types");
        log.info("result {}", result);
        assertEquals(result, "[ \"AVRO\", \"JSON\", \"PROTOBUF\" ]");
    }

    @Test(expectedExceptions = FileNotFoundException.class)
    public void getSchemaAliasesByIdNotFoundTest() throws Exception {
        String result = server.executeGet("/schemas/ids/1/versions");
        log.info("result {}", result);
    }

    @Test
    public void getSchemaAliasesById() throws Exception {
        putSchema(1, "{SCHEMA}");
        putSchema(2, "{SCHEMA}");
        putSchema(3, "{SCHEMA-OTHER}");
        String result = server.executeGet("/schemas/ids/1/versions");
        log.info("result {}", result);
        assertEquals(result, "[ {\n"
                + "  \"subject\" : \"test-subject\",\n"
                + "  \"version\" : 1\n"
                + "}, {\n"
                + "  \"subject\" : \"test-subject\",\n"
                + "  \"version\" : 1\n"
                + "} ]");
    }

    @Test
    public void getAllVersionsSubjects() throws Exception {
        putSchema(1, "subject1", "{SCHEMA1}", 6);
        putSchema(2, "subject2", "{SCHEMA2}", 7);
        putSchema(3, "subject1", "{SCHEMA3}", 8);

        String result = server.executeGet("/subjects/subject1/versions");
        log.info("result {}", result);
        assertEquals(result, "[ 6, 8 ]");
    }

    @Test
    public void getLatestVersionSubject() throws Exception {
        putSchema(1, "subject1", "{SCHEMA1}", 6);
        putSchema(2, "subject2", "{SCHEMA2}", 7);
        putSchema(3, "subject1", "{SCHEMA3}", 8);

        String result = server.executeGet("/subjects/subject1/versions/latest");
        log.info("result {}", result);
        assertEquals(result, "{\n"
                + "  \"id\" : 3,\n"
                + "  \"schema\" : \"{SCHEMA3}\",\n"
                + "  \"subject\" : \"subject1\",\n"
                + "  \"version\" : 8\n"
                + "}");

        server.executeMethod("/subjects/subjectnotexists/versions/latest", "GET",
                null, null, 404);
    }

    @Test
    public void createSchemaTest() throws Exception {

        log.info("body {}", TEST_SCHEMA);
        String jsonResult = server.executePost("/subjects/mysub/versions", TEST_SCHEMA, "application/json");
        Map<String, Object> parsed = new ObjectMapper().readValue(jsonResult, Map.class);
        int schemaId = Integer.parseInt(parsed.get("id") + "");
        String result = server.executeGet("/subjects/mysub/versions");
        log.info("result {}", result);
        assertEquals(result, "[ 1 ]");

        result = server.executeGet("/schemas/ids/" + schemaId);
        log.info("result {}", result);
        assertEquals(result, "{\n"
                + "  \"schema\" : \"{       \\\"type\\\": \\\"record\\\",       \\\"name\\\": \\\"test\\\",       "
                + "\\\"fields\\\":         [           {             \\\"type\\\": \\\"string\\\",             "
                + "\\\"name\\\": \\\"field1\\\"           }          ]     }\"\n"
                + "}");
    }

    @Test
    public void createOrUpdateSchemaTest() throws Exception {

        log.info("body {}", TEST_SCHEMA);
        String jsonResult = server.executePost("/subjects/mysub", TEST_SCHEMA, "application/json");
        Map<String, Object> parsed = new ObjectMapper().readValue(jsonResult, Map.class);
        int schemaId = Integer.parseInt(parsed.get("id") + "");
        String result = server.executeGet("/subjects/mysub/versions");
        log.info("result {}", result);
        assertEquals(result, "[ 1 ]");


        jsonResult = server.executePost("/subjects/mysub", TEST_SCHEMA, "application/json");
        parsed = new ObjectMapper().readValue(jsonResult, Map.class);
        int secondSchemaId = Integer.parseInt(parsed.get("id") + "");
        assertEquals(secondSchemaId, schemaId);

        // new subject
        jsonResult = server.executePost("/subjects/mysub2", TEST_SCHEMA, "application/json");
        parsed = new ObjectMapper().readValue(jsonResult, Map.class);
        int thirdSchemaId = Integer.parseInt(parsed.get("id") + "");
        assertNotEquals(secondSchemaId, thirdSchemaId);

        result = server.executeGet("/schemas/ids/" + schemaId);
        log.info("result {}", result);
        assertEquals(result, "{\n"
                + "  \"schema\" : \"{       \\\"type\\\": \\\"record\\\",       \\\"name\\\": \\\"test\\\",       "
                + "\\\"fields\\\":         [           {             \\\"type\\\": \\\"string\\\",             "
                + "\\\"name\\\": \\\"field1\\\"           }          ]     }\"\n"
                + "}");
    }

    @Test
    public void deleteSubjects() throws Exception {
        putSchema(1, "subject1", "{SCHEMA1}", 6);
        putSchema(2, "subject2", "{SCHEMA2}", 7);
        putSchema(3, "subject1", "{SCHEMA3}", 8);

        String result = server.executeDelete("/subjects/subject1");
        log.info("result {}", result);
        assertEquals(result, "[ 6, 8 ]");

        assertThrows(FileNotFoundException.class, () -> {
            server.executeGet("/subjects/subject1/versions");
        });
        String result2 = server.executeGet("/subjects/subject2/versions");
        assertEquals(result2, "[ 7 ]");
    }

    @Test(expectedExceptions = FileNotFoundException.class)
    public void getAllVersionsSubjectsNotFound() throws Exception {
        server.executeGet("/subjects/subject1/versions");
    }

    @Test(expectedExceptions = FileNotFoundException.class)
    public void getDeleteSubjectsNotFound() throws Exception {
        server.executeDelete("/subjects/subject1/versions");
    }

    @Test
    public void testOptionsAndCORS() throws Exception {
        assertEquals("",
                server.executeMethod("/subjects/test/versions", "",
                        "OPTIONS", "tet/plain", 204));

    }

    @Test
    public void getSetCompatility() throws Exception {
        // default is NONE in KOP
        assertEquals("{\n"
                + "  \"compatibilityLevel\" : \"NONE\"\n"
                + "}", server.executeGet("/config/sub1"));
        for (CompatibilityChecker.Mode mode : CompatibilityChecker.Mode.values()) {
            assertEquals("{\n"
                    + "  \"compatibilityLevel\" : \"" + mode + "\"\n"
                    + "}", server.executeMethod("/config/sub1",
                    "{\n"
                            + "  \"compatibility\" : \"" + mode + "\"\n"
                            + "}", "PUT", "application/json",
                    null
            ));

            assertEquals("{\n"
                    + "  \"compatibilityLevel\" : \"" + mode + "\"\n"
                    + "}", server.executeGet("/config/sub1"));
        }


        // Global configuration
        assertEquals("{\n"
                + "  \"compatibilityLevel\" : \"NONE\"\n"
                + "}", server.executeGet("/config"));

        assertEquals("{\n"
                + "  \"compatibilityLevel\" : \"NONE\"\n"
                + "}", server.executeGet("/config/"));

    }

    @Test
    public void getMode() throws Exception {

        assertEquals("{\n"
                + "  \"mode\" : \"READWRITE\"\n"
                + "}", server.executeGet("/mode"));
    }

    @Test
    public void checkCreateVersions() throws Exception {
        assertEquals("{\n"
                + "  \"id\" : 1\n"
                + "}", server.executePost(
                "/subjects/Kafka-value/versions",
                "{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}",
                "application/vnd.schemaregistry.v1+json"));

        assertEquals("{\n"
                + "  \"id\" : 2,\n"
                + "  \"version\" : 2\n"
                + "}", server.executePost(
                "/subjects/Kafka-value",
                "{\"schema\": \"{\\\"type\\\": \\\"int\\\"}\"}",
                "application/vnd.schemaregistry.v1+json"));
    }

    @Test
    public void checkCompatibility() throws Exception {

        // set FULL_TRANSITIVE
        assertEquals("{\n"
                + "  \"compatibilityLevel\" : \"" + CompatibilityChecker.Mode.FULL_TRANSITIVE + "\"\n"
                + "}", server.executeMethod("/config/Kafka-value",
                "{\n"
                        + "  \"compatibility\" : \"" + CompatibilityChecker.Mode.FULL_TRANSITIVE + "\"\n"
                        + "}", "PUT", "application/json",
                null
        ));

        assertEquals("{\n"
                + "  \"id\" : 1\n"
                + "}", server.executePost(
                "/subjects/Kafka-value/versions",
                "{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}",
                "application/vnd.schemaregistry.v1+json"));

        // verify the same schema is compatible
        assertEquals("{\n"
                + "  \"is_compatible\" : true\n"
                + "}", server.executePost(
                "/compatibility/subjects/Kafka-value/versions/latest",
                "{\"schema\": \"{\\\"type\\\": \\\"string\\\"}\"}",
                "application/vnd.schemaregistry.v1+json"));

        // verify a non-compatible schema
        assertEquals("{\n"
                + "  \"is_compatible\" : false\n"
                + "}", server.executePost(
                "/compatibility/subjects/Kafka-value/versions/latest",
                "{\"schema\": \"{\\\"type\\\": \\\"long\\\"}\"}",
                "application/vnd.schemaregistry.v1+json"));

        // set NONE
        assertEquals("{\n"
                + "  \"compatibilityLevel\" : \"" + CompatibilityChecker.Mode.NONE + "\"\n"
                + "}", server.executeMethod("/config/Kafka-value",
                "{\n"
                        + "  \"compatibility\" : \"" + CompatibilityChecker.Mode.NONE + "\"\n"
                        + "}", "PUT", "application/json",
                null
        ));

        // verify a non-compatible schema, now the result is 'true'
        assertEquals("{\n"
                + "  \"is_compatible\" : true\n"
                + "}", server.executePost(
                "/compatibility/subjects/Kafka-value/versions/latest",
                "{\"schema\": \"{\\\"type\\\": \\\"long\\\"}\"}",
                "application/vnd.schemaregistry.v1+json"));
    }

    @Test
    public void createSchemaWithCheckCompatibility() throws Exception {

        // set FULL_TRANSITIVE
        assertEquals("{\n"
                + "  \"compatibilityLevel\" : \"" + CompatibilityChecker.Mode.FULL_TRANSITIVE + "\"\n"
                + "}", server.executeMethod("/config/mysub",
                "{\n"
                        + "  \"compatibility\" : \"" + CompatibilityChecker.Mode.FULL_TRANSITIVE + "\"\n"
                        + "}", "PUT", "application/json",
                null
        ));

        log.info("body {}", TEST_SCHEMA);
        server.executePost("/subjects/mysub/versions", TEST_SCHEMA, "application/json");
        server.executePost("/subjects/mysub/versions", TEST_SCHEMA_WITH_ADDED_NON_DEFAULT_FIELD,
                "application/json", 409);

        // set FORWARD
        assertEquals("{\n"
                + "  \"compatibilityLevel\" : \"" + CompatibilityChecker.Mode.FORWARD + "\"\n"
                + "}", server.executeMethod("/config/mysub",
                "{\n"
                        + "  \"compatibility\" : \"" + CompatibilityChecker.Mode.FORWARD + "\"\n"
                        + "}", "PUT", "application/json",
                null
        ));

        server.executePost("/subjects/mysub/versions", TEST_SCHEMA_WITH_ADDED_NON_DEFAULT_FIELD,
                "application/json");

    }

    @Test
    public void createUpdateSchemaWithCheckCompatibility() throws Exception {

        // set FULL_TRANSITIVE
        assertEquals("{\n"
                + "  \"compatibilityLevel\" : \"" + CompatibilityChecker.Mode.FULL_TRANSITIVE + "\"\n"
                + "}", server.executeMethod("/config/mysub",
                "{\n"
                        + "  \"compatibility\" : \"" + CompatibilityChecker.Mode.FULL_TRANSITIVE + "\"\n"
                        + "}", "PUT", "application/json",
                null
        ));

        log.info("body {}", TEST_SCHEMA);
        server.executePost("/subjects/mysub", TEST_SCHEMA, "application/json");
        server.executePost("/subjects/mysub", TEST_SCHEMA_WITH_ADDED_NON_DEFAULT_FIELD,
                "application/json", 409);

        // set FORWARD
        assertEquals("{\n"
                + "  \"compatibilityLevel\" : \"" + CompatibilityChecker.Mode.FORWARD + "\"\n"
                + "}", server.executeMethod("/config/mysub",
                "{\n"
                        + "  \"compatibility\" : \"" + CompatibilityChecker.Mode.FORWARD + "\"\n"
                        + "}", "PUT", "application/json",
                null
        ));

        server.executePost("/subjects/mysub", TEST_SCHEMA_WITH_ADDED_NON_DEFAULT_FIELD,
                "application/json");

    }

    @Test
    public void getAllSubjects() throws Exception {
        putSchema(1, "subject1", "{SCHEMA1}");
        putSchema(2, "subject2", "{SCHEMA2}");
        putSchema(3, "subject1", "{SCHEMA3}");

        String result = server.executeGet("/subjects");
        log.info("result {}", result);
        assertEquals(result, "[ \"subject1\", \"subject2\" ]");
    }


    protected void putSchema(int id, String definition) throws Exception {
        putSchema(id, "test-subject", definition);
    }

    protected void putSchema(int id, String subject, String definition) throws Exception {
        putSchema(id, subject, definition, 1);
    }

    protected void putSchema(int id, String subject, String definition, int version) throws Exception {
        Schema schema = Schema.builder()
                .id(id)
                .version(version)
                .schemaDefinition(definition)
                .subject(subject)
                .type(Schema.TYPE_AVRO)
                .tenant(SchemaResource.DEFAULT_TENANT)
                .build();
        schemaStorage.getSchemaStorageForTenant(SchemaResource.DEFAULT_TENANT).storeSchema(schema);
    }

}
