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
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryHandler;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SchemaRegistryRequestAuthenticator;
import io.streamnative.pulsar.handlers.kop.schemaregistry.SimpleAPIServer;
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
            + "           },"
            + "           {"
            + "             \\\"type\\\": \\\"com.acme.Referenced\\\","
            + "             \\\"name\\\": \\\"int\\\""
            + "           }"
            + "          ]"
            + "     }\",\n"
            + "  \"schemaType\": \"AVRO\",\n"
            + "  \"references\": [\n"
            + "    {\n"
            + "       \"name\": \"com.acme.Referenced\",\n"
            + "       \"subject\":  \"childSubject\",\n"
            + "       \"version\": 1\n"
            + "    }\n"
            + "  ]\n"
            + "}";

    private SimpleAPIServer server;
    private MemorySchemaStorageAccessor schemaStorage = new MemorySchemaStorageAccessor();
    private SchemaRegistryRequestAuthenticator schemaRegistryRequestAuthenticator =
                                                            (r) -> SchemaResource.DEFAULT_TENANT;

    @BeforeClass(alwaysRun = true)
    public void startServer() throws Exception {

        SchemaResource schemaResource = new SchemaResource(schemaStorage, schemaRegistryRequestAuthenticator);
        SubjectResource subjectResource = new SubjectResource(schemaStorage, schemaRegistryRequestAuthenticator);
        SchemaRegistryHandler schemaRegistryHandler = new SchemaRegistryHandler();
        schemaResource.register(schemaRegistryHandler);
        subjectResource.register(schemaRegistryHandler);
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

    @Test(expectedExceptions = FileNotFoundException.class)
    public void getSchemaByIdNotFoundTest() throws Exception {
        server.executeGet("/schemas/ids/1");
    }

    @Test
    public void getSchemaBySubjectAndVersion() throws Exception {
        putSchema(1, "aaa", "{SCHEMA-1}", 17);
        String result = server.executeGet("/subjects/aaa/versions/17");

        assertEquals(result, "{\n"
                + "  \"name\" : \"aaa\",\n"
                + "  \"version\" : 17,\n"
                + "  \"schema\" : \"{SCHEMA-1}\"\n"
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

    @Test(expectedExceptions = java.io.FileNotFoundException.class)
    public void getRawSchemaBySubjectAndVersionNotFound() throws Exception {
        server.executeGet("/subjects/aaa/versions/1/schema");
    }

    @Test
    public void getAllSchemaTypesTest() throws Exception {
        String result = server.executeGet("/schemas/types");
        log.info("result {}", result);
        assertEquals(result, "[ \"AVRO\", \"JSON\", \"PROTOBUF\" ]");
    }

    @Test(expectedExceptions = java.io.FileNotFoundException.class)
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
                + "  \"schema\" : \"{       \\\"type\\\": \\\"record\\\",       \\\"name\\\": \\\"test\\\",  "
                + "     \\\"fields\\\":         [           {             \\\"type\\\": \\\"string\\\",       "
                + "      \\\"name\\\": \\\"field1\\\"           },         "
                + "  {             \\\"type\\\": \\\"com.acme.Referenced\\\",   "
                + "          \\\"name\\\": \\\"int\\\"           }          ]     }\"\n"
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
                + "  \"schema\" : \"{       \\\"type\\\": \\\"record\\\",       \\\"name\\\": \\\"test\\\","
                + "       \\\"fields\\\":         "
                + "[           { "
                + "            \\\"type\\\": \\\"string\\\",             \\\"name\\\": \\\"field1\\\"           },  "
                + "         {             \\\"type\\\": \\\"com.acme.Referenced\\\",         "
                + "    \\\"name\\\": \\\"int\\\"    "
                + "       }          ]     }\"\n"
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
