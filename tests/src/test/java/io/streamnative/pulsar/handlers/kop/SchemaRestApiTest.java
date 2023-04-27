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
package io.streamnative.pulsar.handlers.kop;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.Schema;
import io.streamnative.pulsar.handlers.kop.schemaregistry.resources.SubjectResource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.Cleanup;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test the Schema related REST APIs.
 */
public class SchemaRestApiTest extends KopProtocolHandlerTestBase {

    protected static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(SerializationFeature.INDENT_OUTPUT, true);

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.enableSchemaRegistry = true;
        this.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        this.internalCleanup();
    }

    @Test
    public void testDeleteSubject() throws Exception {
        final var createSchemaRequest = new SubjectResource.CreateSchemaRequest();
        createSchemaRequest.setSchema("{\"type\":\"record\",\"name\":\"User1\",\"namespace\":\"example.avro\""
                + ",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}");
        final var subject = "my-subject";
        sendHttpRequest("POST", "/subjects/" + subject + "/versions",
                MAPPER.writeValueAsString(createSchemaRequest));

        assertEquals(getSubjects(), Collections.singletonList(subject));
        resetSchemaStorage();
        assertEquals(getSubjects(), Collections.singletonList(subject));

        sendHttpRequest("DELETE", "/subjects/" + subject, null);
        assertTrue(getSubjects().isEmpty());
        resetSchemaStorage();
        assertTrue(getSubjects().isEmpty());
    }

    @Test
    public void testGetSubjectByVersion() throws Exception {
        final var createSchemaRequest = new SubjectResource.CreateSchemaRequest();
        final var schemaDefinition = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\""
                + ",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";
        createSchemaRequest.setSchema(schemaDefinition);
        final var subject = "test-get-subject-by-version";
        sendHttpRequest("POST", "/subjects/" + subject + "/versions",
                MAPPER.writeValueAsString(createSchemaRequest));

        final var versions = MAPPER.readValue(
                sendHttpRequest("GET", "/subjects/" + subject + "/versions", null),
                Integer[].class);
        assertEquals(Arrays.asList(versions), Collections.singletonList(1));

        for (String version : new String[]{"1", "latest"}) {
            final var schema = MAPPER.readValue(
                    sendHttpRequest("GET", "/subjects/" + subject + "/versions/" + version, null),
                    Schema.class);
            assertEquals(schema.getVersion(), 1);
            assertEquals(schema.getSubject(), subject);
            assertEquals(schema.getSchemaDefinition(), schemaDefinition);
            assertNull(schema.getType());
        }
    }

    private void resetSchemaStorage() {
        final var handler = getProtocolHandler();
        handler.getSchemaRegistryManager().getSchemaStorage().close();
    }

    private List<String> getSubjects() throws IOException {
        final var output = sendHttpRequest("GET", "/subjects", null);
        return Arrays.asList(MAPPER.readValue(output, String[].class));
    }

    private String sendHttpRequest(final String method, final String path, final String body) throws IOException {
        final var url = new URL(restConnect + path);
        final var conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        if (body != null) {
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Content-Length", Integer.toString(body.length()));
            final var output = conn.getOutputStream();
            output.write(body.getBytes(StandardCharsets.UTF_8));
            output.close();
        }
        @Cleanup final var in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        final var buffer = new StringBuilder();
        while (true) {
            final var line = in.readLine();
            if (line == null) {
                break;
            }
            buffer.append(line);
        }
        return buffer.toString();
    }
}
