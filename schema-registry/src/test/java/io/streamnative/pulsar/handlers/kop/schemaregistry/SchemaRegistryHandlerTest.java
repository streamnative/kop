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
package io.streamnative.pulsar.handlers.kop.schemaregistry;

import static org.testng.Assert.assertEquals;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.streamnative.pulsar.handlers.kop.schemaregistry.model.impl.SchemaStorageException;
import java.io.FileNotFoundException;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SchemaRegistryHandlerTest {

    private SimpleAPIServer server = new SimpleAPIServer(new SchemaRegistryHandler()
            .addProcessor(new DemoHandler())
            .addProcessor(new JsonBodyHandler())
            .addProcessor(new JsonHandler()));

    @BeforeClass(alwaysRun = true)
    public void startServer() throws Exception {
        server.startServer();
    }

    @AfterClass(alwaysRun = true)
    public void stopServer() throws Exception {
        server.stopServer();
    }

    private static class DemoHandler extends HttpRequestProcessor {

        @Override
        public FullHttpResponse processRequest(FullHttpRequest request) {
            if (!request.uri().startsWith("/demo")) {
                return null;
            }
            return buildStringResponse("ok", "text/plain; charset=UTF-8");
        }
    }

    private static class JsonHandler extends HttpRequestProcessor {

        @Data
        @AllArgsConstructor
        private static class SchemaPojo {
            String value;
        }

        @Override
        public FullHttpResponse processRequest(FullHttpRequest request) {
            if (!request.uri().startsWith("/json")) {
                return null;
            }
            String content = request.uri();
            return buildJsonResponse(new SchemaPojo(content), "application/json; charset=UTF-8");
        }
    }


    @Data
    private static class RequestPojo {
        String value;
    }

    @Data
    @AllArgsConstructor
    private static class ResponsePojo {
        String subject;
        String value;
    }

    private static class JsonBodyHandler extends HttpJsonRequestProcessor<RequestPojo, ResponsePojo> {

        public JsonBodyHandler() {
            super(RequestPojo.class, "/subjects/(.*)", "POST");
        }

        @Override
        protected ResponsePojo processRequest(RequestPojo payload, List<String> groups, FullHttpRequest request)
                                                        throws Exception {
            String subject = groups.get(0);
            if (subject.equals("errorsubject401")) {
                throw new SchemaStorageException("Bad auth", HttpResponseStatus.UNAUTHORIZED.code());
            }
            if (subject.equals("errorsubject403")) {
                throw new SchemaStorageException("Forbidden", HttpResponseStatus.FORBIDDEN.code());
            }
            if (subject.equals("errorsubject500")) {
                throw new SchemaStorageException("Error");
            }
            return new ResponsePojo(subject, payload.value);
        }

    }

    @Test
    public void testBasicGet() throws Exception {
        assertEquals("ok", server.executeGet("/demo/ok"));
    }

    @Test
    public void testBasicJson() throws Exception {
        assertEquals("{\n"
                + "  \"value\" : \"/json/test\"\n"
                + "}", server.executeGet("/json/test"));
    }

    @Test
    public void testBasicJsonApi() throws Exception {
        assertEquals("{\n"
                        + "  \"subject\" : \"testsubject\",\n"
                        + "  \"value\" : \"/json/test\"\n"
                + "}",
                server.executePost("/subjects/testsubject", "{\n"
                        + "  \"value\" : \"/json/test\"\n"
                        + "}", "application/json"));
    }

    @Test
    public void testBasicJsonApiError401() throws Exception {
        assertEquals("{\n"
                        + "  \"message\" : \"Bad auth\",\n"
                        + "  \"error_code\" : 401\n"
                        + "}",
                server.executePost("/subjects/errorsubject401", "{\n"
                        + "  \"value\" : \"/json/test\"\n"
                        + "}", "application/json", 401));
    }

    @Test
    public void testBasicJsonApiError403() throws Exception {
        assertEquals("{\n"
                        + "  \"message\" : \"Forbidden\",\n"
                        + "  \"error_code\" : 403\n"
                        + "}",
                server.executePost("/subjects/errorsubject403", "{\n"
                        + "  \"value\" : \"/json/test\"\n"
                        + "}", "application/json", 403));
    }

    @Test
    public void testBasicJsonApiError500() throws Exception {
        assertEquals("{\n"
                        + "  \"message\" : \"Error\",\n"
                        + "  \"error_code\" : 500\n"
                        + "}",
                server.executePost("/subjects/errorsubject500", "{\n"
                        + "  \"value\" : \"/json/test\"\n"
                        + "}", "application/json", 500));
    }

    @Test(expectedExceptions = FileNotFoundException.class)
    public void testBasicNotFound() throws Exception {
        server.executeGet("/notfound");
    }
}
