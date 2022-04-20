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

import java.io.Closeable;
import java.io.IOException;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

public class HydraHttpClient implements Closeable {

    private final Client client = ClientBuilder.newClient();
    private final WebTarget root = client.target("http://localhost:4445");

    public HydraClientInfo createClient(String clientId, String clientSecret, String audience) {
        final HydraClientInfo clientInfo = new HydraClientInfo();
        clientInfo.setId(clientId);
        clientInfo.setSecret(clientSecret);
        clientInfo.setAudience(new String[]{ audience });

        try {
            root.path("/clients").request(MediaType.APPLICATION_JSON)
                    .post(Entity.entity(clientInfo.toString(), MediaType.APPLICATION_JSON), String.class);
        } catch (ClientErrorException e) {
            if (!e.getMessage().contains("409")) { // ignore conflict
                throw e;
            }
        }
        return clientInfo;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
