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
package io.streamnative.pulsar.handlers.kop.security.oauth;

import java.io.IOException;
import java.util.Objects;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClientCredentialsFlowTest {

    @Test
    public void testFindAuthorizationServer() throws IOException {
        final ClientCredentialsFlow flow = new ClientCredentialsFlow(ClientConfigHelper.create(
                "http://localhost:4444", // a local OAuth2 server started by init_hydra_oauth_server.sh
                "file:///tmp/not_exist.json"
        ));
        final ClientCredentialsFlow.Metadata metadata = flow.findAuthorizationServer();
        Assert.assertEquals(metadata.getTokenEndPoint(), "http://127.0.0.1:4444/oauth2/token");
    }

    @Test
    public void testLoadPrivateKey() throws Exception {
        final ClientCredentialsFlow flow = new ClientCredentialsFlow(ClientConfigHelper.create(
                "http://localhost:4444",
                Objects.requireNonNull(
                        getClass().getClassLoader().getResource("private_key.json")).toString()
        ));
        final ClientCredentialsFlow.ClientInfo clientInfo = flow.loadPrivateKey();
        Assert.assertEquals(clientInfo.getId(), "my-id");
        Assert.assertEquals(clientInfo.getSecret(), "my-secret");
    }
}
