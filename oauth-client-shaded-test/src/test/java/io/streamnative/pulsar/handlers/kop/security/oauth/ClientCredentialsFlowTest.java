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

import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClientCredentialsFlowTest {

    @Test
    public void testFindAuthorizationServer() throws IOException {
        final ClientCredentialsFlow flow = new ClientCredentialsFlow(ClientConfigHelper.create(
                "http://localhost:4444", // a local OAuth2 server started by init_hydra_oauth_server.sh
                Objects.requireNonNull(
                        getClass().getClassLoader().getResource("private_key.json")).toString()
        ));
        final ClientCredentialsFlow.Metadata metadata = flow.findAuthorizationServer();
        Assert.assertEquals(metadata.getTokenEndPoint(), "http://127.0.0.1:4444/oauth2/token");
    }

    @Test
    public void testLoadPrivateKey() {
        ClientConfig clientConfig = ClientConfigHelper.create(
                "http://localhost:4444",
                Objects.requireNonNull(
                        getClass().getClassLoader().getResource("private_key.json")).toString()
        );
        ClientInfo clientInfo = clientConfig.getClientInfo();
        Assert.assertEquals(clientInfo.getId(), "my-id");
        Assert.assertEquals(clientInfo.getSecret(), "my-secret");
        Assert.assertEquals(clientInfo.getTenant(), "my-tenant");
    }

    @Test
    public void testTenantToken() throws IOException {
        WireMockServer mockOauthServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
        try {
            mockOauthServer.start();
            final ClientCredentialsFlow flow = spy(new ClientCredentialsFlow(ClientConfigHelper.create(
                    mockOauthServer.url("/"),
                    Objects.requireNonNull(
                            getClass().getClassLoader().getResource("private_key.json")).toString()
            )));

            ClientCredentialsFlow.Metadata mockMetadata = mock(ClientCredentialsFlow.Metadata.class);
            doReturn(mockOauthServer.url("/mockTokenEndPoint")).when(mockMetadata).getTokenEndPoint();

            doReturn(mockMetadata).when(flow).findAuthorizationServer();

            String responseString = "{\n"
                    + "    \"access_token\":\"my-token\",\n"
                    + "    \"expires_in\":42,\n"
                    + "    \"scope\":\"test\"\n"
                    + "}";
            configureFor("localhost", mockOauthServer.port());
            stubFor(WireMock.post(urlPathEqualTo("/mockTokenEndPoint"))
                    .willReturn(WireMock.ok(responseString))
            );
            OAuthBearerTokenImpl token = flow.authenticate();
            Assert.assertEquals(token.value(), "my-tenant" + OAuthBearerTokenImpl.DELIMITER + "my-token");
            Assert.assertEquals(token.scope(), Collections.singleton("test"));
        } finally {
            mockOauthServer.shutdown();
        }

    }
}
