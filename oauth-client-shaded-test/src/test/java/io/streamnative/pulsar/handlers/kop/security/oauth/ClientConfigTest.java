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

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test ClientConfig.
 *
 * @see ClientConfig
 */
public class ClientConfigTest {

    @Test
    public void testValidConfig() {
        String credentialsUrl = Objects.requireNonNull(
                getClass().getClassLoader().getResource("private_key.json")).toString();
        final ClientConfig clientConfig = ClientConfigHelper.create(
                "https://issuer-url.com",
                credentialsUrl,
                "audience"
        );
        Assert.assertEquals(clientConfig.getIssuerUrl().toString(), "https://issuer-url.com");
        Assert.assertEquals(clientConfig.getCredentialsUrl().toString(), credentialsUrl);
        Assert.assertEquals(clientConfig.getAudience(), "audience");
        Assert.assertEquals(clientConfig.getClientInfo(),
                new ClientInfo("my-id", "my-secret", "my-tenant", null));
    }

    @Test
    public void testValidConfigWithGroupId() {
        String credentialsUrl = Objects.requireNonNull(
                getClass().getClassLoader().getResource("private_key_with_group_id.json")).toString();
        final ClientConfig clientConfig = ClientConfigHelper.create(
                "https://issuer-url.com",
                credentialsUrl,
                "audience"
        );
        Assert.assertEquals(clientConfig.getIssuerUrl().toString(), "https://issuer-url.com");
        Assert.assertEquals(clientConfig.getCredentialsUrl().toString(), credentialsUrl);
        Assert.assertEquals(clientConfig.getAudience(), "audience");
        Assert.assertEquals(clientConfig.getClientInfo(),
                new ClientInfo("my-id", "my-secret", "my-tenant", "my-group-id"));
    }

    @Test
    public void testRequiredConfigs() {
        final Map<String, String> configs = new HashMap<>();

        try {
            new ClientConfig(configs);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "no key for " + ClientConfig.OAUTH_ISSUER_URL);
        }

        configs.put(ClientConfig.OAUTH_ISSUER_URL, "https://issuer-url.com");
        try {
            new ClientConfig(configs);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "no key for " + ClientConfig.OAUTH_CREDENTIALS_URL);
        }
    }

    @Test
    public void testInvalidUrl() {
        try {
            ClientConfigHelper.create("xxx", "file:///tmp/key.json");
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().startsWith("invalid " + ClientConfig.OAUTH_ISSUER_URL + " \"xxx\""));
        }

        try {
            ClientConfigHelper.create("https://issuer-url.com", "xxx");
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().startsWith("failed to load client credentials from xxx"));
        }

        try {
            ClientConfigHelper.create("https://issuer-url.com", "data:application/json;base64,xxx");
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage()
                    .startsWith("failed to load client credentials from data:application/json;base64"));
        }

        try {
            ClientConfigHelper.create("https://issuer-url.com", "data:application/json;base64");
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage()
                    .startsWith("Unsupported media type or encoding format"));
        }
    }

    @Test
    public void testBase64Url() {
        String json = "{\n"
                + "  \"client_id\": \"my-id\",\n"
                + "  \"client_secret\": \"my-secret\",\n"
                + "  \"tenant\": \"my-tenant\"\n"
                + "}\n";

        String credentialsUrlData = "data:application/json;base64,"
                + new String(Base64.getEncoder().encode(json.getBytes()));

        final ClientConfig clientConfig = ClientConfigHelper.create(
                "https://issuer-url.com",
                credentialsUrlData,
                "audience"
        );
        Assert.assertEquals(clientConfig.getIssuerUrl().toString(), "https://issuer-url.com");
        Assert.assertEquals(clientConfig.getCredentialsUrl().toString(), credentialsUrlData);
        Assert.assertEquals(clientConfig.getAudience(), "audience");
        Assert.assertEquals(clientConfig.getClientInfo(),
                new ClientInfo("my-id", "my-secret", "my-tenant", null));
    }
}
