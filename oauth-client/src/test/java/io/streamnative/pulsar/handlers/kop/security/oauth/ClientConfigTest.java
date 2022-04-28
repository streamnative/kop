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

import java.util.HashMap;
import java.util.Map;
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
        final ClientConfig clientConfig = ClientConfigHelper.create(
                "https://issuer-url.com",
                "file:///etc/config/credentials.json",
                "audience"
        );
        Assert.assertEquals(clientConfig.getIssuerUrl().toString(), "https://issuer-url.com");
        Assert.assertEquals(clientConfig.getCredentialsUrl().toString(), "file:/etc/config/credentials.json");
        Assert.assertEquals(clientConfig.getAudience(), "audience");
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
            Assert.assertTrue(e.getMessage().startsWith("invalid " + ClientConfig.OAUTH_CREDENTIALS_URL + " \"xxx\""));
        }
    }
}
