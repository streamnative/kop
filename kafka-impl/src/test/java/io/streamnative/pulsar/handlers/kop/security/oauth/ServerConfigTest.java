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
 * Test ServerConfig.
 *
 * @see ServerConfig
 */
public class ServerConfigTest {

    @Test
    public void testValidConfig() {
        final Map<String, String> configs = new HashMap<>();
        configs.put(ServerConfig.OAUTH_VALIDATE_METHOD, "token");

        final ServerConfig serverConfig = new ServerConfig(configs);
        Assert.assertEquals(serverConfig.getValidateMethod(), "token");
    }

    @Test
    public void testGetDefaultValidateMethod() {
        ServerConfig serverConfig = new ServerConfig(new HashMap<>());
        Assert.assertEquals(ServerConfig.DEFAULT_OAUTH_VALIDATE_METHOD, serverConfig.getValidateMethod());
    }
}
