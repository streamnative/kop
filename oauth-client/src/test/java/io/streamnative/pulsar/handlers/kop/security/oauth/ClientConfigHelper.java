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
import javax.annotation.Nullable;

public class ClientConfigHelper {

    public static ClientConfig create(final String issuerUrl, final String credentialsUrl) {
        return create(issuerUrl, credentialsUrl, null);
    }

    public static ClientConfig create(final String issuerUrl,
                                      final String credentialsUrl,
                                      @Nullable final String audience) {
        final Map<String, String> map = new HashMap<>();
        map.put(ClientConfig.OAUTH_ISSUER_URL, issuerUrl);
        map.put(ClientConfig.OAUTH_CREDENTIALS_URL, credentialsUrl);
        if (audience != null) {
            map.put(ClientConfig.OAUTH_AUDIENCE, audience);
        }
        return new ClientConfig(map);
    }
}
