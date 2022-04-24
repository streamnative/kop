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

import java.util.Map;
import lombok.Getter;

/**
 * The server configs associated with OauthValidatorCallbackHandler.
 *
 * @see OauthValidatorCallbackHandler
 */
@Getter
public class ServerConfig {

    public static final String DEFAULT_OAUTH_VALIDATE_METHOD = "token";

    public static final String OAUTH_VALIDATE_METHOD = "oauth.validate.method";

    private final String validateMethod;

    public ServerConfig(Map<String, String> configs) {
        this.validateMethod = configs.getOrDefault(OAUTH_VALIDATE_METHOD, DEFAULT_OAUTH_VALIDATE_METHOD);
    }
}
