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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;

@JsonIgnoreProperties(ignoreUnknown = true)
public class OAuthBearerTokenImpl implements OAuthBearerToken {

    @JsonProperty("access_token")
    private String accessToken;

    @JsonProperty("expires_in")
    private int expiresIn;

    @JsonProperty("scope")
    private String scope;

    private final long startTimeMs = System.currentTimeMillis();

    @Override
    public String value() {
        return accessToken;
    }

    @Override
    public Set<String> scope() {
        return (scope != null)
                ? Arrays.stream(scope.split(" ")).collect(Collectors.toSet())
                : Collections.emptySet();
    }

    @Override
    public long lifetimeMs() {
        return startTimeMs + expiresIn * 1000L;
    }

    @Override
    public String principalName() {
        return "undefined";
    }

    @Override
    public Long startTimeMs() {
        return startTimeMs;
    }
}
