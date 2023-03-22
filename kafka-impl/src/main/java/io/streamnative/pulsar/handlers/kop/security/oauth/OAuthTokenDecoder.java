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

import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

public class OAuthTokenDecoder {

    protected static final String DELIMITER = "__with_tenant_";

    /**
     * Decode the raw token to token and tenant.
     *
     * @param rawToken Raw token, it contains token and tenant. Format: Tenant + "__with_tenant_" + Token.
     * @return Token and tenant pair. Left is token, right is tenant.
     */
    public static Pair<String, String> decode(@NonNull String rawToken) {
        final String token;
        final String tenant;
        // Extract real token.
        int idx = rawToken.indexOf(DELIMITER);
        if (idx != -1) {
            token = rawToken.substring(idx + DELIMITER.length());
            tenant = rawToken.substring(0, idx);
        } else {
            token = rawToken;
            tenant = null;
        }
        return Pair.of(token, tenant);
    }
}
