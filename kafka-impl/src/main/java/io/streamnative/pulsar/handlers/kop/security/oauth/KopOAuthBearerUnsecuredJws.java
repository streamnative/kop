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

import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerIllegalTokenException;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredJws;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;

/**
 * A simple unsecured JWS implementation.
 * The '{@code nbf}' claim is ignored if it is given because the related logic is not required for Kafka testing and
 * development purposes.
 *
 * @see <a href="https://tools.ietf.org/html/rfc7515">RFC 7515</a>
 */
public class KopOAuthBearerUnsecuredJws extends OAuthBearerUnsecuredJws implements KopOAuthBearerToken {

    private final AuthenticationDataCommand authData;
    /**
     * Constructor with the given principal and scope claim names.
     *
     * @param compactSerialization the compact serialization to parse as an unsecured JWS
     * @param principalClaimName   the required principal claim name
     * @param scopeClaimName       the required scope claim name
     * @throws OAuthBearerIllegalTokenException if the compact serialization is not a valid unsecured JWS
     *                                          (meaning it did not have 3 dot-separated Base64URL sections
     *                                          without an empty digital signature; or the header or claims
     *                                          either are not valid Base 64 URL encoded values or are not JSON
     *                                          after decoding; or the mandatory '{@code alg}' header value is
     *                                          not "{@code none}")
     */
    public KopOAuthBearerUnsecuredJws(String compactSerialization, String principalClaimName, String scopeClaimName)
            throws OAuthBearerIllegalTokenException {
        super(compactSerialization, principalClaimName, scopeClaimName);
        this.authData = new AuthenticationDataCommand(compactSerialization);
    }

    @Override
    public AuthenticationDataSource authDataSource() {
        return authData;
    }
}
