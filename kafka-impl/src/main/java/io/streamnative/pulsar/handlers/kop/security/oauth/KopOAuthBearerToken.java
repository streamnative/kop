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

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;

/**
 * Support pass the auth data to oauth server.
 */
@InterfaceStability.Evolving
public interface KopOAuthBearerToken extends OAuthBearerToken {

    /**
     * Pass the auth data to oauth server.
     */
    AuthenticationDataSource authDataSource();

    /**
     * Pass the tenant to oauth server if credentials set.
     */
    String tenant();
}

