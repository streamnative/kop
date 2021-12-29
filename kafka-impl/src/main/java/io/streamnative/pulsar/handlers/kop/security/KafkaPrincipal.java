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
package io.streamnative.pulsar.handlers.kop.security;


import java.security.Principal;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;


/**
 * Store client login info.
 */
@Getter
@ToString
@AllArgsConstructor
public class KafkaPrincipal implements Principal {

    public static final String USER_TYPE = "User";

    private final String principalType;

    /**
     * Pulsar role.
     */
    private final String name;

    /**
     * Pulsar Tenant Specs.
     * It can be "tenant" or "tenant/namespace"
     */
    private final String tenantSpec;

    private final AuthenticationDataSource authenticationData;
}