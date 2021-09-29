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
package io.streamnative.pulsar.handlers.kop.security.auth;

import io.streamnative.pulsar.handlers.kop.security.KafkaPrincipal;
import java.util.concurrent.CompletableFuture;

/**
 * Interface of authorizer.
 */
public interface Authorizer {

    /**
     * Check whether the specified role can perform a lookup for the specified topic.
     *
     * For that the caller needs to have producer or consumer permission.
     *
     * @param principal login info
     * @param resource resources to be authorized
     * @return a boolean to determine whether authorized or not
     */
    CompletableFuture<Boolean> canLookupAsync(KafkaPrincipal principal, Resource resource);

    /**
     * Check whether the specified role can access a Pulsar Tenant.
     *
     * @param principal login info
     * @param resource resources to be authorized
     * @return a boolean to determine whether authorized or not
     */
    CompletableFuture<Boolean> canAccessTenantAsync(KafkaPrincipal principal, Resource resource);

    /**
     * Check whether the specified role can manage Pulsar tenant.
     * This permission mapping to pulsar is Tenant Admin or Super Admin.
     *
     * @param principal login info
     * @param resource resources to be authorized
     * @return a boolean to determine whether authorized or not
     */
    CompletableFuture<Boolean> canManageTenantAsync(KafkaPrincipal principal, Resource resource);

    /**
     * Check whether the specified role can perform a produce for the specified topic.
     *
     * For that the caller needs to have producer permission.
     *
     * @param principal login info
     * @param resource resources to be authorized
     * @return a boolean to determine whether authorized or not
     */
    CompletableFuture<Boolean> canProduceAsync(KafkaPrincipal principal, Resource resource);

    /**
     * Check whether the specified role can perform a consumer for the specified topic.
     *
     * For that the caller needs to have consumer permission.
     *
     * @param principal login info
     * @param resource resources to be authorized
     * @return a boolean to determine whether authorized or not
     */
    CompletableFuture<Boolean> canConsumeAsync(KafkaPrincipal principal, Resource resource);

}
