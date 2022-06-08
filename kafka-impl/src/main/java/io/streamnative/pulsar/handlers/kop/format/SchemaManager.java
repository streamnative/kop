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
package io.streamnative.pulsar.handlers.kop.format;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;

public interface SchemaManager {

    interface KeyValueSchemaIds {
        int getKeySchemaId();
        int getValueSchemaId();
    }

    /**
     * Returns the IDs in the Kafka Schema Registry for the given version of the Pulsar schema.
     * @param topic
     * @param schemaVersion
     * @return
     */
    CompletableFuture<KeyValueSchemaIds> getSchemaIds(String topic, BytesSchemaVersion schemaVersion);
}
