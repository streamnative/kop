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
package io.streamnative.pulsar.handlers.kop.migration.metadata;

import static io.streamnative.pulsar.handlers.kop.migration.metadata.MigrationMetadata.KAFKA_CLUSTER_ADDRESS;
import static io.streamnative.pulsar.handlers.kop.migration.metadata.MigrationMetadata.TOPIC_MIGRATION_STATUS;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.testng.annotations.Test;

public class MigrationMetadataTest {

    @Test
    public void testAsProperties() {
        MigrationMetadata migrationMetadata =
                MigrationMetadata.builder().migrationStatus(MigrationStatus.DONE).kafkaClusterAddress("localhost")
                        .build();
        Map<String, String> properties =
                ImmutableMap.of(TOPIC_MIGRATION_STATUS, MigrationStatus.DONE.name(), KAFKA_CLUSTER_ADDRESS,
                        "localhost");
        assertEquals(properties, migrationMetadata.asProperties());
    }

    @Test
    public void testFromProperties() {
        Map<String, String> properties =
                ImmutableMap.of(TOPIC_MIGRATION_STATUS, MigrationStatus.DONE.name(), KAFKA_CLUSTER_ADDRESS,
                        "localhost");
        assertEquals(MigrationMetadata.fromProperties(properties),
                MigrationMetadata.builder().migrationStatus(MigrationStatus.DONE).kafkaClusterAddress("localhost")
                        .build());
    }
}
