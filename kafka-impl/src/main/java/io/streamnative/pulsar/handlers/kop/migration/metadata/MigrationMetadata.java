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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Metadata about Kafka migration for a topic.
 */
@AllArgsConstructor
@Builder
@Data
public class MigrationMetadata {
    @VisibleForTesting
    static final String KAFKA_CLUSTER_ADDRESS = "migrationKafkaClusterAddress";
    @VisibleForTesting
    static final String TOPIC_MIGRATION_STATUS = "migrationTopicMigrationStatus";
    /**
     * Address of the Kafka cluster backing this topic.
     */
    private String kafkaClusterAddress;

    /**
     * Migration status of the topic.
     */
    private MigrationStatus migrationStatus;

    public Map<String, String> asProperties() {
        Map<String, String> props = new HashMap<>();
        if (kafkaClusterAddress != null) {
            props.put(KAFKA_CLUSTER_ADDRESS, kafkaClusterAddress);
        }
        if (migrationStatus != null) {
            props.put(TOPIC_MIGRATION_STATUS, migrationStatus.name());
        }
        return props;
    }

    public static MigrationMetadata fromProperties(Map<String, String> props) {
        String status = props.get(TOPIC_MIGRATION_STATUS);
        if (status == null) {
            return null;
        }

        String kafkaClusterAddress = props.get(KAFKA_CLUSTER_ADDRESS);
        if (kafkaClusterAddress == null) {
            return null;
        }

        return new MigrationMetadata(kafkaClusterAddress, MigrationStatus.valueOf(status));
    }
}
