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
package io.streamnative.kop;

import io.streamnative.kop.coordinator.group.OffsetConfig;
import java.util.Optional;
import java.util.Properties;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.record.CompressionType;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;

/**
 * Kafka on Pulsar service configuration object.
 */
@Getter
@Setter
public class KafkaServiceConfiguration extends ServiceConfiguration {

    // Group coordinator configuration
    private static final int GroupMinSessionTimeoutMs = 6000;
    private static final int GroupMaxSessionTimeoutMs = 300000;
    private static final int GroupInitialRebalanceDelayMs = 3000;
    // offset configuration
    private static final int OffsetsRetentionMinutes = 7 * 24 * 60;
    public static final int DefaultOffsetsTopicNumPartitions = 1;

    private Properties properties = new Properties();

    @Category
    private static final String CATEGORY_KOP = "Kafka on Pulsar";

    //
    // --- Kafka on Pulsar Broker configuration ---
    //

    @FieldContext(
        category = CATEGORY_KOP,
        required = true,
        doc = "Kafka on Pulsar Broker tenant"
    )
    private String kafkaTenant = "public";

    @FieldContext(
        category = CATEGORY_KOP,
        required = true,
        doc = "Kafka on Pulsar Broker namespace"
    )
    private String kafkaNamespace = "default";

    @FieldContext(
        category = CATEGORY_KOP,
        required = true,
        doc = "The tenant used for storing Kafka metadata topics"
    )
    private String kafkaMetadataTenant = "public";

    @FieldContext(
        category = CATEGORY_KOP,
        required = true,
        doc = "The namespace used for storing Kafka metadata topics"
    )
    private String kafkaMetadataNamespace = "__kafka";

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "The port for serving Kafka requests"
    )

    private Optional<Integer> kafkaServicePort = Optional.of(9092);

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "The port for serving tls secured Kafka requests"
    )
    private Optional<Integer> kafkaServicePortTls = Optional.empty();

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "Flag to enable group coordinator"
    )
    private boolean enableGroupCoordinator = false;

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "The minimum allowed session timeout for registered consumers."
            + " Shorter timeouts result in quicker failure detection at the cost"
            + " of more frequent consumer heartbeating, which can overwhelm broker resources."
    )
    private int groupMinSessionTimeoutMs = GroupMinSessionTimeoutMs;

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "The maximum allowed session timeout for registered consumers."
            + " Longer timeouts give consumers more time to process messages in"
            + " between heartbeats at the cost of a longer time to detect failures."
    )
    private int groupMaxSessionTimeoutMs = GroupMaxSessionTimeoutMs;

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "The amount of time the group coordinator will wait for more consumers"
            + " to join a new group before performing  the first rebalance. A longer"
            + " delay means potentially fewer rebalances, but increases the time until"
            + " processing begins."
    )
    private int groupInitialRebalanceDelayMs = GroupInitialRebalanceDelayMs;

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "Compression codec for the offsets topic - compression may be used to achieve \\\"atomic\\\" commits"
    )
    private String offsetsTopicCompressionCodec = CompressionType.NONE.name();

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "The maximum size for a metadata entry associated with an offset commit"
    )
    private int offsetMetadataMaxSize = OffsetConfig.DefaultMaxMetadataSize;

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "Offsets older than this retention period will be discarded"
    )
    private long offsetsRetentionMinutes = OffsetsRetentionMinutes;

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "Frequency at which to check for stale offsets"
    )
    private long offsetsRetentionCheckIntervalMs = OffsetConfig.DefaultOffsetsRetentionCheckIntervalMs;
}
