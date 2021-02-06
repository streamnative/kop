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
package io.streamnative.pulsar.handlers.kop;

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.coordinator.group.OffsetConfig;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import lombok.NonNull;
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
    public static final int DefaultOffsetsTopicNumPartitions = 8;

    @Category
    private static final String CATEGORY_KOP = "Kafka on Pulsar";

    @Category
    private static final String CATEGORY_KOP_SSL = "Kafka on Pulsar SSL configuration";

    @Category
    private static final String CATEGORY_KOP_TRANSACTION = "Kafka on Pulsar transaction";
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
        doc = "Flag to enable group coordinator"
    )
    private boolean enableGroupCoordinator = true;

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
        doc = "Number of partitions for the offsets topic"
    )
    private int offsetsTopicNumPartitions = DefaultOffsetsTopicNumPartitions;

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "The maximum size in Bytes for a metadata entry associated with an offset commit"
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

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "Zookeeper path for storing kop consumer group"
    )
    private String groupIdZooKeeperPath = "/client_group_id";

    @Deprecated
    @FieldContext(
        category = CATEGORY_KOP,
        doc = "Use `kafkaListeners` instead"
    )
    private String listeners;

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "Comma-separated list of URIs we will listen on and the listener names.\n"
                + "e.g. PLAINTEXT://localhost:9092,SSL://localhost:9093.\n"
                + "If hostname is not set, bind to the default interface."
    )
    private String kafkaListeners;

    public String getListeners() {
        return (kafkaListeners != null) ? kafkaListeners : listeners;
    }

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "Listeners to publish to ZooKeeper for clients to use.\n"
                + "The format is the same as `kafkaListeners`.\n"
    )
    private String kafkaAdvertisedListeners;

    public @NonNull String getKafkaAdvertisedListeners() {
        if (kafkaAdvertisedListeners != null) {
            return kafkaAdvertisedListeners;
        } else {
            if (getListeners() == null) {
                throw new IllegalStateException("listeners or kafkaListeners is required");
            }
            return getListeners();
        }
    }

    // Kafka SSL configs
    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: SSL_PROTOCOL_CONFIG = \"ssl.protocol\""
    )
    private String kopSslProtocol = "TLS";

    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: SSL_PROVIDER_CONFIG = \"ssl.provider\""
    )
    private String kopSslProvider;

    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: SSL_CIPHER_SUITES_CONFIG = \"ssl.cipher.suites\""
    )
    private Set<String> kopSslCipherSuites;

    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: SSL_ENABLED_PROTOCOLS_CONFIG = \"ssl.enabled.protocols\""
    )
    private Set<String> kopSslEnabledProtocols = Sets.newHashSet("TLSv1.2", "TLSv1.1", "TLSv1");

    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: SSL_KEYSTORE_TYPE_CONFIG = \"ssl.keystore.type\""
    )
    private String kopSslKeystoreType = "JKS";

    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: SSL_KEYSTORE_LOCATION_CONFIG = \"ssl.keystore.location\""
    )
    private String kopSslKeystoreLocation;

    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: SSL_KEYSTORE_PASSWORD_CONFIG = \"ssl.keystore.password\""
    )
    private String kopSslKeystorePassword;

    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: SSL_KEY_PASSWORD_CONFIG = \"ssl.key.password\""
    )
    private String kopSslKeyPassword;

    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: SSL_TRUSTSTORE_TYPE_CONFIG = \"ssl.truststore.type\""
    )
    private String kopSslTruststoreType = "JKS";

    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: SSL_TRUSTSTORE_LOCATION_CONFIG = \"ssl.truststore.location\""
    )
    private String kopSslTruststoreLocation;

    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: SSL_TRUSTSTORE_PASSWORD_CONFIG = \"ssl.truststore.password\""
    )
    private String kopSslTruststorePassword;

    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: SSL_KEYMANAGER_ALGORITHM_CONFIG = \"ssl.keymanager.algorithm\""
    )
    private String kopSslKeymanagerAlgorithm = "SunX509";

    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: SSL_TRUSTMANAGER_ALGORITHM_CONFIG = \"ssl.trustmanager.algorithm\""
    )
    private String kopSslTrustmanagerAlgorithm = "SunX509";

    @FieldContext(
        category = CATEGORY_KOP_SSL,
        doc = "Kafka ssl configuration map with: "
            + "SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG = \"ssl.secure.random.implementation\""
    )
    private String kopSslSecureRandomImplementation;

    @FieldContext(
            category = CATEGORY_KOP_SSL,
            doc = "Kafka ssl configuration map with: SSL_CLIENT_AUTH_CONFIG = \"ssl.client.auth\""
    )
    private String kopSslClientAuth;

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "supported SASL mechanisms exposed by broker"
    )
    private Set<String> saslAllowedMechanisms = new HashSet<String>();

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "Maximum number of entries that are read from cursor once per time, default is 1"
    )
    private int maxReadEntriesNum = 1;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "The format of an entry. Default: pulsar. Optional: [pulsar, kafka]"
    )
    private String entryFormat = "pulsar";

    @FieldContext(
            category = CATEGORY_KOP_TRANSACTION,
            doc = "Flag to enable transaction coordinator"
    )
    private boolean enableTransactionCoordinator = false;

}
