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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.kafka.common.record.CompressionType;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;

/**
 * Kafka on Pulsar service configuration object.
 */
@Getter
@Setter
public class KafkaServiceConfiguration extends ServiceConfiguration {

    public static final String TENANT_PLACEHOLDER = "${tenant}";
    public static final String TENANT_ALLNAMESPACES_PLACEHOLDER = "*";


    // Group coordinator configuration
    private static final int GroupMinSessionTimeoutMs = 6000;
    private static final int GroupMaxSessionTimeoutMs = 300000;
    private static final int GroupInitialRebalanceDelayMs = 3000;
    // offset configuration
    private static final int OffsetsRetentionMinutes = 3 * 24 * 60;
    private static final int DefaultSystemTopicRetentionSizeInMb = -1;
    public static final int DefaultOffsetsTopicNumPartitions = 50;
    private static final int OffsetsMessageTTL = 3 * 24 * 3600;
    // txn configuration
    public static final int DefaultTxnLogTopicNumPartitions = 50;
    public static final int DefaultTxnProducerStateLogTopicNumPartitions = 8;
    public static final int DefaultTxnCoordinatorSchedulerNum = 1;
    public static final int DefaultTxnStateManagerSchedulerNum = 1;
    public static final long DefaultAbortTimedOutTransactionsIntervalMs = TimeUnit.SECONDS.toMillis(10);
    public static final long DefaultRemoveExpiredTransactionalIdsIntervalMs = TimeUnit.HOURS.toMillis(1);
    public static final long DefaultTransactionalIdExpirationMs = TimeUnit.DAYS.toMillis(7);

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
            doc = "The number of threads used to respond to the response."
    )
    private int numSendKafkaResponseThreads = 4;

    @FieldContext(
            required = true,
            doc = "Manage automatically system namespaces and topic"
    )
    private boolean kafkaManageSystemNamespaces = true;

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
            doc = "Use the current tenant as namespace name for Metadata topics."
    )
    private boolean kafkaEnableMultiTenantMetadata = true;

    @FieldContext(
        category = CATEGORY_KOP,
        required = true,
        doc = "Use to enable/disable Kafka authorization force groupId check."
    )
    private boolean kafkaEnableAuthorizationForceGroupIdCheck = false;

    @FieldContext(
        category = CATEGORY_KOP,
        required = true,
        doc = "The namespace used for storing Kafka metadata topics"
    )
    private String kafkaMetadataNamespace = "__kafka";

    @FieldContext(
            category = CATEGORY_KOP,
            required = true,
            doc = "The namespace used for storing Kafka Schema Registry"
    )
    private String kopSchemaRegistryNamespace = "__kafka_schemaregistry";

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
            doc = "System topic retention size in mb"
    )
    private int systemTopicRetentionSizeInMB = DefaultSystemTopicRetentionSizeInMb;

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "Offsets message ttl in seconds. default is 259200."
    )
    private int offsetsMessageTTL = OffsetsMessageTTL;

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "Frequency at which to check for stale offsets"
    )
    private long offsetsRetentionCheckIntervalMs = OffsetConfig.DefaultOffsetsRetentionCheckIntervalMs;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "Offset commit will be delayed until the offset metadata be persisted or this timeout is reached"
    )
    private int offsetCommitTimeoutMs = 5000;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "send queue size of system client to produce system topic."
    )
    private int kafkaMetaMaxPendingMessages = 10000;

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
                + "Each URI's scheme represents a listener name if `kafkaProtocolMap` is configured.\n"
                + "Otherwise, the scheme must be a valid protocol in [PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL].\n"
                + "If hostname is not set, bind to the default interface."
    )
    private String kafkaListeners;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "Comma-separated map of listener name and protocol.\n"
                    + "e.g. PRIVATE:PLAINTEXT,PRIVATE_SSL:SSL,PUBLIC:PLAINTEXT,PUBLIC_SSL:SSL.\n"
    )
    private String kafkaProtocolMap;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "Listeners to publish to ZooKeeper for clients to use.\n"
                    + "The format is the same as `kafkaListeners`.\n"
    )
    private String kafkaAdvertisedListeners;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "limit the queue size for request, \n"
                + "like queued.max.requests in kafka.\n"
    )
    private int maxQueuedRequests = 500;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "The largest record batch size allowed by Kop, \n"
                    + "like max.message.bytes in kafka.\n"
    )
    private int maxMessageSize = 5 * 1024 * 1024;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "limit the timeout for request, \n"
                + "like request.timeout.ms in kafka\n"
    )
    private int requestTimeoutMs = 30000;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "Idle connections timeout: the server handler close the connections that idle more than this, \n"
                    + "like connections.max.idle.ms in kafka server."
    )
    private long connectionMaxIdleMs = 10 * 60 * 1000L;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "Connection close delay on failed authentication: "
                    + "this is the time (in milliseconds) by which connection close "
                    + "will be delayed on authentication failure. "
    )
    private int failedAuthenticationDelayMs = 300;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "The timeout for broker lookups (in milliseconds)"
    )
    private int brokerLookupTimeoutMs = 30_000;

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
            doc = "Use TLS while connecting to other brokers"
    )
    private boolean kopTlsEnabledWithBroker = false;

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
            doc = "Maximum number of entries that are read from cursor once per time, default is 5"
    )
    private int maxReadEntriesNum = 5;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "The format of an entry. Default: pulsar. Optional: [pulsar, kafka, mixed_kafka]"
    )
    private String entryFormat = "pulsar";

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "The broker id, default is 1"
    )
    private int kafkaBrokerId = 1;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "Store producer id sequence on a Pulsar topic"
    )
    private boolean kafkaTransactionProducerIdsStoredOnPulsar = false;

    @FieldContext(
            category = CATEGORY_KOP_TRANSACTION,
            doc = "Flag to enable transaction coordinator"
    )
    private boolean kafkaTransactionCoordinatorEnabled = false;

    @FieldContext(
            category = CATEGORY_KOP_TRANSACTION,
            doc = "Number of partitions for the transaction log topic"
    )
    private int kafkaTxnLogTopicNumPartitions = DefaultTxnLogTopicNumPartitions;

    @FieldContext(
            category = CATEGORY_KOP_TRANSACTION,
            doc = "Number of partitions for the transaction producer state topic"
    )
    private int kafkaTxnProducerStateTopicNumPartitions = DefaultTxnProducerStateLogTopicNumPartitions;

    @FieldContext(
            category = CATEGORY_KOP_TRANSACTION,
            doc = "Interval for taking snapshots of the status of pending transactions"
    )
    private int kafkaTxnProducerStateTopicSnapshotIntervalSeconds = 300;

    @FieldContext(
            category = CATEGORY_KOP_TRANSACTION,
            doc = "Number of threads dedicated to transaction recovery"
    )
    private int kafkaTransactionRecoveryNumThreads = 8;

    @FieldContext(
            category = CATEGORY_KOP_TRANSACTION,
            doc = "Interval for purging aborted transactions from memory (requires reads from storage)"
    )
    private int kafkaTxnPurgeAbortedTxnIntervalSeconds = 60 * 60;

    @FieldContext(
            category = CATEGORY_KOP_TRANSACTION,
            doc = "The interval in milliseconds at which to rollback transactions that have timed out."
    )
    private long kafkaTxnAbortTimedOutTransactionCleanupIntervalMs = DefaultAbortTimedOutTransactionsIntervalMs;

    @FieldContext(
            category = CATEGORY_KOP_TRANSACTION,
            doc = "Whether to enable transactional ID expiration."
    )
    private boolean kafkaTransactionalIdExpirationEnable = true;

    @FieldContext(
            category = CATEGORY_KOP_TRANSACTION,
            doc = "The time (in ms) that the transaction coordinator waits without receiving any transaction status"
                    + " updates for the current transaction before expiring its transactional ID."
    )
    private long kafkaTransactionalIdExpirationMs = DefaultTransactionalIdExpirationMs;

    @FieldContext(
            category = CATEGORY_KOP_TRANSACTION,
            doc = "The interval (in ms) at which to remove expired transactions."
    )
    private long kafkaTransactionsRemoveExpiredTransactionalIdCleanupIntervalMs =
            DefaultRemoveExpiredTransactionalIdsIntervalMs;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "The fully qualified name of a SASL server callback handler class that implements the "
                    + "AuthenticateCallbackHandler interface, which is used for OAuth2 authentication. "
                    + "If it's not set, the class will be Kafka's default server callback handler for "
                    + "OAUTHBEARER mechanism: KopOAuthBearerUnsecuredValidatorCallbackHandler."
    )
    private String kopOauth2AuthenticateCallbackHandler;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "The properties configuration file of OAuth2 authentication."
    )
    private String kopOauth2ConfigFile;

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "KOP Prometheus stats rollover latency"
    )
    private int kopPrometheusStatsLatencyRolloverSeconds = 60;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "KOP Enable the group level consumer metrics. (Default: false)"
    )
    private boolean kopEnableGroupLevelConsumerMetrics = false;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "The allowed namespaces to list topics with a comma separator.\n"
                    + " For example, \"public/default,public/kafka\".\n"
                    + "If it's not set or empty, the allowed namespaces will be \"<kafkaTenant>/<kafkaNamespace>\"."
    )
    private Set<String> kopAllowedNamespaces;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "Whether to enable the Schema Registry."
    )
    private boolean kopSchemaRegistryEnable = false;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "The name of the topic used by the Schema Registry."
    )
    private String kopSchemaRegistryTopicName = "__schema-registry";

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "The Schema Registry port."
    )
    private int kopSchemaRegistryPort = 8001;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "Start the Migration service."
    )
    private boolean kopMigrationEnable = false;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "Migration service port."
    )
    private int kopMigrationServicePort = 8002;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "KOP server compression type. Only used for entryFormat=mixed_kafka. If it's not set to none, "
                    + "the client messages will be used compression type which configured in here.\n"
                    + "The supported compression types are: [\"none\", \"gzip\", \"snappy\", \"lz4\"]"
    )
    private String kafkaCompressionType = "none";

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "Whether to skip messages without the index (Kafka offset).\n"
                    + "It should be enabled if KoP is upgraded from version lower than 2.8.0 to 2.8.0 or higher\n"
                    + "After that, the old messages without index will be skipped."
    )
    private boolean skipMessagesWithoutIndex = false;

    private String checkAdvertisedListeners(String advertisedListeners) {
        StringBuilder listenersReBuilder = new StringBuilder();
        for (String listener : advertisedListeners.split(EndPoint.END_POINT_SEPARATOR)) {
            AdvertisedListener advertisedListener = AdvertisedListener.create(listener);
            String hostname = advertisedListener.getHostname();
            if (hostname.equals("advertisedAddress")) {
                hostname = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(getAdvertisedAddress());
                listenersReBuilder.append(advertisedListener.getListenerName())
                        .append("://")
                        .append(hostname)
                        .append(":")
                        .append(advertisedListener.getPort());
            } else {
                listenersReBuilder.append(advertisedListener.getListenerName())
                        .append("://")
                        .append(advertisedListener.getHostname())
                        .append(":")
                        .append(advertisedListener.getPort());
            }
            listenersReBuilder.append(EndPoint.END_POINT_SEPARATOR);
        }
        return listenersReBuilder.deleteCharAt(
                listenersReBuilder.lastIndexOf(EndPoint.END_POINT_SEPARATOR)).toString();
    }

    public @NonNull String getKafkaAdvertisedListeners() {
        String advertisedListeners = getListeners();

        if (kafkaAdvertisedListeners != null) {
            advertisedListeners = kafkaAdvertisedListeners;
        }

        if (advertisedListeners == null) {
            throw new IllegalStateException("listeners or kafkaListeners is required");
        }

        return checkAdvertisedListeners(advertisedListeners);
    }

    public String getListeners() {
        return (kafkaListeners != null) ? kafkaListeners : listeners;
    }

    public @NonNull Properties getKopOauth2Properties() {
        final Properties props = new Properties();
        if (kopOauth2ConfigFile == null) {
            return props;
        }
        try (InputStream inputStream = new FileInputStream(kopOauth2ConfigFile)) {
            props.load(inputStream);
        } catch (FileNotFoundException e) {
            return props;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return props;
    }

    public @NonNull Set<String> getKopAllowedNamespaces() {
        if (kopAllowedNamespaces == null || kopAllowedNamespaces.isEmpty()) {
            return Collections.singleton(TENANT_PLACEHOLDER + "/" + getKafkaNamespace());
        }
        return kopAllowedNamespaces;
    }

}
