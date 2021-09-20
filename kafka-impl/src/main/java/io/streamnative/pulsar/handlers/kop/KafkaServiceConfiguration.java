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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.kop.coordinator.group.OffsetConfig;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    private static final int OffsetsRetentionMinutes = 3 * 24 * 60;
    public static final int DefaultOffsetsTopicNumPartitions = 50;
    private static final int OffsetsMessageTTL = 3 * 24 * 3600;
    // txn configuration
    public static final int DefaultTxnLogTopicNumPartitions = 50;

    @Category
    private static final String CATEGORY_KOP = "Kafka on Pulsar";

    @Category
    private static final String CATEGORY_KOP_SSL = "Kafka on Pulsar SSL configuration";

    @Category
    private static final String CATEGORY_KOP_TRANSACTION = "Kafka on Pulsar transaction";

    private static final String END_POINT_SEPARATOR = ",";
    private static final String REGEX = "^(.*)://\\[?([0-9a-zA-Z\\-%._:]*)\\]?:(-?[0-9]+)";
    private static final Pattern PATTERN = Pattern.compile(REGEX);
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
            doc = "Use the current tenant as namespace name for Metadata topics."
    )
    private boolean kafkaEnableMultitenantMetadata = true;

    @FieldContext(
        category = CATEGORY_KOP,
        required = true,
        doc = "The namespace used for storing Kafka metadata topics"
    )
    private String kafkaMetadataNamespace = "__kafka";

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

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "Listeners to publish to ZooKeeper for clients to use.\n"
                + "The format is the same as `kafkaListeners`.\n"
    )
    private String kafkaAdvertisedListeners;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "Specify the internal listener name for the broker.\n"
                    + "The listener name must be contained in the advertisedListeners.\n"
                    + "This config is used as the listener name in topic lookup."
    )
    private String kafkaListenerName;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "limit the queue size for request, \n"
                + "like queued.max.requests in kafka.\n"
    )
    private int maxQueuedRequests = 500;

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
            doc = "Maximum number of entries that are read from cursor once per time, default is 5"
    )
    private int maxReadEntriesNum = 5;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "The format of an entry. Default: pulsar. Optional: [pulsar, kafka]"
    )
    private String entryFormat = "pulsar";

    @FieldContext(
        category = CATEGORY_KOP,
        doc = "The broker id, default is 1"
    )
    private int brokerId = 1;

    @FieldContext(
            category = CATEGORY_KOP_TRANSACTION,
            doc = "Flag to enable transaction coordinator"
    )
    private boolean enableTransactionCoordinator = false;

    @FieldContext(
            category = CATEGORY_KOP_TRANSACTION,
            doc = "Number of partitions for the transaction log topic"
    )
    private int txnLogTopicNumPartitions = DefaultTxnLogTopicNumPartitions;

    @FieldContext(
            category = CATEGORY_KOP,
            doc = "The fully qualified name of a SASL server callback handler class that implements the "
                    + "AuthenticateCallbackHandler interface, which is used for OAuth2 authentication. "
                    + "If it's not set, the class will be Kafka's default server callback handler for "
                    + "OAUTHBEARER mechanism: OAuthBearerUnsecuredValidatorCallbackHandler."
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
            doc = "The allowed namespaces to list topics with a comma separator.\n"
                    + " For example, \"public/default,public/kafka\".\n"
                    + "If it's not set or empty, the allowed namespaces will be \"<kafkaTenant>/<kafkaNamespace>\"."
    )
    private Set<String> kopAllowedNamespaces;

    private String checkAdvertisedListeners(String advertisedListeners) {
        StringBuilder listenersReBuilder = new StringBuilder();
        for (String listener : advertisedListeners.split(END_POINT_SEPARATOR)) {
            final String errorMessage = "listener '" + listener + "' is invalid";
            final Matcher matcher = PATTERN.matcher(listener);
            checkState(matcher.find(), errorMessage);
            checkState(matcher.groupCount() == 3, errorMessage);
            String hostname = matcher.group(2);
            if (hostname.isEmpty()) {
                try {
                    hostname = InetAddress.getLocalHost().getCanonicalHostName();
                    listenersReBuilder.append(matcher.group(1))
                            .append("://")
                            .append(hostname)
                            .append(":")
                            .append(matcher.group(3));
                } catch (UnknownHostException e) {
                    throw new IllegalStateException("hostname is empty and localhost is unknown: " + e.getMessage());
                }
            } else {
                listenersReBuilder.append(listener);
            }
            listenersReBuilder.append(END_POINT_SEPARATOR);
        }
        return listenersReBuilder.deleteCharAt(listenersReBuilder.lastIndexOf(END_POINT_SEPARATOR)).toString();
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
            return Collections.singleton(getKafkaTenant() + "/" + getKafkaNamespace());
        }
        return kopAllowedNamespaces;
    }
}
