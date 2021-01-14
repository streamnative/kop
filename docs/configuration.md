# Configuration

KoP shares the same configuration files with Pulsar broker, e.g. `conf/broker.conf` or `conf/standalone.conf`. The log configurations can be configured in `conf/log4j2.yaml` file like:

```yaml
Logger:
    - name: io.streamnative.pulsar.handlers.kop
    level: warn
    additivity: false
    AppenderRef:
        - ref: Console
```

The following table lists all KoP configurations.

|Name|Description|Default|
|---|---|---|
|messagingProtocols|  Messaging protocols available for being loaded by Pulsar Broker |kafka|
|kafkaListeners|Comma-separated list of URIs we will listen on and the listener names.<br>e.g. PLAINTEXT://localhost:9092,SSL://localhost:9093.<br>If hostname is not set, bind to the default interface.||
|kafkaAdvertisedListeners|Listeners to publish to ZooKeeper for clients to use.<br>The format is the same as `kafkaListeners`.||
|listeners|Deprecated - use `kafkaListeners` instead. ||
|kafkaTenant| The default tenant of Kafka's topics |public|
|kafkaNamespace| The default namespace of Kafka's topics |default|
|kafkaMetadataTenant| Tenant used for storing Kafka metadata topics |public|
|kafkaMetadataNamespace| Namespace used for storing Kafka metadata topics  |__kafka|
|maxReadEntriesNum| Maximum number of entries that are read from cursor once per time  |5|
|enableGroupCoordinator|  Flag used to enable the group coordinator  |true|
|groupMinSessionTimeoutMs| The minimum allowed session timeout for registered consumers<br>Shorter timeouts result in quicker failure detection while require more frequent consumer heart beating, which can overwhelm broker resources.  |6000|
|groupMaxSessionTimeoutMs| The maximum allowed session timeout for registered consumers. <br>Longer timeouts give consumers more time to process messages between heartbeats while require longer time to detect failures. |300000|
|groupInitialRebalanceDelayMs| The time the group coordinator waits for more consumers to join a new group before performing the first rebalance <br> A longer delay potentially reduces rebalances, but increases the time until processing begins.  |3000|
|offsetsTopicCompressionCodec| Compression codec for the offsets topic <br>compression may be used to achieve "atomic" commits  |N/A|
|offsetMetadataMaxSize| The maximum size in bytes for a metadata entry associated with an offset commit  |4096|
|offsetsRetentionMinutes| Offsets older than this retention period are discarded |10080|
|offsetsRetentionCheckIntervalMs| Frequency at which to check for stale offsets  |600000|
|offsetsTopicNumPartitions| Number of partitions for the offsets topic  |8|
|kopSslProtocol| Kafka SSL configuration map with: SSL_PROTOCOL_CONFIG = ssl.protocol |TLS|
|kopSslProvider| Kafka SSL configuration map with: SSL_PROVIDER_CONFIG = ssl.provider | N/A |
|kopSslCipherSuites| Kafka SSL configuration map with: SSL_CIPHER_SUITES_CONFIG = ssl.cipher.suites|  N/A |
|kopSslEnabledProtocols| Kafka SSL configuration map with: SSL_ENABLED_PROTOCOLS_CONFIG = ssl.enabled.protocols| TLSv1.2, TLSv1.1, TLSv1 |
|kopSslKeystoreType| Kafka SSL configuration map with: SSL_KEYSTORE_TYPE_CONFIG = ssl.keystore.type |JKS|
|kopSslKeystoreLocation| Kafka SSL configuration map with: SSL_KEYSTORE_LOCATION_CONFIG = ssl.keystore.location  |N/A |
|kopSslKeystorePassword| Kafka SSL configuration map with: SSL_TRUSTSTORE_PASSWORD_CONFIG = ssl.truststore.password  |N/A |
|kopSslTruststoreType| Kafka SSL configuration map with: SSL_KEYSTORE_TYPE_CONFIG = ssl.keystore.type |JKS|
|kopSslTruststoreLocation| Kafka SSL configuration map with: SSL_TRUSTSTORE_LOCATION_CONFIG = ssl.truststore.location | N/A |
|kopSslTruststorePassword| Kafka SSL configuration map with: SSL_TRUSTSTORE_PASSWORD_CONFIG = ssl.truststore.password |N/A |
|kopSslKeymanagerAlgorithm|Kafka SSL configuration map with: SSL_KEYMANAGER_ALGORITHM_CONFIG = ssl.keymanager.algorithm |SunX509|
|kopSslTrustmanagerAlgorithm| Kafka SSL configuration map with: SSL_TRUSTMANAGER_ALGORITHM_CONFIG = ssl.trustmanager.algorithm |SunX509|
|kopSslSecureRandomImplementation| Kafka SSL configuration map with: SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG = ssl.secure.random.implementation  | N/A|
|saslAllowedMechanisms| Supported SASL mechanisms exposed by the broker |N/A|

