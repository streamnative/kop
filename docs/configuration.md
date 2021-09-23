# Configuration

## Listeners

| Name                     | Description                                                  |
| ------------------------ | ------------------------------------------------------------ |
| kafkaListeners           | Comma-separated list of URIs that we will listen on and the listener names.<br>e.g. PLAINTEXT://localhost:9092,SSL://localhost:9093.<br>If the hostname is not set, the default interface is used. |
| listeners                | Deprecated. `kafkaListeners` is used.                   |
| kafkaAdvertisedListeners | Listeners published to the ZooKeeper for clients to use.<br>The format is the same as `kafkaListeners`. |
| kafkaListenerName        | Specify the internal listener name for the broker.<br>The listener name must be contained in the advertisedListeners.<br>This config is used as the listener name in topic lookup. |

> **NOTE**
> 
> Among all configurations, only `kafkaListeners` or `listeners` (deprecated) is required.

## Logger

KoP shares the same configuration files with the Pulsar broker, e.g. `conf/broker.conf` or `conf/standalone.conf`. The log configurations can be configured in `conf/log4j2.yaml` file like below:

```yaml
Logger:
  - name: io.streamnative.pulsar.handlers.kop
    level: warn
    additivity: false
    AppenderRef:
      - ref: Console
```

## Namespace

Pulsar is a multi-tenant system that requires users to specify the [tenant and namespace](http://pulsar.apache.org/docs/en/concepts-multi-tenancy/). While, most Kafka users just specify the short topic name. So KoP provides following configurations to specify the default namespace.

| Name                   | Description                                      | Default |
| ---------------------- | ------------------------------------------------ | ------- |
| kafkaTenant            | The default tenant of Kafka topics             | public  |
| kafkaNamespace         | The default namespace of Kafka topics          | default |
| kafkaMetadataTenant    | The tenant used for storing Kafka metadata topics    | public  |
| kafkaEnableMultiTenantMetadata    | Use the SASL username as `kafkaMetadataTenant`  | true  |
| kafkaMetadataNamespace | The namespace used for storing Kafka metadata topics | __kafka |
| kopAllowedNamespaces   | The allowed namespace to list topics with a comma separator.<br>For example, "public/default,public/kafka".<br>If it's not set or empty, the allowed namespaces will be "<kafkaTenant>/<kafkaNamespace>". | |

When you enable `kafkaEnableMultiTenantMetadata`, KoP uses separate tenants for handling the system metadata.
This enables you to fully isolate your tenants in your Pulsar cluster.
This is not available in pure Kafka, because usually you share  the system metadata among all the users.

## Performance

This section lists configurations that may affect the performance.

| Name              | Description                                                  | Range             | Default |
| ----------------- | ------------------------------------------------------------ | ----------------- | ------- |
| entryFormat       | The format of an entry. Changing it to `pulsar` will avoid unnecessary encoding and decoding to improve the performance, whose cost is that a topic cannot be used by mixed Pulsar clients and Kafka clients. | kafka,<br> pulsar | kafka   |
| maxReadEntriesNum | The maximum number of entries that are read from the cursor once per time.<br>Increasing this value can make FETCH request read more bytes each time.<br>**NOTE**: Currently, KoP does not check the maximum byte limit. Therefore, if the value is too great, the response size may be over the network limit. |                   | 5       |

## Network

| Name              | Description                                                  | Default |
| ----------------- | ------------------------------------------------------------ | ------- |
| maxQueuedRequests | Limit the queue size for request, like `queued.max.requests` in Kafka server. | 500     |
| requestTimeoutMs  | Limit the timeout in milliseconds for request, like `request.timeout.ms` in Kafka client.<br>If a request was not processed in the timeout, KoP would return an error response to client. | 30000   |
| connectionMaxIdleMs | The idle connection timeout in milliseconds. If the idle connection timeout (such as `connections.max.idle.ms` used in the Kafka server) is reached, the server handler will close this idle connection.<br>**Note**: If it is set to `-1`, it indicates that the idle connection timeout is disabled. | 600000 |
| failedAuthenticationDelayMs | Connection close delay on failed authentication: this is the time (in milliseconds) by which connection close will be delayed on authentication failure, like `connection.failed.authentication.delay.ms` in Kafka server. | 300 |

> **NOTE**
> 
> These limits are based on each connection.

## Prometheus

| Name                                     | Description                                                  | Default |
| ---------------------------------------- | ------------------------------------------------------------ | ------- |
| kopPrometheusStatsLatencyRolloverSeconds | Kop metrics exposed to prometheus rollover latency in seconds. | 60      |

## Group Coordinator

This section lists configurations about the group coordinator and the `__consumer_offsets` topic that is used to store committed offsets.

|Name|Description|Default|
|---|---|---|
|groupMinSessionTimeoutMs| The minimum allowed session timeout for registered consumers. <br>Shorter timeouts result in quicker failure detection while require more frequent consumer heart beating, which can overwhelm broker resources.  |6000|
|groupMaxSessionTimeoutMs| The maximum allowed session timeout for registered consumers. <br>Longer timeouts give consumers more time to process messages between heartbeats while require longer time to detect failures. |300000|
|groupInitialRebalanceDelayMs| The time the group coordinator waits for more consumers to join a new group before performing the first rebalance. <br> A longer delay potentially reduces rebalances, but increases the time until processing begins.  |3000|
|offsetsTopicCompressionCodec| Compression codec for the offsets topic. <br>Compression may be used to achieve "atomic" commits.  ||
|offsetMetadataMaxSize| The maximum size in bytes for a metadata entry associated with an offset commit.  |4096|
|offsetsRetentionMinutes| Offsets older than this retention period are discarded. |4320|
|offsetsMessageTTL| The offsets message TTL in seconds. | 259200 |
|offsetsRetentionCheckIntervalMs| The frequency at which to check for stale offsets.  |600000|
|offsetsTopicNumPartitions| The number of partitions for the offsets topic.  |50|

## Transaction

This section lists configurations about the transaction.

| Name                         | Description                                        | Default |
| ---------------------------- | -------------------------------------------------- | ------- |
| enableTransactionCoordinator | Whether to enable transaction coordinator.          | false   |
| brokerId                     | The broker ID that is used to create the producer ID.  | 1       |
| txnLogTopicNumPartitions     | the number of partitions for the transaction log topic. | 50      |

## Authentication

This section lists configurations about the authentication.

| Name                                 | Description                                                  | Range                 | Default |
| ------------------------------------ | ------------------------------------------------------------ | --------------------- | ------- |
| saslAllowedMechanisms                | A set of supported SASL mechanisms exposed by the broker.     | PLAIN,<br>OAUTHBEARER |         |
| kopOauth2AuthenticateCallbackHandler | The fully qualified name of a SASL server callback handler class that implements the <br>AuthenticateCallbackHandler interface, which is used for OAuth2 authentication. <br>If it is not set, the class will be Kafka's default server callback handler for <br>OAUTHBEARER mechanism: OAuthBearerUnsecuredValidatorCallbackHandler. |                       |         |

## SSL encryption

|Name|Description|Default|
|---|---|---|
|kopSslProtocol| Kafka SSL configuration map with: SSL_PROTOCOL_CONFIG = ssl.protocol |TLS|
|kopSslProvider| Kafka SSL configuration map with: SSL_PROVIDER_CONFIG = ssl.provider |  |
|kopSslCipherSuites| Kafka SSL configuration map with: SSL_CIPHER_SUITES_CONFIG = ssl.cipher.suites|   |
|kopSslEnabledProtocols| Kafka SSL configuration map with: SSL_ENABLED_PROTOCOLS_CONFIG = ssl.enabled.protocols| TLSv1.2, TLSv1.1, TLSv1 |
|kopSslKeystoreType| Kafka SSL configuration map with: SSL_KEYSTORE_TYPE_CONFIG = ssl.keystore.type |JKS|
|kopSslKeystoreLocation| Kafka SSL configuration map with: SSL_KEYSTORE_LOCATION_CONFIG = ssl.keystore.location  | |
|kopSslKeystorePassword| Kafka SSL configuration map with: SSL_TRUSTSTORE_PASSWORD_CONFIG = ssl.truststore.password  |N/A |
|kopSslTruststoreType| Kafka SSL configuration map with: SSL_KEYSTORE_TYPE_CONFIG = ssl.keystore.type |JKS|
|kopSslTruststoreLocation| Kafka SSL configuration map with: SSL_TRUSTSTORE_LOCATION_CONFIG = ssl.truststore.location |  |
|kopSslTruststorePassword| Kafka SSL configuration map with: SSL_TRUSTSTORE_PASSWORD_CONFIG = ssl.truststore.password | |
|kopSslKeymanagerAlgorithm|Kafka SSL configuration map with: SSL_KEYMANAGER_ALGORITHM_CONFIG = ssl.keymanager.algorithm |SunX509|
|kopSslTrustmanagerAlgorithm| Kafka SSL configuration map with: SSL_TRUSTMANAGER_ALGORITHM_CONFIG = ssl.trustmanager.algorithm |SunX509|
|kopSslSecureRandomImplementation| Kafka SSL configuration map with: SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG = ssl.secure.random.implementation  |  |

