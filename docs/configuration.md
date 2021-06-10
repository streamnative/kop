# Configuration

## Listeners

| Name                     | Description                                                  |
| ------------------------ | ------------------------------------------------------------ |
| kafkaListeners           | Comma-separated list of URIs we will listen on and the listener names.<br>e.g. PLAINTEXT://localhost:9092,SSL://localhost:9093.<br>If hostname is not set, bind to the default interface. |
| listeners                | Deprecated - use `kafkaListeners` instead.                   |
| kafkaAdvertisedListeners | Listeners to publish to ZooKeeper for clients to use.<br>The format is the same as `kafkaListeners`. |

> NOTE: among all configurations, only `kafkaListeners` or `listeners` (deprecated) is required.

## Logger

KoP shares the same configuration files with Pulsar broker, e.g. `conf/broker.conf` or `conf/standalone.conf`. The log configurations can be configured in `conf/log4j2.yaml` file like:

```yaml
Logger:
  - name: io.streamnative.pulsar.handlers.kop
    level: warn
    additivity: false
    AppenderRef:
      - ref: Console
```

## Namespace

Pulsar is a multi-tenant system that requires users to specify [tenant and namespace](http://pulsar.apache.org/docs/en/concepts-multi-tenancy/), while mostly Kafka users just specify the short topic name. So KoP provides following configurations to specify the default namespace.÷

| Name                   | Description                                      | Default |
| ---------------------- | ------------------------------------------------ | ------- |
| kafkaTenant            | The default tenant of Kafka's topics             | public  |
| kafkaNamespace         | The default namespace of Kafka's topics          | default |
| kafkaMetadataTenant    | Tenant used for storing Kafka metadata topics    | public  |
| kafkaMetadataNamespace | Namespace used for storing Kafka metadata topics | __kafka |

## Performance

This section lists configurations that may affect the performance.

| Name              | Description                                                  | Range             | Default |
| ----------------- | ------------------------------------------------------------ | ----------------- | ------- |
| entryFormat       | The format of an entry. Changing it to pulsar will avoid unnecessary encoding and decoding to improve the performance, whose cost is that a topic cannot be used by mixed Pulsar clients and Kafka clients. | kafka,<br> pulsar | kafka   |
| maxReadEntriesNum | Maximum number of entries that are read from cursor once per time.<br>Increasing this value can make FETCH request read more bytes each time.<br>NOTE: KoP doesn't check the max bytes limit currently so if the value is too large, the response size may be over the network limit. |                   | 5       |

## Network

| Name              | Description                                                  | Default |
| ----------------- | ------------------------------------------------------------ | ------- |
| maxQueuedRequests | Limit the queue size for request, like queued.max.requests in kafka server. | 500     |
| requestTimeoutMs  | Limit the timeout in milliseconds for request, like request.timeout.ms in kafka client.<br>If a request was not processed in the timeout, KoP would return an error response to client. | 30000   |

> NOTE: these limits are based on each connection.

## Prometheus

| Name                                     | Description                                                  | Default |
| ---------------------------------------- | ------------------------------------------------------------ | ------- |
| kopPrometheusStatsLatencyRolloverSeconds | Kop metrics expose to prometheus rollover latency in seconds. | 60      |

## Group Coordinator

This sections lists configurations about group coordinator and `__consumer_offsets` topic that is used to store committed offsets.

|Name|Description|Default|
|---|---|---|
|groupMinSessionTimeoutMs| The minimum allowed session timeout for registered consumers<br>Shorter timeouts result in quicker failure detection while require more frequent consumer heart beating, which can overwhelm broker resources.  |6000|
|groupMaxSessionTimeoutMs| The maximum allowed session timeout for registered consumers. <br>Longer timeouts give consumers more time to process messages between heartbeats while require longer time to detect failures. |300000|
|groupInitialRebalanceDelayMs| The time the group coordinator waits for more consumers to join a new group before performing the first rebalance <br> A longer delay potentially reduces rebalances, but increases the time until processing begins.  |3000|
|offsetsTopicCompressionCodec| Compression codec for the offsets topic <br>compression may be used to achieve "atomic" commits  ||
|offsetMetadataMaxSize| The maximum size in bytes for a metadata entry associated with an offset commit  |4096|
|offsetsRetentionMinutes| Offsets older than this retention period are discarded |4320|
|offsetsMessageTTL| Offsets message ttl in seconds. | 259200 |
|offsetsRetentionCheckIntervalMs| Frequency at which to check for stale offsets  |600000|
|offsetsTopicNumPartitions| Number of partitions for the offsets topic  |50|

## Transaction

| Name                         | Description                                        | Default |
| ---------------------------- | -------------------------------------------------- | ------- |
| enableTransactionCoordinator | Whether to enable transaction coordinator          | false   |
| brokerId                     | The broker id that is used to create producer id.  | 1       |
| txnLogTopicNumPartitions     | Number of partitions for the transaction log topic | 50      |

## Authentication

| Name                                 | Description                                                  | Range                 | Default |
| ------------------------------------ | ------------------------------------------------------------ | --------------------- | ------- |
| saslAllowedMechanisms                | A set of supported SASL mechanisms exposed by the broker     | PLAIN,<br>OAUTHBEARER |         |
| kopOauth2AuthenticateCallbackHandler | The fully qualified name of a SASL server callback handler class that implements the <br>AuthenticateCallbackHandler interface, which is used for OAuth2 authentication. <br>If it's not set, the class will be Kafka's default server callback handler for <br>OAUTHBEARER mechanism: OAuthBearerUnsecuredValidatorCallbackHandler. |                       |         |

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

