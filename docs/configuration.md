# Configuration

## Listeners

| Name                     | Description                                                  |
| ------------------------ | ------------------------------------------------------------ |
| kafkaListeners           | Comma-separated list of URIs that we will listen on and the listener names.<br>e.g. PLAINTEXT://localhost:9092,SSL://localhost:9093.<br>Each URI's scheme represents a listener name if `kafkaProtocolMap` is configured.<br>Otherwise, the scheme must be a valid protocol in [PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL].<br>If the hostname is not set, it will be bound to the default interface. |
| kafkaProtocolMap         | Comma-separated map of listener name and protocol.<br>e.g. PRIVATE:PLAINTEXT,PRIVATE_SSL:SSL,PUBLIC:PLAINTEXT,PUBLIC_SSL:SSL. |
| listeners                | Deprecated. `kafkaListeners` is used.                        |
| kafkaAdvertisedListeners | Listeners to publish to ZooKeeper for clients to use.<br>The format is the same as `kafkaListeners`. |

> **NOTE**
> 
> Among all configurations, only `kafkaListeners` or `listeners` (deprecated) is required.

To support multiple listeners, you need to specify different listener names in `kafkaListeners` and `kafkaAdvertisedListeners`. Then, map the listener name to the proper protocol in `kafkaProtocolMap`.

For example, assuming you need to listen on port 9092 and 19092 with the `PLAINTEXT` protocol, the associated names are `kafka_internal` and `kafka_external`. Then you need to add the following configurations:

```properties
kafkaListeners=kafka_internal://0.0.0.0:9092,kafka_external://0.0.0.0:19092
kafkaProtocolMap=kafka_internal:PLAINTEXT,kafka_external:PLAINTEXT
kafkaAdvertisedListeners=kafka_internal://localhost:9092,kafka_external://localhost:19092
```

In the above example,
- `kafkaListener` is split into multiple tokens by a comma (`,`), the token is in a format of `<listener-name>://<host>:<port>`.
- `kafkaProtocolMap` is split into multiple tokens by a comma (`,`), the token is in a format of `<listener-name>:<protocol>`.
- `kafkaAdvertisedListeners` is split into multiple tokens by a comma(`,`), the token is in a format of `<listener-name>:<scheme>://<host>:<port>`.


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
| kopAllowedNamespaces   | The allowed namespace to list topics with a comma separator.<br>For example, `kopAllowedNamespaces=public/default,public/kafka`.<br>If it is not configured or is empty, the allowed namespaces will get values from `kafkaTenant` and `kafkaNamespace`, which is `<kafkaTenant>/<kafkaNamespace>`. | |

When you enable `kafkaEnableMultiTenantMetadata`, KoP uses separate tenants for handling the system metadata.
This enables you to fully isolate your tenants in your Pulsar cluster.
This is not available in pure Kafka, because usually you share  the system metadata among all the users.

## Performance

This section lists configurations that may affect the performance.

| Name              | Description                                                  | Range             | Default |
| ----------------- | ------------------------------------------------------------ | ----------------- | ------- |
| entryFormat       | The format of an entry. If it is set to`kafka`, there is no unnecessary encoding and decoding work, which helps improve the performance. However, in this situation, a topic cannot be used by mixed Pulsar clients and Kafka clients. If it is set to `mixed_kafka`, some non-official Kafka clients implementation are supported. <br>- **Note**: Compared with performance for `mixed_kafka`, performance is improved by 2 to 3 times when the parameter is set to `kafka`. | kafka, <br> mixed_kafka,<br> pulsar | pulsar   |
| maxReadEntriesNum | The maximum number of entries that are read from the cursor once per time.<br>Increasing this value can make FETCH request read more bytes each time.<br>**NOTE**: Currently, KoP does not check the maximum byte limit. Therefore, if the value is too great, the response size may be over the network limit. |                   | 5       |

### Choose the proper `entryFormat`

This table lists `entryFormat` values that are supported in KoP.

| Name        | Description                                                                                                                                                                                                                                                                                                 |
|-------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| pulsar      | `pulsar` is the default `entryFormat` in KoP. It is used to encode or decode formats between the Kafka message and the Pulsar message. Therefore, the performance is the worst. The benefit is that both the Kafka client and the Pulsar client consumers can consume the messages from the Pulsar cluster. |
| kafka       | When you set the `entryFormat` option to `kafka`, KoP does not encode or decode Kafka messages. The messages will be directly stored in the bookie cluster in entries format, and the Pulsar client can not parse these messages. Therefore, the performance is the best.                                   |
| mixed_kafka | The `mixed_kafka` format works similarly to the `kafka` format.  You can set this option for some non-official Kafka clients for encoding or decoding Kafka messages. The performance is medium.                                                                                                            |

You can run the `io.streamnative.pulsar.handlers.kop.format.EncodePerformanceTest.java` to get the performance result among the above formats.

Generally, if you don't have Pulsar consumers that consume messages from Kafka producers, `kafka` format is perferred because **it has much higher performance** when Kafka consumers interact with Kafka producers.

However, some non-official Kafka clients might not work for `kafka` format. For example, old [Golang Sarama](https://github.com/Shopify/sarama) client didn't assign relative offsets in compressed message sets before [Shopify/sarama #1002](https://github.com/Shopify/sarama/pull/1002). In this case, the broker has to assign relative offsets and then do recompression. Since this behavior leads to some performance loss, KoP adds the `mixed_kafka` format to perform the conversion. The `mixed_kafka` format should be chosen when you have such an old Kafka client. Like `kafka` format, in this case, Pulsar consumers still cannot consume messages from Kafka producers.

### Kafka payload processor

[PIP 96](https://github.com/apache/pulsar/wiki/PIP-96%3A-Message-payload-processor-for-Pulsar-client) introduced a message payload processor for Pulsar consumer. KoP provides a processor implementation so that even if you configured `entryFormat=kafka` for better performance among Kafka clients, it could be also possible for Pulsar consumer to consume messages from Kafka producer.

You just need to configure the processor in your consumer application via `messagePayloadProcessor`. See the following code example:

```java
PulsarClient client = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
Consumer<byte[]> consumer = client.newConsumer()
        .topic("my-topic")
        .subscriptionName("my-sub")
        .messagePayloadProcessor(new KafkaPayloadProcessor()) // this extra line is needed
        .subscribe();
```

To import the `KafkaPayloadProcessor`, you should add the additional dependency.

```xml
    <dependency>
      <groupId>io.streamnative.pulsar.handlers</groupId>
      <artifactId>kafka-payload-processor</artifactId>
      <version>${pulsar.version}</version>
    </dependency>
```

The `pulsar.version` should be same with the version of your `pulsar-client` dependency.

## Network

| Name              | Description                                                  | Default |
| ----------------- | ------------------------------------------------------------ | ------- |
| maxQueuedRequests | Limit the queue size for request, like `queued.max.requests` in Kafka server. | 500     |
| requestTimeoutMs  | Limit the timeout in milliseconds for request, like `request.timeout.ms` in Kafka client.<br>If a request was not processed in the timeout, KoP would return an error response to client. | 30000   |
| connectionMaxIdleMs | The idle connection timeout in milliseconds. If the idle connection timeout (such as `connections.max.idle.ms` used in the Kafka server) is reached, the server handler will close this idle connection.<br>**Note**: If it is set to `-1`, it indicates that the idle connection timeout is disabled. | 600000 |
| failedAuthenticationDelayMs | Connection close delay on failed authentication: this is the time (in milliseconds) by which connection close will be delayed on authentication failure, like `connection.failed.authentication.delay.ms` in Kafka server. | 300 |
| brokerLookupTimeoutMs | The timeout for broker lookups (in milliseconds). | 30000 |

> **NOTE**
> 
> These limits are based on each connection.

## Prometheus

| Name                                     | Description                                                  | Default |
| ---------------------------------------- | ------------------------------------------------------------ | ------- |
| kopPrometheusStatsLatencyRolloverSeconds | Kop metrics exposed to prometheus rollover latency in seconds. | 60      |
| kopEnableGroupLevelConsumerMetrics       | Enable the group level consumer metrics.                     | false   |

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
|systemTopicRetentionSizeInMB| The system topic retention size in mb. | -1 |

## Transaction

This section lists configurations about the transaction.

| Name                         | Description                                        | Default |
| ---------------------------- | -------------------------------------------------- | ------- |
| kafkaTransactionCoordinatorEnabled | Whether to enable transaction coordinator.          | false   |
| kafkaBrokerId                     | The broker ID that is used to create the producer ID.  | 1       |
| kafkaTxnLogTopicNumPartitions     | the number of partitions for the transaction log topic. | 50      |
| kafkaTxnAbortTimedOutTransactionCleanupIntervalMs | The interval in milliseconds at which to rollback transactions that have timed out. | 10000 |
| kafkaTransactionalIdExpirationEnable | Whether to enable transactional ID expiration. | true |
| kafkaTransactionalIdExpirationMs | The time (in ms) that the transaction coordinator waits without receiving any transaction status updates for the current transaction before expiring its transactional ID. | 604800 |
| kafkaTransactionsRemoveExpiredTransactionalIdCleanupIntervalMs | The interval (in ms) at which to remove expired transactions. | 3600 |

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

## Schema Registry

| Name | Description | Default |
| ---- | ----------- | ------- |
| kopSchemaRegistryEnable | Whether to enable the Schema Registry | false |
| kopSchemaRegistryPort | The Schema Registry port | 8001 |
| kopSchemaRegistryNamespace | The namespace used for storing Kafka Schema Registry | __kafka_schemaregistry | 
| kopSchemaRegistryTopicName | The name of the topic used by the Schema Registry | __schema-registry |
