# Kafka-on-Pulsar (KoP)

KoP (Kafka on Pulsar) brings the native Apache Kafka protocol support to Apache Pulsar by introducing a Kafka protocol handler on Pulsar brokers. By adding the KoP protocol handler to your existing Pulsar cluster, you can migrate your existing Kafka applications and services to Pulsar without modifying the code. This enables Kafka applications to leverage Pulsar’s powerful features, such as:

- Streamlined operations with enterprise-grade multi-tenancy
- Simplified operations with a rebalance-free architecture
- Infinite event stream retention with Apache BookKeeper and tiered storage
- Serverless event processing with Pulsar Functions

KoP, implemented as a Pulsar [protocol handler](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/protocol/ProtocolHandler.java) plugin with the protocol name "kafka", is loaded when Pulsar broker starts. It helps reduce the barriers for people adopting Pulsar to achieve their business success by providing a native Kafka protocol support on Apache Pulsar. By integrating the two popular event streaming ecosystems, KoP unlocks new use cases. You can leverage advantages from each ecosystem and build a truly unified event streaming platform with Apache Pulsar to accelerate the development of real-time applications and services.

KoP implements the Kafka wire protocol on Pulsar by leveraging the existing components (such as topic discovery, the distributed log library - ManagedLedger, cursors and so on) that Pulsar already has.

The following figure illustrates how the Kafka-on-Pulsar protocol handler is implemented within Pulsar.

![](docs/kop-architecture.png)

# Version compatibility

The version of KoP `x.y.z.m` conforms to Pulsar `x.y.z`, while `m` is the patch version number. KoP might also be compatible with older patched versions, but it's not guaranteed. See [upgrade.md](./docs/upgrade.md) for details.

KoP is compatible with Kafka clients 0.9 or higher. For Kafka clients 3.2.0 or higher, you have to add the following configurations in KoP because of [KIP-679](https://cwiki.apache.org/confluence/display/KAFKA/KIP-679%3A+Producer+will+enable+the+strongest+delivery+guarantee+by+default).

```properties
kafkaTransactionCoordinatorEnabled=true
brokerDeduplicationEnabled=true
```

# How to use KoP
You can configure and manage KoP based on your requirements. Check the following guides for more details.
-   [Quick Start](docs/kop.md)
-   [Configure KoP](docs/configuration.md)
-   [Monitor KoP](docs/reference-metrics.md)
-   [Upgrade](docs/upgrade.md)
-   [Secure KoP](docs/security.md)
-   [Schema Registry](docs/schema.md)
-   [Implementation: How to converse Pulsar and Kafka](docs/implementation.md)

# Project Maintainers

-   [@aloyszhang](https://github.com/aloyszhang)
-   [@BewareMyPower](https://github.com/BewareMyPower)
-   [@Demogorgon314](https://github.com/Demogorgon314)
-   [@dockerzhang](https://github.com/dockerzhang)
-   [@hangc0276](https://github.com/hangc0276)
-   [@jiazhai](https://github.com/jiazhai)
-   [@PierreZ](https://github.com/PierreZ)
-   [@wenbingshen](https://github.com/wenbingshen)
-   [@wuzhanpeng](https://github.com/wuzhanpeng)
