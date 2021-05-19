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

# What's New in KoP 2.8.0
The following new features are introduced in KoP 2.8.0.

- Offset management
- Message encoding and decoding
- OAuth 2.0 authentication
- Metrics related to produce, fetch and request

## Enhancement 
The following enhancement is added in KoP 2.8.0.
- Support more clients for Kafka admin
- Enable advertised listeners, so users can use Envoy Kafka filter directly

# Version compatibility

Since Pulsar 2.6.2, KoP version changes with Pulsar version accordingly. The version of KoP `x.y.z.m` conforms to Pulsar `x.y.z`, while `m` is the patch version number. 

| KoP version | Pulsar version |
| :---------- | :------------- |
| [2.8.0](https://github.com/streamnative/kop/releases/tag/v2.8.0) |Pulsar 2.8.0|
| [2.7.1](https://github.com/streamnative/kop/releases/tag/v2.7.1) |Pulsar 2.7.1|
| [2.7.0](https://github.com/streamnative/kop/releases/tag/v2.7.0) |Pulsar 2.7.0|
| [2.6.3](https://github.com/streamnative/kop/releases/tag/v2.6.3) |Pulsar 2.6.3|
| [2.6.2](https://github.com/streamnative/kop/releases/tag/v2.6.2) |Pulsar 2.6.2|
| [0.3.0](https://github.com/streamnative/kop/releases/tag/v0.3.0) |Pulsar 2.6.1|
| [0.2.0](https://github.com/streamnative/kop/releases/tag/v0.2.0) |Pulsar 2.5.0|
| [0.1.0](https://github.com/streamnative/kop/releases/tag/v0.1.0) |Pulsar 2.5.0|

- You can download different versions of Pulsar at the [Pulsar download center](http://pulsar.apache.org/en/download/).
- For supported Kafka clients, see [here](integrations/README.md).
- The `pulsar-protocol-handler-kafka-${version}.nar` is the KoP binary file which can be loaded by Pulsar broker.

# Get Started with KoP

If you have a Apache Pulsar cluster, you can enable Kafka-on-Pulsar on your existing Pulsar cluster by downloading and installing the KoP protocol handler to Pulsar brokers directly. It takes three steps:
1. Download KoP protocol handler, get the `./kafka-impl/target/pulsar-protocol-handler-kafka-${version}.nar` file, and then copy it to your Pulsar `/protocols` directory.
2. Set the configuration of the KoP protocol handler in Pulsar `broker.conf` or `standalone.conf` files.
3. Restart Pulsar brokers to load KoP protocol handler.

And then you can start your broker and use KoP. The following are detailed instructions for each step.

## Download KoP protocol handler

You can download the KoP protocol handler [here](https://github.com/streamnative/kop/releases), or you can build the KoP protocol handler from source code.

1. Clone the KoP github project to your local. 

```bash
git clone https://github.com/streamnative/kop.git
cd kop
```

2. Build the project.
```bash
mvn clean install -DskipTests
```

3. Get the nar file in the following directory and copy it your Pulsar `/protocols` directory.
```bash
./kafka-impl/target/pulsar-protocol-handler-kafka-${version}.nar
```

## Set configuration for KoP

After you copy the .nar file to your Pulsar `/protocols` directory, you need to configure the Pulsar broker to run the KoP protocol handler as a plugin by adding configurations in Pulsar configuration file `broker.conf` or `standalone.conf`.

1. Set the configuration of the KoP protocol handler in `broker.conf` or `standalone.conf` file.

    ```properties
    messagingProtocols=kafka
    protocolHandlerDirectory=./protocols
    allowAutoTopicCreationType=partitioned
    ```

    | Property | The value you need to set | Default value |
    | :------- | :---------------------------- | :------------ |
    | `messagingProtocols` | kafka | null |
    | `protocolHandlerDirectory`| Location of KoP NAR file | ./protocols |
    | `allowAutoTopicCreationType`| partitioned | non-partitioned |

    You need to set `allowAutoTopicCreationType` to `partitioned` since KoP only supports partitioned topics. If it is set to `non-partitioned` by default, the topics created automatically by KoP are still partitioned topics, yet topics created automatically by Pulsar broker are non-partitioned topics.

2. Set Kafka service listeners.

    ```properties
    listeners=PLAINTEXT://127.0.0.1:9092
    advertisedAddress=127.0.0.1
    ```

    > The hostname in listeners should be the same as Pulsar broker's `advertisedAddress`.

3. Set offset management as follows since offset management for KoP depends on Pulsar "Broker Entry Metadata".

    ```properties
    brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor
    ```

## Restart Pulsar brokers to load KoP

After you have installed the KoP protocol handler to Pulsar broker, you can restart the Pulsar brokers to load KoP. You can verify if your KoP works well by running a Kafka client.

1. Download [Kafka 2.0.0](https://www.apache.org/dyn/closer.cgi?path=/kafka/2.0.0/kafka_2.11-2.0.0.tgz) and untar the release package.

    ```
    tar -xzf kafka_2.11-2.0.0.tgz
    cd kafka_2.11-2.0.0
    ```

2. Verify the KoP by using a Kafka producer and a Kafka consumer. Kafka binary contains a command-line producer and consumer.

    Run the command-line producer and send messages to the server.

    ```
    > bin/kafka-console-producer.sh --broker-list [pulsar-broker-address]:9092 --topic test
    This is a message
    This is another message
    ```

    Kafka has a command-line consumer dumping out messages to standard output.

    ```
    > bin/kafka-console-consumer.sh --bootstrap-server [pulsar-broker-address]:9092 --topic test --from-beginning
    This is a message
    This is another message
    ```

# How to use KoP
You can configure and manage KoP based on your requirements. Check the following guides for more details.
- [Configure KoP](docs/configuration.md)
- [Secure KoP](docs/security.md)
- [Manage KoP with the [Envoy](https://www.envoyproxy.io) proxy](docs/envoy-proxy.md)
- [Implementation: How to converse Pulsar and Kafka](docs/implementation.md)

## Upgrade
If you want to upgrade your KoP version, you must first upgrade your Pulsar version accordingly.

# Project Maintainers

-   [@aloyszhang](https://github.com/aloyszhang)
-   [@BewareMyPower](https://github.com/BewareMyPower)
-   [@dockerzhang](https://github.com/dockerzhang)
-   [@jiazhai](https://github.com/jiazhai)
-   [@PierreZ](https://github.com/PierreZ)
