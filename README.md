# Kafka-on-Pulsar (aka KoP)

KoP (Kafka on Pulsar) brings the native Apache Kafka protocol support to Apache Pulsar by introducing a Kafka protocol handler on Pulsar brokers. By adding the KoP protocol handler to your existing Pulsar cluster, you can now migrate your existing Kafka applications and services to Pulsar without modifying the code. This enables Kafka applications to leverage Pulsar’s powerful features, such as:

- Streamlined operations with enterprise-grade multi-tenancy
- Simplified operations with a rebalance-free architecture
- Infinite event stream retention with Apache BookKeeper and tiered storage
- Serverless event processing with Pulsar Functions

KoP, implemented as a Pulsar [protocol handler](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/protocol/ProtocolHandler.java) plugin with the protocol name "kafka", is loaded when Pulsar broker starts. It is believed that providing a native Kafka protocol support on Apache Pulsar helps reduce the barriers for people adopting Pulsar to achieve their business success. By integrating two popular event streaming ecosystems, KoP unlocks new use cases. You can leverage advantages from each ecosystem and build a truly unified event streaming platform with Apache Pulsar to accelerate the development of real-time applications and services.

KoP implements the Kafka wire protocol on Pulsar by leveraging the existing components (such as topic discovery, the distributed log library - ManagedLedger, cursors and so on) that Pulsar already has.

The following figure illustrates how is the Kafka-on-Pulsar protocol handler was implemented within Pulsar.

![](docs/kop-architecture.png)

## Prerequisite

From 2.6.2.0 on, KoP `x.y.z.m` is based on Pulsar `x.y.z`, while `m` is the patch version number. See [here](integrations/README.md) for supported Kafka clients.

Before KoP 2.6.2.0, check the following requirements.

| KoP version | Kafka client version | Pulsar version |
| :---------- | :------------------- | :------------- |
| [0.3.0](https://github.com/streamnative/kop/releases/tag/v0.3.0) | [Kafka client 1.x, 2.0~2.6](integrations/README.md) | [Pulsar 2.6.1](http://pulsar.apache.org/en/download/) |
| [0.2.0](https://github.com/streamnative/kop/releases/tag/v0.2.0) | [Kafka client 2.0.0](https://kafka.apache.org/20/documentation.html) | [Pulsar 2.5.0](http://pulsar.apache.org/en/download/) |
| [0.1.0](https://github.com/streamnative/kop/releases/tag/v0.1.0) | [Kafka client 2.0.0](https://kafka.apache.org/20/documentation.html) | [Pulsar 2.5.0](http://pulsar.apache.org/en/download/) |

The `pulsar-protocol-handler-kafka-${version}.nar` is the KoP binary file which can be loaded by Pulsar broker.

## Enable KoP on your existing Apache Pulsar clusters

If you already have a Apache Pulsar cluster running, it is very straightforward to enable Kafka-on-Pulsar on your existing Pulsar
cluster by downloading and installing the KoP protocol handler to Pulsar brokers.

### Download or build KoP protocol handler

You can download the released KoP protocol handler from [here](https://github.com/streamnative/kop/releases).

Alternatively, you can also build the KoP protocol handler from source code.

1. clone this project from GitHub to your local.

```bash
git clone https://github.com/streamnative/kop.git
cd kop
```

2. build the project.
```bash
mvn clean install -DskipTests
```

3. the nar file can be found at this location.
```bash
./kafka-impl/target/pulsar-protocol-handler-kafka-${version}.nar
```

### Install KoP protocol handler

As mentioned previously, the KoP protocol handler is a plugin that can be installed to the Pulsar brokers.

All what you need to do is to configure the Pulsar broker to run the KoP protocol handler as a plugin, that is,
add configurations in Pulsar's configuration file, such as `broker.conf` or `standalone.conf`.

1. Set the configuration of the KoP protocol handler.

    Add the following properties and set their values in Pulsar configuration file, such as `conf/broker.conf` or `conf/standalone.conf`.

    KoP supports partitioned topics only. Therefore, you had better to set `allowAutoTopicCreationType` to `partitioned`. If it is set to `non-partitioned` by default, the topics that are automatically created by KoP are still partitioned topics. However, topics that are created automatically by Pulsar broker are non-partitioned topics.

    | Property | Set it to the following value | Default value |
    | :------- | :---------------------------- | :------------ |
    | `messagingProtocols` | kafka | null |
    | `protocolHandlerDirectory`| Location of KoP NAR file | ./protocols |
    | `allowAutoTopicCreationType`| partitioned | non-partitioned |

    **Example**

    ```properties
    messagingProtocols=kafka
    protocolHandlerDirectory=./protocols
    allowAutoTopicCreationType=partitioned
    ```

2. Set Kafka service listeners.

    > #### Note
    > The hostname in listeners should be the same as Pulsar broker's `advertisedAddress`.

    **Example**

    ```properties
    listeners=PLAINTEXT://127.0.0.1:9092
    advertisedAddress=127.0.0.1
    ```
3. Offset Management

    Offset management for KoP is dependent on "Broker Entry Metadata" feature of Pulsar. So, you should set `brokerEntryMetadataInterceptors` to `org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor`.

    **Example**
    ```properties
    brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor
    ```
### Restart Pulsar brokers to load KoP

After you have installed the KoP protocol handler to Pulsar broker, you can restart the Pulsar brokers to load KoP.

### Run Kafka client to verify

1. Download the [Kafka 2.0.0](https://www.apache.org/dyn/closer.cgi?path=/kafka/2.0.0/kafka_2.11-2.0.0.tgz) release and untar it.

    ```
    tar -xzf kafka_2.11-2.0.0.tgz
    cd kafka_2.11-2.0.0
    ```

2. Use a Kafka producer and a Kafka consumer to verify.

    In Kafka’s binary, there is a command-line producer and consumer.

    Run the command-line producer and send a few messages to the server.

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

## Configure KoP

For details, see [Configure KoP](docs/configuration.md).

## Secure KoP

KoP supports TLS encryption and integrates with Pulsar's authentication and authorization providers seamlessly.

For details, see [Secure KoP](docs/security.md).

## Manage KoP

### Envoy proxy for KoP

You can use [Envoy](https://www.envoyproxy.io) as a proxy for KoP. For more information, see [here](docs/envoy-proxy.md).

## Implementation

See [Implementation](docs/implementation.md) for the implementation details, including some difference of basic concepts between Kafka and Pulsar, and how the conversion is done.

## Project Maintainers

-   [@aloyszhang](https://github.com/aloyszhang)
-   [@BewareMyPower](https://github.com/BewareMyPower)
-   [@dockerzhang](https://github.com/dockerzhang)
-   [@jiazhai](https://github.com/jiazhai)
-   [@PierreZ](https://github.com/PierreZ)
