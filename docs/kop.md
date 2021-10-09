---
download: "https://github.com/streamnative/kop/releases/download/v{{protocol:version}}/pulsar-protocol-handler-kafka-{{protocol:version}}.nar"
alias: KoP - Kafka on Pulsar
---

KoP (Kafka on Pulsar) brings the native Apache Kafka protocol support to Apache Pulsar by introducing a Kafka protocol handler on Pulsar brokers. By adding the KoP protocol handler to your existing Pulsar cluster, you can migrate your existing Kafka applications and services to Pulsar without modifying the code. This enables Kafka applications to leverage Pulsar’s powerful features, such as:

- Streamlined operations with enterprise-grade multi-tenancy
- Simplified operations with a rebalance-free architecture
- Infinite event stream retention with Apache BookKeeper and tiered storage
- Serverless event processing with Pulsar Functions

KoP, implemented as a Pulsar [protocol handler](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/protocol/ProtocolHandler.java) plugin with the protocol name "kafka", is loaded when Pulsar broker starts. It helps reduce the barriers for people adopting Pulsar to achieve their business success by providing a native Kafka protocol support on Apache Pulsar. By integrating the two popular event streaming ecosystems, KoP unlocks new use cases. You can leverage advantages from each ecosystem and build a truly unified event streaming platform with Apache Pulsar to accelerate the development of real-time applications and services.

KoP implements the Kafka wire protocol on Pulsar by leveraging the existing components (such as topic discovery, the distributed log library - ManagedLedger, cursors and so on) that Pulsar already has.

The following figure illustrates how the Kafka-on-Pulsar protocol handler is implemented within Pulsar.

![](kop-architecture.png)

# Get Started with KoP

If you have an Apache Pulsar cluster, you can enable Kafka-on-Pulsar on your existing Pulsar cluster by downloading and installing the KoP protocol handler to Pulsar brokers directly. It takes three steps:
1. Download KoP protocol handler, or build the `./kafka-impl/target/pulsar-protocol-handler-kafka-{{protocol:version}}.nar` file, and then copy it to your Pulsar `protocols` directory.
2. Set the configuration of the KoP protocol handler in Pulsar `broker.conf` or `standalone.conf` files.
3. Restart Pulsar brokers to load KoP protocol handler.

And then you can start your broker and use KoP. The followings are detailed instructions for each step.

## Get KoP protocol handler

This section describes how to get the KoP protocol handler.

### Download KoP protocol handler

StreamNative provide a ready-to-use KoP docker image. You can download the [KoP protocol handler](https://github.com/streamnative/kop/releases) directly.

### Build KoP protocol handler from source code

To build the KoP protocol handler from the source, follow thse steps.

1. Clone the KoP GitHub project to your local. 

    ```bash
    git clone https://github.com/streamnative/kop.git
    cd kop
    ```

2. Build the project.
    ```bash
    mvn clean install -DskipTests
    ```

3. Get the `.nar` file in the following directory and copy it your Pulsar `protocols` directory. You need to create the `protocols` folder in Pulsar if it's the first time you use protocol handlers.

    ```bash
    ./kafka-impl/target/pulsar-protocol-handler-kafka-{{protocol:version}}.nar
    ```

## Set configuration for KoP

After you copy the `.nar` file to your Pulsar `/protocols` directory, you need to configure the Pulsar broker to run the KoP protocol handler as a plugin by adding configurations in the Pulsar configuration file `broker.conf` or `standalone.conf`.

1. Set the configuration of the KoP protocol handler in `broker.conf` or `standalone.conf` file.

    ```properties
    messagingProtocols=kafka
    protocolHandlerDirectory=./protocols
    allowAutoTopicCreationType=partitioned
    ```

    | Property | Default value | Proposed value |
    | :------- | :---------------------------- | :------------ |
    | `messagingProtocols` |  | kafka |
    | `protocolHandlerDirectory`|./protocols  | Location of KoP NAR file |
    | `allowAutoTopicCreationType`| non-partitioned | partitioned |

    By default, `allowAutoTopicCreationType` is set to `non-partitioned`. Since topics are partitioned by default in Kafka, it's better to avoid creating non-partitioned topics for Kafka clients unless Kafka clients need to interact with existing non-partitioned topics.

2. Set Kafka listeners.

    ```properties
    # Use `kafkaListeners` here for KoP 2.8.0 because `listeners` is marked as deprecated from KoP 2.8.0 
    kafkaListeners=PLAINTEXT://127.0.0.1:9092
    # This config is not required unless you want to expose another address to the Kafka client.
    # If it’s not configured, it will be the same with `kafkaListeners` config by default
    kafkaAdvertisedListeners=PLAINTEXT://127.0.0.1:9092
    ```
    - `kafkaListeners` is a comma-separated list of listeners and the host/IP and port to which Kafka binds to for listening. 
    - `kafkaAdvertisedListeners` is a comma-separated list of listeners with their host/IP and port. 

3. Set offset management as below since offset management for KoP depends on Pulsar "Broker Entry Metadata". It’s required for KoP 2.8.0 or higher version.

    ```properties
    brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor
    ```

4. Disable the deletion of inactive topics. It’s not required but **very important** in KoP. Currently, Pulsar deletes inactive partitions of a partitioned topic while the metadata of the partitioned topic is not deleted. KoP cannot [create missed partitions](http://pulsar.apache.org/docs/en/admin-api-topics/#create-missed-partitions) in this case.

    ```properties
    brokerDeleteInactiveTopicsEnabled=false
    ```

## Load KoP by restarting Pulsar brokers

After you have installed the KoP protocol handler to Pulsar broker, you can restart the Pulsar brokers to load KoP if you have configured the `conf/broker.conf` file. For a quick start, you can configure the `conf/standalone.conf` file and run a Pulsar standalone. You can verify if your KoP works well by running a Kafka client.

1. Download [Kafka 2.0.0](https://www.apache.org/dyn/closer.cgi?path=/kafka/2.0.0/kafka_2.11-2.0.0.tgz) and untar the release package.

    ```
    tar -xzf kafka_2.11-2.0.0.tgz
    cd kafka_2.11-2.0.0
    ```

2. Verify the KoP by using a Kafka producer and a Kafka consumer. Kafka binary contains a command-line producer and consumer.

    1. Run the command-line producer and send messages to the server.

        ```
        > bin/kafka-console-producer.sh --broker-list [pulsar-broker-address]:9092 --topic test
        This is a message
        This is another message
        ```

    
    2. Run the command-line consumer to receive messages from the server.

        ```
        > bin/kafka-console-consumer.sh --bootstrap-server [pulsar-broker-address]:9092 --topic test --from-beginning
        This is a message
        This is another message
        ```

# How to use KoP
You can configure and manage KoP based on your requirements. Check the following guides for more details.
- [Configure KoP](https://github.com/streamnative/kop/blob/branch-{{protocol:version}}/docs/configuration.md)
- [Secure KoP](https://github.com/streamnative/kop/blob/branch-{{protocol:version}}/docs/security.md)
- [Manage KoP with the Envoy proxy](https://github.com/streamnative/kop/blob/branch-{{protocol:version}}/docs/envoy-proxy.md)
- [Implementation: How to converse Pulsar and Kafka](https://github.com/streamnative/kop/blob/branch-{{protocol:version}}/docs/implementation.md)

The followings are important information when you configure and use KoP.

- Set both [retention and time to live (TTL)](http://pulsar.apache.org/docs/en/cookbooks-retention-expiry/) for KoP topics. If you only configure retention without configuring TTL, all messages of KoP topics cannot be deleted because KoP does not update a durable cursor.
-  If a Pulsar consumer and a Kafka consumer both subscribe the same topic with the same subscription (or group) name, the two consumers consume messages independently and they do not share the same subscription though the subscription name of a Pulsar client is the same with the group name of a Kafka client.
- KoP supports interaction between Pulsar client and Kafka client by default. If your topic is used only by the Pulsar client or only by the Kafka client, you can set `entryFormat=kafka` for better performance.

## Upgrade
If you want to upgrade your KoP version, you must first [upgrade your Pulsar version](https://pulsar.apache.org/docs/en/next/administration-upgrade/) accordingly, and then configure offset management.
