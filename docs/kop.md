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

StreamNative provide ready-to-use [KoP docker images](https://hub.docker.com/r/streamnative/sn-pulsar). You can also download the [KoP protocol handler](https://github.com/streamnative/kop/releases) directly to deploy with [the official Apache Pulsar docker images](https://hub.docker.com/r/apachepulsar/pulsar) or [the Pulsar binaries](https://pulsar.apache.org/download/).

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
    narExtractionDirectory=/path/to/nar
    ```

    | Property | Default value | Proposed value |
    | :------- | :---------------------------- | :------------ |
    | `messagingProtocols` |  | kafka |
    | `protocolHandlerDirectory`|./protocols  | Location of KoP NAR file |
    | `allowAutoTopicCreationType`| non-partitioned | partitioned |
    | `narExtractionDirectory` | `/tmp/pulsar-nar` | Location of unpacked KoP NAR file |

    By default, `allowAutoTopicCreationType` is set to `non-partitioned`. Since topics are partitioned by default in Kafka, it's better to avoid creating non-partitioned topics for Kafka clients unless Kafka clients need to interact with existing non-partitioned topics.

    By default, the `/tmp/pulsar-nar` directory is under the `/tmp` directory. If we unpackage the KoP NAR file into the `/tmp` directory, some classes could be automatically deleted by the system, which will generate a`ClassNotFoundException` or `NoClassDefFoundError` error. Therefore, it is recommended to set the `narExtractionDirectory` option to another path.

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

### Run KoP in Docker

KoP is a built-in component in StreamNative's `sn-pulsar` image, whose tag matches KoP's version. Take KoP 2.9.1.1 for example, you can execute `docker compose up` command in the KoP project directory to start a Pulsar standalone with KoP being enabled. KoP has a single advertised listener `127.0.0.1:19092`, so you should use Kafka's CLI tool to connect KoP, as shown below:

```bash
$ ./bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic my-topic                 
>hello
>world
>^C                                                                                                                                                                                                                                                        $ ./bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic my-topic --from-beginning
hello
world
^CProcessed a total of 2 messages
```

See [docker-compose.yml](../docker-compose.yml) for more details.

Similar to configuring KoP in a cluster that is started in Docker, you only need to add the environment varialble according to your customized configuration and ensure to execute `bin/apply-config-from-env.py conf/broker.conf` before executing `bin/pulsar broker`. The environment variable should be a property's key if it already exists in the configuration file. Otherwise it should have the prefix `PULSAR_PREFIX_`.

# How to use KoP

You can configure and manage KoP based on your requirements. Check the following guides for more details.

> **NOTE**
>
> The following links are invalid when you check this document in the `master` branch from GitHub. You can go to the same chapter of the [README](../README.md) for the correct links.

-   [Configure KoP](https://github.com/streamnative/kop/blob/branch-{{protocol:version}}/docs/configuration.md)
-   [Monitor KoP](https://github.com/streamnative/kop/blob/branch-{{protocol:version}}/docs/reference-metrics.md)
-   [Upgrade](https://github.com/streamnative/kop/blob/branch-{{protocol:version}}/docs/upgrade.md)
-   [Secure KoP](https://github.com/streamnative/kop/blob/branch-{{protocol:version}}/docs/security.md)
-   [Schema Registry](https://github.com/streamnative/kop/blob/branch-{{protocol:version}}/docs/schema.md)
-   [Implementation: How to converse Pulsar and Kafka](https://github.com/streamnative/kop/blob/branch-{{protocol:version}}/docs/implementation.md)

The followings are important information when you configure and use KoP.

- Set both [retention and time to live (TTL)](http://pulsar.apache.org/docs/en/cookbooks-retention-expiry/) for KoP topics. If you only configure retention without configuring TTL, all messages of KoP topics cannot be deleted because KoP does not update a durable cursor.
-  If a Pulsar consumer and a Kafka consumer both subscribe the same topic with the same subscription (or group) name, the two consumers consume messages independently and they do not share the same subscription though the subscription name of a Pulsar client is the same with the group name of a Kafka client.
- KoP supports interaction between Pulsar client and Kafka client by default. If your topic is used only by the Pulsar client or only by the Kafka client, you can set `entryFormat=kafka` for better performance.
