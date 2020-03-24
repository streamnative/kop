# Kafka-on-Pulsar (aka KoP)

Kafka-on-Pulsar (aka KoP) was developed to support Kafka protocol natively on Apache Pulsar.

KoP brings the native Apache Kafka protocol support to Apache Pulsar by introducing a Kafka [protocol handler](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/protocol/ProtocolHandler.java) on
Pulsar brokers. By adding the KoP protocol handler to your existing Pulsar cluster, you can now migrate your existing
Kafka applications and services to Pulsar without modifying the code. This enables Kafka applications to leverage Pulsar’s
powerful features, such as:

- [x] Streamlined operations with enterprise-grade multi-tenancy. 
- [x] Simplified operations with a rebalance-free architecture.
- [x] Infinite event stream retention with Apache BookKeeper and tiered storage.
- [x] Serverless event processing with Pulsar Functions.

KoP unlocks new use cases by integrating two popular messaging and event streaming ecosystems together.
Customers can leverage advantages from each ecosystem and build a truly unified event streaming platform with
Apache Pulsar to accelerate the development of real-time applications and services. 

The following figure illustrates how is the Kafka-on-Pulsar protocol handler was implemented within Pulsar.

![](docs/kop-architecture.png)

> KoP is part of StreamNative Platform. Please visit [StreamNative Docs](https://streamnative.io/docs) for more details.

## Get started

Kafka-on-Pulsar is released and bundled as part of StreamNative Platform. You can download [StreamNative Platform](https://streamnative.io/download/platform) to [get started](https://streamnative.io/docs/v1.0.0/get-started/local/)

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


    Property | Set it to the following value | Default value
    |---|---|---
    `messagingProtocols` | kafka | null
    `protocolHandlerDirectory`| Location of KoP NAR file | ./protocols

    **Example**

    ```
    messagingProtocols=kafka
    protocolHandlerDirectory=./protocols
    ```

2. Set Kafka service listeners.

    > #### Note
    > The hostname in listeners should be the same as Pulsar broker's `advertisedAddress`.

    **Example**

    ```
    listeners=PLAINTEXT://127.0.0.1:9092
    advertisedAddress=127.0.0.1
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

See [Configure KoP](https://streamnative.io/docs/v1.0.0/configure/pulsar-core/kop/) for more details.

## Secure KoP

KoP supports TLS encryption and integrates with Pulsar's authentication and authorization providers seamlessly.

See [Secure KoP](https://streamnative.io/docs/v1.0.0/secure/pulsar-core/kop/) for more details.

---

For the complete documentation of KoP, please checkout [StreamNative Documentation](https://streamnative.io/docs/kop).
