# KoP

KoP (Kafka on Pulsar) supports Kafka protocol and it is backed by Pulsar, which means you can use Pulsar as the infrastructure without modifying various applications and services based on Kafka API.

KoP, implemented as a Pulsar [protocol handler](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/protocol/ProtocolHandler.java) plugin with protocol name "kafka", is loaded when Pulsar broker starts.

![](docs/kop-architecture.png)

KoP is part of StreamNative Platform. Please visit [StreamNative Docs](https://streamnative.io/docs) for more details.
