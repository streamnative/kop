# KoP

KoP stands for Kafka on Pulsar. KoP broker supports Kafka protocols, and is backed by Pulsar.

KoP is implemented as a Pulsar [ProtocolHandler](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/protocol/ProtocolHandler.java) with protocol name "kafka"
ProtocolHandler is built as a `nar` file, and will be loaded when Pulsar Broker starting.

> NOTE: KoP currently supports [Kafka Client 2.0.0](https://kafka.apache.org/20/documentation.html). And KoP is build based on [Pulsar 2.5.0](http://pulsar.apache.org/en/download/)

## Limitations for KoP

KoP leverage Pulsar features, but some of the manners between Pulsar and Kafka are different. In this implementation, there are some limitations.

- KoP does not support Pulsar non-partitioned topic. Because all topics in Kafka are partitioned type, not support non-partitioned topic is easy to align this.
- All topics in KoP are placed under a user specified tenant and namespace. 

## Get started

In this guide, you will learn how to use the KoP broker to serve requests from Kafka client.

### Download Pulsar 

Download [Pulsar 2.5.0](http://pulsar.apache.org/en/download/) binary package `apache-pulsar-2.5.0-bin.tar.gz`. and unzip it.

### Download KoP Plugin

https://github.com/streamnative/kop/releases

### Config Pulsar broker to run KoP protocol handler as Plugin

As mentioned above, KoP module is loaded along with Pulsar broker. You need to add configs in Pulsar's config file, such as `broker.conf` or `standalone.conf`.

1. Protocol handler's config

You need to add `messagingProtocols`(default value is null) and  `protocolHandlerDirectory` ( default value is "./protocols"), in Pulsar's config file, such as `broker.conf` or `standalone.conf`
For KoP, value for `messagingProtocols` is `kafka`; value for `protocolHandlerDirectory` is the place of KoP nar file.

e.g.
```access transformers
messagingProtocols=kafka
protocolHandlerDirectory=./protocols
```

2. Set Kafka service listeners

Set Kafka service `listeners`. Note that the hostname value in listeners should be the same as Pulsar broker's `advertisedAddress`.

e.g.
```
listeners=PLAINTEXT://127.0.0.1:9092
advertisedAddress=127.0.0.1
```

### Run Pulsar broker.

With above 2 configs, you can start your Pulsar broker. You can follow Pulsar's [Get started page](http://pulsar.apache.org/docs/en/standalone/) for more details

```access transformers
cd apache-pulsar-2.5.0
bin/pulsar standalone
```

### Run Kafka Client to verify.

1. Download the [Kafka 2.0.0](https://www.apache.org/dyn/closer.cgi?path=/kafka/2.0.0/kafka_2.11-2.0.0.tgz) release and un-tar it.

```access transformers
tar -xzf kafka_2.11-2.0.0.tgz
cd kafka_2.11-2.0.0
```

2. Use console producer/consumer to verify.

Run the producer and then type a few messages into the console to send to the server.

```access transformers
> bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic test
This is a message
This is another message
```

Kafka also has a command line consumer that will dump out messages to standard output.

```access transformers
> bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic test --from-beginning
This is a message
This is another message
```



### Other configs.

#### log level config

In Pulsar's [log4j2.yaml config file](https://github.com/apache/pulsar/blob/master/conf/log4j2.yaml), you can set KoP's log level.

e.g.
```
    Logger:
      - name: io.streamnative.pulsar.handlers.kop
        level: warn
        additivity: false
        AppenderRef:
          - ref: Console
``` 

#### SSL Connection

KoP support Kafka listeners config of type "PLAINTEXT" and "SSL". 
You could set config like `listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093`. 
Please reference [Kafka SSL document](https://kafka.apache.org/documentation/#security_ssl) for how to config SSL keys.
Here is some steps that you need to be able to connect KoP through SSL.

1. create SSL related Keys.

Here is an example of a bash script to create related CA and jks files.
```access transformers
            #!/bin/bash
            #Step 1
            keytool -keystore server.keystore.jks -alias localhost -validity 365 -keyalg RSA -genkey
            #Step 2
            openssl req -new -x509 -keyout ca-key -out ca-cert -days 365
            keytool -keystore server.truststore.jks -alias CARoot -import -file ca-cert
            keytool -keystore client.truststore.jks -alias CARoot -import -file ca-cert
            #Step 3
            keytool -keystore server.keystore.jks -alias localhost -certreq -file cert-file
            openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days 365 -CAcreateserial -passin pass:test1234
            keytool -keystore server.keystore.jks -alias CARoot -import -file ca-cert
            keytool -keystore server.keystore.jks -alias localhost -import -file cert-signed
```

2. config KoP Broker.

In Pulsar's config file (`broker.conf` or `standalone.conf`), Add related configurations that using the jks configs that create in step1:
```access transformers
listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093

kopSslKeystoreLocation=/Users/kop/server.keystore.jks
kopSslKeystorePassword=test1234
kopSslKeyPassword=test1234
kopSslTruststoreLocation=/Users/kop/server.truststore.jks
kopSslTruststorePassword=test1234
```

3. config kafka clients

This is similar to [Kafka client config doc](https://kafka.apache.org/documentation/#security_configclients).

Prepare a file named `client-ssl.properties`, which contains:
```
security.protocol=SSL
ssl.truststore.location=client.truststore.jks
ssl.truststore.password=test1234
ssl.endpoint.identification.algorithm=
```

And verify us console-producer and console-consumer:
```access transformers
kafka-console-producer.sh --broker-list localhost:9093 --topic test --producer.config client-ssl.properties
kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic test --consumer.config client-ssl.properties
```

#### KoP auth

You can enable both authentication and authorization on KoP. It will use the underlying Pulsar auth mechanisms.

To forward your credentials, `SASL-PLAIN` is used on Kafka's side:

* The user must be your fully qualified namespace
* the password must be your auth params from pulsar, for example `token:xxx`

###### Enable Auth on broker

To enable KoP auth, you need to set all the options required by Pulsar to enable auth, and also:

*  `saslAllowedMechanisms`: default value is `PLAIN`

###### Enable auth on Kafka client

You can use the following code to enable SASL-PLAIN through jaas:
```java
String tenant = "ns1/tenant1";
String pasword = "token:xxx";

String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
String jaasCfg = String.format(jaasTemplate, tenant, password);
props.put("sasl.jaas.config", jaasCfg);
props.put("security.protocol", "SASL_PLAINTEXT");
props.put("sasl.mechanism", "PLAIN");
```

#### all the KoP configs.

There is also other configs that can be changed and placed into Pulsar broker config file.

```bash

# The messaging Protocols that avilabale when loaded by Pulsar Broker.
messagingProtocols=kafka

# ListenersProp for Kafka service(host should follow the advertisedAddress).
#   e.g. PLAINTEXT://localhost:9092,SSL://localhost:9093
listeners=PLAINTEXT://127.0.0.1:9092

# Kafka on Pulsar Broker tenant
kafkaTenant=public

# Kafka on Pulsar Broker namespace
kafkaNamespace=default

# The tenant used for storing Kafka metadata topics
kafkaMetadataTenant=public

# The namespace used for storing Kafka metadata topics
kafkaMetadataNamespace=__kafka

# Flag to enable group coordinator
enableGroupCoordinator=true

# The minimum allowed session timeout for registered consumers.
# Shorter timeouts result in quicker failure detection at the cost
# of more frequent consumer heartbeating, which can overwhelm broker resources.
groupMinSessionTimeoutMs=6000

# The maximum allowed session timeout for registered consumers.
# Longer timeouts give consumers more time to process messages in
# between heartbeats at the cost of a longer time to detect failures.
groupMaxSessionTimeoutMs=300000

# The amount of time the group coordinator will wait for more consumers
# to join a new group before performing  the first rebalance. A longer
# delay means potentially fewer rebalances, but increases the time until
# processing begins
groupInitialRebalanceDelayMs=3000

# Compression codec for the offsets topic - compression may be used to achieve "atomic" commits
offsetsTopicCompressionCodec=NONE

# The maximum size in Bytes for a metadata entry associated with an offset commit
offsetMetadataMaxSize=4096

# Offsets older than this retention period will be discarded, default 7 days
offsetsRetentionMinutes=10080

# Frequency at which to check for stale offsets
offsetsRetentionCheckIntervalMs=600000

# Number of partitions for the offsets topic
offsetsTopicNumPartitions=8

### --- KoP SSL configs--- ###

# Kafka ssl configuration map with: SSL_PROTOCOL_CONFIG = ssl.protocol
kopSslProtocol=TLS

# Kafka ssl configuration map with: SSL_PROVIDER_CONFIG = ssl.provider
kopSslProvider=

# Kafka ssl configuration map with: SSL_CIPHER_SUITES_CONFIG = ssl.cipher.suites
kopSslCipherSuites=

# Kafka ssl configuration map with: SSL_ENABLED_PROTOCOLS_CONFIG = ssl.enabled.protocols
kopSslEnabledProtocols=TLSv1.2,TLSv1.1,TLSv1

# Kafka ssl configuration map with: SSL_KEYSTORE_TYPE_CONFIG = ssl.keystore.type
kopSslKeystoreType=JKS

# Kafka ssl configuration map with: SSL_KEYSTORE_LOCATION_CONFIG = ssl.keystore.location
kopSslKeystoreLocation=

# Kafka ssl configuration map with: SSL_KEYSTORE_PASSWORD_CONFIG = ssl.keystore.password
kopSslKeystorePassword=

# Kafka ssl configuration map with: SSL_KEY_PASSWORD_CONFIG = ssl.key.password
kopSslKeyPassword=

# Kafka ssl configuration map with: SSL_TRUSTSTORE_TYPE_CONFIG = ssl.truststore.type
kopSslTruststoreType=JKS

# Kafka ssl configuration map with: SSL_TRUSTSTORE_LOCATION_CONFIG = ssl.truststore.location
kopSslTruststoreLocation=

# Kafka ssl configuration map with: SSL_TRUSTSTORE_PASSWORD_CONFIG = ssl.truststore.password
kopSslTruststorePassword=

# Kafka ssl configuration map with: SSL_KEYMANAGER_ALGORITHM_CONFIG = ssl.keymanager.algorithm
kopSslKeymanagerAlgorithm=SunX509

# Kafka ssl configuration map with: SSL_TRUSTMANAGER_ALGORITHM_CONFIG = ssl.trustmanager.algorithm
kopSslTrustmanagerAlgorithm=SunX509

# Kafka ssl configuration map with:
#      SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG = ssl.secure.random.implementation
kopSslSecureRandomImplementation=

# supported SASL mechanisms exposed by broker
saslAllowedMechanisms=
```
