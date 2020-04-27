# KoP

KoP (Kafka on Pulsar) supports Kafka protocol and it is backed by Pulsar, which means you can use Pulsar as the infrastructure without modifying various applications and services based on Kafka API.

KoP, implemented as a Pulsar [protocol handler](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/protocol/ProtocolHandler.java) plugin with protocol name "kafka", is loaded when Pulsar broker starts.

![](docs/kop-architecture.png)

KoP is part of StreamNative Platform. Please visit [StreamNative Docs](https://streamnative.io/docs) for more details.

## Supported version

Currently, KoP has the following version.

KoP version | Release notes | Download link
|---|---|---
0.1.0 | See [here](#release-notes) | See [here](https://github.com/streamnative/kop/releases/tag/v0.1.0)

## Prerequisite

Check the following requirements before using KoP. 

Currently, KoP supports **[Kafka Client 2.0.0](https://kafka.apache.org/20/documentation.html)** and it is build based on **[Pulsar 2.5.0](http://pulsar.apache.org/en/download/)**.

KoP version | Kafka client version | Pulsar version
|---|---|---
[0.1.0]((#release-notes)) | [Kafka client 2.0.0](https://kafka.apache.org/20/documentation.html) | [Pulsar 2.5.0](http://pulsar.apache.org/en/download/)


## Download 

1. Download [Pulsar 2.5.0](http://pulsar.apache.org/en/download/) binary package `apache-pulsar-2.5.0-bin.tar.gz`. and unzip it.

2. Download KoP Plugin at [here](https://github.com/streamnative/kop/releases).

## Build KoP nar from source code

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

## Configure

As mentioned previously, KoP module is loaded along with the Pulsar broker. You need to configure the Pulsar broker to run the KoP protocol handler as a plugin, that is, add configurations in Pulsar's configuration file, such as `broker.conf` or `standalone.conf`.

1. Set the configuration of the KoP protocol handler.

    Add the following properties and set their values in Pulsar configuration file, such as `conf/broker.conf` or `conf/standalone.conf`.
    
    Regarding topic auto create partition type, if you are not using [StreamNative Platform](https://streamnative.io/docs/v1.0.0/), please set it to `partitioned`.

    Property | Set it to the following value | Default value
    |---|---|---
    `messagingProtocols` | kafka | null
    `protocolHandlerDirectory`| Location of KoP NAR file | ./protocols
    `protocolHandlerDirectory`| Location of KoP NAR file | ./protocols
    `allowAutoTopicCreationType`| partitioned | non-partitioned
    
    **Example**

    ```
    messagingProtocols=kafka
    protocolHandlerDirectory=./protocols
    allowAutoTopicCreationType=partitioned
    ```

2. Set Kafka service listeners.

    > #### Note
    > The hostname in listeners should be the same as Pulsar broker's `advertisedAddress`.

    **Example**

    ```
    listeners=PLAINTEXT://127.0.0.1:9092
    advertisedAddress=127.0.0.1
    ```

## Run 

The instructions below assume you use KoP 0.1.0.

### Run Pulsar broker in standalone mode

Run the following commands to start Pulsar locally. 

```
cd apache-pulsar-2.5.0
bin/pulsar standalone
```

> #### Tip
> For more information about how to set up a standalone Pulsar locally, see [here](https://pulsar.apache.org/docs/en/next/standalone/).

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
    > bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic test
    This is a message
    This is another message
    ```

    Kafka has a command-line consumer dumping out messages to standard output.

    ```
    > bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic test --from-beginning
    This is a message
    This is another message
    ```

# Configure

You can configure the following properties for KoP.

## Log level 

In Pulsar's [log4j2.yaml config file](https://github.com/apache/pulsar/blob/master/conf/log4j2.yaml), 
you can set KoP's log level.

**Example**

```
Logger:
    - name: io.streamnative.pulsar.handlers.kop
    level: debug
    additivity: false
    AppenderRef:
        - ref: Console
``` 

## Secure

### SSL connection

KoP supports the following configuration types for Kafka listeners:

- PLAINTEXT  
  
- SSL

**Example**

```
listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093
```

> #### Tip
> For how to configure SSL keys, see [Kafka SSL](https://kafka.apache.org/documentation/#security_ssl). 

The following example shows how to connect KoP through SSL.

1. Create SSL related keys.

    This example creates related CA and jks files.

    ```
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

2. Configure KoP broker.

    In Pulsar configuration file (`broker.conf` or `standalone.conf`), add the related configurations that using the jks configs created in step1:

    ```
    listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093

    kopSslKeystoreLocation=/Users/kop/server.keystore.jks
    kopSslKeystorePassword=test1234
    kopSslKeyPassword=test1234
    kopSslTruststoreLocation=/Users/kop/server.truststore.jks
    kopSslTruststorePassword=test1234
    ```

3. Configure Kafka client.

    (1) Prepare a file named `client-ssl.properties` containing the following information.

    ```
    security.protocol=SSL
    ssl.truststore.location=client.truststore.jks
    ssl.truststore.password=test1234
    ssl.endpoint.identification.algorithm=
    ```

    (2) Verify console-producer and console-consumer.

    ```
    kafka-console-producer.sh --broker-list localhost:9093 --topic test --producer.config client-ssl.properties
    kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic test --consumer.config client-ssl.properties
    ```

    > #### Tip
    > For more information, see [configure Kafka client](https://kafka.apache.org/documentation/#security_configclients).

### KoP authentication

You can enable both authentication and authorization on KoP, they use the underlying Pulsar [token based authentication](http://pulsar.apache.org/docs/en/security-jwt/) mechanisms.

> #### Tip
> For more information about Kafka authentication, see [Kafka security documentation](https://kafka.apache.org/documentation/#security_sasl). 

To forward your credentials, `SASL-PLAIN` is used on the Kafka client side. The two important settings are `username and `password`:

* The `username` of Kafka JAAS is the `tenant/namespace`, in which Kafka’s topics are stored in Pulsar. 
For example, `public/default`.  

* The `password` must be your token authentication parameters from Pulsar. For example, `token:xxx`.

    The token can be created by [Pulsar tokens tools](http://pulsar.apache.org/docs/en/security-jwt/#generate-tokens). The [role](http://pulsar.apache.org/docs/en/security-overview/#role-tokens) is the `subject` for token, it is embedded in the created token, and the broker can get `role` by parsing this token.


#### Enable authentication on Pulsar broker

To enable KoP authentication, you need to set all the options required by [Pulsar token based authentication](http://pulsar.apache.org/docs/en/security-jwt/) and set `saslAllowedMechanisms` (set it to`PLAIN`) in Pulsar configuration file (`broker.conf` or `standalone.conf`).

```
saslAllowedMechanisms=PLAIN

# Configuration to enable authentication and authorization
authenticationEnabled=true
authorizationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken

# If using secret key
tokenSecretKey=file:///path/to/secret.key
```

#### Enable authentication on Kafka client

You can use the following code to enable SASL-PLAIN through jaas.

```java
String tenant = "public/default";
String pasword = "token:xxx";

String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
String jaasCfg = String.format(jaasTemplate, tenant, password);
props.put("sasl.jaas.config", jaasCfg);
props.put("security.protocol", "SASL_PLAINTEXT");
props.put("sasl.mechanism", "PLAIN");
```

Kafka consumers and Kafka producers can use the props to connect to brokers.


## Limitations for KoP

KoP leverage Pulsar features, but some of the manners between Pulsar and Kafka are different. In this implementation, there are some limitations.

- KoP does not support Pulsar non-partitioned topic. Because all topics in Kafka are partitioned type, not support non-partitioned topic is easy to align this.
- All topics in KoP are placed under a user specified tenant and namespace. 

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

# Kafka ssl configuration map with: SSL_CLIENT_AUTH_CONFIG = "ssl.client.auth"
kopSslClientAuth=

# supported SASL mechanisms exposed by broker
saslAllowedMechanisms=
```
