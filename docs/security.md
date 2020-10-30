# Security

## KoP authentication

> **Tip**
> For more details about Kafka authentication, see [Kafka security documentation](https://kafka.apache.org/documentation/#security_sasl).

To forward your credentials, `SASL-PLAIN` is used on the Kafka client side. The two important settings are `username` and `password`:

* The `username` of Kafka JAAS is the `tenant/namespace`, in which Kafka’s topics are stored in Pulsar.

    For example, `public/default`.

* The `password` must be your token authentication parameters from Pulsar.

    For example, `token:xxx`.

    The token can be created by Pulsar tokens tools. The role is the `subject` for token. It is embedded in the created token, and the broker can get `role` by parsing this token.

## Enable authentication on Pulsar broker

To enable KoP authentication, you need to set all the options required by the Pulsar token based authentication and set `saslAllowedMechanisms` (set it to`PLAIN`). The Kafka authentication is forwarded to Pulsar's JWT (Json Web Token) authentication, so you also need to configure the [JWT authentication](https://pulsar.apache.org/docs/en/security-jwt/).

```properties
saslAllowedMechanisms=PLAIN

# Configuration to enable authentication and authorization
authenticationEnabled=true
authorizationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken

# Configuration to enable authentication between KoP and Pulsar broker
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
brokerClientAuthenticationParameters=token:<token-of-super-user-role>
superUserRoles=<super-user-roles>

# If using secret key
tokenSecretKey=file:///path/to/secret.key
```

## Enable authentication on Kafka client

You can use the following code to enable SASL-PLAIN through jaas.

```java
String tenant = "public/default";
String password = "token:xxx";

String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
String jaasCfg = String.format(jaasTemplate, tenant, password);
props.put("sasl.jaas.config", jaasCfg);

props.put("security.protocol", "SASL_PLAINTEXT");
props.put("sasl.mechanism", "PLAIN");
```

Kafka consumers and Kafka producers can use the props to connect to brokers.

## SSL connection

KoP supports the following configuration types for Kafka listeners:
- PLAINTEXT
- SSL

**Example**

```shell
listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093
```

> **Tip**
> For how to configure SSL keys, see [Kafka SSL](https://kafka.apache.org/documentation/#security_ssl).

The following example shows how to connect KoP through SSL.

1. Create SSL related keys.

    This example creates the related CA and JKS files.

    ```shell
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

2. Configure the KoP broker.

    In the StreamNative Platform configuration file (`${PLATFORM_HOME}/etc/pulsar/broker.conf` or `${PLATFORM_HOME}/etc/pulsar/standalone.conf`), add the related configurations that using the jks configurations created in Step 1:

    ```shell
    listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093

    kopSslKeystoreLocation=/Users/kop/server.keystore.jks
    kopSslKeystorePassword=test1234
    kopSslKeyPassword=test1234
    kopSslTruststoreLocation=/Users/kop/server.truststore.jks
    kopSslTruststorePassword=test1234
    ```

3. Configure the Kafka client.

    (1) Prepare a file named `client-ssl.properties`. The file contains the following information.

    ```shell
    security.protocol=SSL
    ssl.truststore.location=client.truststore.jks
    ssl.truststore.password=test1234
    ssl.endpoint.identification.algorithm=
    ```

    (2) Verify the console-producer and the console-consumer.

    ```shell
    kafka-console-producer.sh --broker-list localhost:9093 --topic test --producer.config client-ssl.properties
    kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic test --consumer.config client-ssl.properties
    ```

    > **Tip**
    > For more information, see [Configure Kafka client](https://kafka.apache.org/documentation/#security_configclients).

