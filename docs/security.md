# Security

KoP supports authentication and SSL connection.

## Authentication

By default, KoP is installed with no encryption, authentication, and authorization. You can enable the authentication feature for KoP to improve security. Currently, this feature is **only available in Java**.

KoP authentication mechanism uses [Kafka SASL mechanisms](https://docs.confluent.io/platform/current/kafka/overview-authentication-methods.html) and achieves authentication with [Pulsar token-based authentication mechanism](https://pulsar.apache.org/docs/en/security-overview/). Consequently, if you want to enable the authentication feature for KoP, you need to enable authentication for the following components:

- Pulsar brokers

- KoP (some configurations of KoP rely on the configurations of Pulsar brokers)

- Kafka clients

Currently, KoP supports the following SASL mechanisms:

- [`PLAIN`](https://docs.confluent.io/platform/current/kafka/authentication_sasl/authentication_sasl_plain.html#kafka-sasl-auth-plain)

- [`OAUTHBEARER`](https://docs.confluent.io/platform/current/kafka/authentication_sasl/authentication_sasl_oauth.html#kafka-oauth-auth)

### `PLAIN`

If you want to enable the authentication feature for KoP using the `PLAIN` mechanism, follow the steps below.

1. Enable authentication on Pulsar broker.

    For the `PLAIN` mechanism, the Kafka authentication is forwarded to the [JWT authentication](https://pulsar.apache.org/docs/en/security-jwt/) of Pulsar, so you need to configure the JWT authentication and set the following properties in the `conf/broker.conf` or `conf/standalone.conf` file.

    (1) Enable authentication for the Pulsar broker.

    ```properties
    authenticationEnabled=true
    authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken
    ```

    (2) Enable authentication between Pulsar broker and KoP.

    ```properties
    brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
    brokerClientAuthenticationParameters=token:<token-of-super-user-role>
    superUserRoles=<super-user-roles>
    ```

    (3) Specify the key.

    For more information, see [Enable Token Authentication on Brokers](https://pulsar.apache.org/docs/en/next/security-jwt/#enable-token-authentication-on-brokers).

    - If you use a secret key, set the property as below.

        ```properties
        tokenSecretKey=file:///path/to/secret.key
        ```

        The key can also be passed inline.

        ```properties
        tokenSecretKey=data:;base64,FLFyW0oLJ2Fi22KKCm21J18mbAdztfSHN/lAT5ucEKU=
        ```

   - If you use a public/private key, set the property as below.

        ```properties
        tokenPublicKey=file:///path/to/public.key
        ```

2. Enable authentication on KoP.

    Set the following property in the `conf/broker.conf` or `conf/standalone.conf` file.

    ```properties
    saslAllowedMechanisms=PLAIN
    ```

3. Enable authentication on Kafka client.

    To forward your credentials, `SASL-PLAIN` is used on the Kafka client side. To enable `SASL-PLAIN`, you need to set the following properties through Kafka JAAS.

    Property | Description | Example value
    |---|---|---
    `username` | `username` | The `username` of Kafka JAAS is `tenant/namespace` or `tenant`, where Kafka’s topics are stored in Pulsar. <br><br> **Note** In KoP 2.9.0 or higher, the username can be used with `kafkaEnableMultiTenantMetadata` to implement multi-tenancy for metadata. | empty string
    `password`|`password` must be your token authentication parameters from Pulsar.<br><br>The token can be created by Pulsar token tools. The role is the `subject` for the token. It is embedded in the created token and the broker can get `role` by parsing this token.|`token:xxx`

    ```properties
    security.protocol=SASL_PLAINTEXT  # or security.protocol=SASL_SSL if SSL connection is used
    sasl.mechanism=PLAIN
    sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule \
    required username="public/default" password="token:xxx";
    ```

### `OAUTHBEARER`

If you want to enable the authentication feature for KoP using the `OAUTHBEARER` mechanism, follow the steps below.

1. Enable authentication on Pulsar broker.

    Set the following properties in the `conf/broker.conf` or `conf/standalone.conf` file.

    The properties here are same to that of the `PLAIN` mechanism except `brokerClientAuthenticationPlugin` and `brokerClientAuthenticationParameters`.

    |Property|Description|Required or optional|Example value
    |---|---|---|---
    | `type` | Oauth 2.0 authentication type <br><br> The **default** value is `client_credentials`| Optional | `client_credentials`  |
    | `privateKey` | URL to a JSON credential file <br><br>The following pattern formats are supported:<br> - `file:///path/to/file` <br> - `file:/path/to/file` <br> - `data:application/json;base64,<base64-encoded value>` |   Required |file:///path/to/credentials_file.json
    | `issuerUrl` | URL of the authentication provider which allows the Pulsar client to obtain an access token | Required | `https://accounts.google.com` |
    | `audience`  | An OAuth 2.0 "resource server" identifier for the Pulsar cluster | Required |`https://broker.example.com` |


    ```properties
    authenticationEnabled=true
    authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken
    superUserRoles=<super-user-roles>
    brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2
    brokerClientAuthenticationParameters={"type":"client_credentials","privateKey":"file:///path/to/credentials_file.json","issuerUrl":"<issuer-url>","audience":"<audience>"}
    tokenPublicKey=<token-public-key>
    ```

2. Enable authentication on KoP.

    (1) Set the following property in the `conf/broker.conf` or `conf/standalone.conf` file.

    ```properties
    saslAllowedMechanisms=OAUTHBEARER
    ```

    (2) Specify the Kafka server callback handler.

    For the `OAUTHBEARER` mechanism, you can use `AuthenticationProviderToken` or custom your authentication provider to process the access tokens from OAuth 2.0 server.

    KoP provides a built-in `AuthenticateCallbackHandler` that uses the authentication provider of Pulsar for authentication. You need to configure the following properties in the `conf/kop-handler.properties` file.

    ```properties
    # Use the KoP's built-in handler
    kopOauth2AuthenticateCallbackHandler=io.streamnative.pulsar.handlers.kop.security.oauth.OauthValidatorCallbackHandler

    # Java property configuration file of OauthValidatorCallbackHandler
    kopOauth2ConfigFile=conf/kop-handler.properties
    ```

(3) Specify the authentication method name of the provider (that is, `oauth.validate.method`) in the `conf/kop-handler.properties` file.

   - If you use `AuthenticationProviderToken`, since `AuthenticationProviderToken#getAuthMethodName()` returns `token`, set the `oauth.validate.method` as the token.

   - If you use other providers, set the `oauth.validate.method` as the result of `getAuthMethodName()`.

        ```properties
        oauth.validate.method=token
        ```

3. Enable authentication on Kafka client.

    (1) Install the KoP built-in callback handler to your local Maven repository.

    To get an access token from an OAuth 2.0 server, you need to use the KoP built-in callback handler instead of the Kafka login callback handler.

    ```bash
    mvn clean install -pl oauth-client -DskipTests
    ```

    (2) Add the following dependencies to the `pom.xml` file.

    For stable releases, the `pulsar.version` is the same to the `kop.version`.

    ```xml
    <dependency>
      <groupId>io.streamnative.pulsar.handlers</groupId>
      <artifactId>oauth-client</artifactId>
      <version>${kop.version}</version>
      <exclusions>
        <exclusion>
          <groupId>org.apache.logging.log4j</groupId>
          <artifactId>log4j-slf4j-impl</artifactId>
        </exclusion>
      </exclusions>
    </dependency>

    <!-- KoP's login callback handler has a pulsar-client dependency -->
    <dependency>
      <groupId>org.apache.pulsar</groupId>
      <artifactId>pulsar-client</artifactId>
      <version>${pulsar.version}</version>
    </dependency>
    ```

    (3) Configure the producer or consumer with the following **required** properties.

    <table>
        <thead>
            <tr>
                <th>Property</th>
                <th>Description</th>
                <th>Example value</th>
                <th>Note</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><code>sasl.login.callback.handler.class<code></td>
                <td>Class of SASL login callback handler</td>
                <td>io.streamnative.pulsar.handlers.kop.security.oauth.OauthLoginCallbackHandler</td>
                <td rowspan=4>Set the value of this property as the same to the value in the example below.</td>
            </tr>
            <tr>
                <td><code>security.protocol<code></td>
                <td>Security protocol</td>
                <td>SASL_PLAINTEXT</td>
                <td></td>
            </tr>
            <tr>
                <td><code>sasl.mechanism<code></td>
                <td>SASL mechanism</td>
                <td>OAUTHBEARER</td>
                <td></td>
            </tr>
            <tr>
                <td><code>sasl.jaas.config<code></td>
                <td>JAAS configuration</td>
                <td>org.apache.kafka.coPropertymmon.security.oauthbearer.OAuthBearerLoginModule</td>
                <td></td>
            </tr>
            <tr>
                <td><code>oauth.issuer.url<code></td>
                <td>URL of the authentication provider which allows the Pulsar client to obtain an access token.
                </td>
                <td>https://accounts.google.com</td>
                <td>This property is the same to the issuerUrl property in <a href="http://pulsar.apache.org/docs/en/security-oauth2/#authentication-types">Pulsar client credentials</a></td>
            </tr>
            <tr>
                <td><code>oauth.credentials.url<code></td>
                <td>URL to a JSON credentials file.<br><br>The following pattern formats are supported:<br> - file:///path/to/file <br> - file:/path/to/file <br> - data:application/json;base64,<base64-encoded value>
                </td>
                <td>file:///path/to/credentials_file.json</td>
                <td>This property is the same to the privateKey property in <a href="http://pulsar.apache.org/docs/en/security-oauth2/#authentication-types">Pulsar client credentials</a></td>
            </tr>
            <tr>
                <td><code>oauth.audience<code></td>
                <td>OAuth 2.0 "resource server" identifier for the Pulsar cluster.
                </td>
                <td>https://broker.example.com</td>
                <td>This property is the same to the audience property in <a href="http://pulsar.apache.org/docs/en/security-oauth2/#authentication-types">Pulsar client credentials</a></td>
            </tr>
        </tbody>
    </table>

```properties
sasl.login.callback.handler.class=io.streamnative.pulsar.handlers.kop.security.oauth.OauthLoginCallbackHandler
security.protocol=SASL_PLAINTEXT  # or security.protocol=SASL_SSL if SSL connection is used
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule \
   required oauth.issuer.url="https://accounts.google.com"\
   oauth.credentials.url="file:///path/to/credentials_file.json"\
   oauth.audience="https://broker.example.com";
```

## Authorization
To enable authorization on KoP, please make sure the authentication is enabled.

**Note**: For more information, see [Authorization](http://pulsar.apache.org/docs/en/security-jwt/#authorization).

1. Enable authorization and assign superusers for the Pulsar broker.

   ```properties
   authorizationEnabled=true
   ```
   
2. Generate JWT tokens.

    A token is the credential associated with a user. The association is done through the "`principal`" or "`role`". In the case of JWT tokens, this field is typically referred as `subject`, though they are exactly the same concept.
    Then, you need to use this command to require the generated token to have a `subject` field set.

    ```shell
    $ bin/pulsar tokens create --secret-key file:///path/to/secret.key \
         --subject <user-role>
    ```
   This command prints the token string on stdout.

3. Grant permission to specific role.

   The token itself does not have any permission associated. The authorization engine determines whether the token should have permissions or not. Once you have created the token, you can grant permission for this token to do certain actions. <br/>The following is an example.
      
   ```shell
   $ bin/pulsar-admin --auth-plugin "org.apache.pulsar.client.impl.auth.AuthenticationToken" --auth-params "token:<token-of-super-user-role>" \
            namespaces grant-permission <tenant>/<namespace> \
            --role <user-role> \
            --actions produce,consume
   ```

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

    ```bash
    # You need to input a password, for example "server-keystore".
    keytool -keystore server.keystore.jks -alias localhost -validity 365 -keyalg RSA -genkey
    # You need to input a password, for example "server".
    openssl req -new -x509 -keyout ca-key -out ca-cert -days 365
    # You need to input a password, for example "server-truststore"
    keytool -keystore server.truststore.jks -alias CARoot -import -file ca-cert
    # You need to input a password, for example "client-truststore"
    keytool -keystore client.truststore.jks -alias CARoot -import -file ca-cert
    # You must input the password of server.keystore.jks: "server-keystore"
    keytool -keystore server.keystore.jks -alias localhost -certreq -file cert-file
    # NOTE: the password followed by `-passin pass:` is the password of ca-cert: "server"
    openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days 365 -CAcreateserial -passin pass:server
    # You must input the password of server.keystore.jks: "server-keystore"
    keytool -keystore server.keystore.jks -alias CARoot -import -file ca-cert
    # You must input the password of server.keystore.jks: "server-keystore"
    keytool -keystore server.keystore.jks -alias localhost -import -file cert-signed
    ```

    In above example, we have input four passwords:

    - `server-keystore` for `server.keystore.jks` file
    - `server` for `ca-cert` and `ca-key` files
    - `server-truststore` for `server.truststore.jks` file
    - `client-truststore` for `client.truststore.jks` file

2. Configure the KoP broker.

    In the StreamNative Platform configuration file (`${PLATFORM_HOME}/etc/pulsar/broker.conf` or `${PLATFORM_HOME}/etc/pulsar/standalone.conf`), add the related configurations that using the jks configurations created in Step 1:

    ```shell
    listeners=PLAINTEXT://localhost:9092,SSL://localhost:9093

    # You need to use the full path of server.keystore.jks
    kopSslKeystoreLocation=server.keystore.jks
    kopSslKeystorePassword=server-keystore
    kopSslKeyPassword=server-keystore
    # You need to use the full path of server.truststore.jks
    kopSslTruststoreLocation=server.truststore.jks
    kopSslTruststorePassword=server-truststore
    ```

3. Configure the Kafka client.

    (1) Prepare a file named `client-ssl.properties`. The file contains the following information.

    ```shell
    security.protocol=SSL
    # You need to use the full path of client.truststore.jks
    ssl.truststore.location=client.truststore.jks
    ssl.truststore.password=client-truststore
    # The identification algorithm must be empty
    ssl.endpoint.identification.algorithm=
    ```

    (2) Verify the console-producer and the console-consumer.

    ```shell
    kafka-console-producer.sh --broker-list localhost:9093 --topic test --producer.config client-ssl.properties
    kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic test --consumer.config client-ssl.properties
    ```

    > **Tip**
    > For more information, see [Configure Kafka client](https://kafka.apache.org/documentation/#security_configclients).
