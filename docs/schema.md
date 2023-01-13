# Schema Registry

KoP has implemented with the Schema Registry to support Kafka clients with [Confluent's Schema serializers and deserializers](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#supported-formats).

To enable the Schema Registry, you should add the following configuration:

```properties
kopSchemaRegistryEnable=true
```

By default, the Schema Registry listens on port 8001, which means you should add the following property to create your Kafka producer or consumer.

```properties
schema.registry.url=http://<broker-ip>:8001
```

> **Note**
>
> For other configurations, such as how to change the port where the Schema Registry listens on, see the [configuration guide](./configuration.md).

To manage the schemas, see the [Confluent's Schema REST API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html#compatibility).

## Example: Use KoP Schema Registry with Confluent's Avro serializer

This section provides an example about how to use Confluent's Avro serializer on KoP.

### Enable the Schema Registry on KoP

Start KoP with the following properties in standalone mode. For details, see [Set configuration for KoP](./kop.md#set-configuration-for-kop).

```properties
messagingProtocols=kafka
kafkaListeners=PLAINTEXT://0.0.0.0:9092
brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor
brokerDeleteInactiveTopicsEnabled=false
allowAutoTopicCreationType=partitioned

# Enable the transaction to be compatible with Kafka clients 3.2.0 or later
# See https://kafka.apache.org/documentation/#upgrade_320_notable
kafkaTransactionCoordinatorEnabled=true
brokerDeduplicationEnabled=true

kopSchemaRegistryEnable=true
```

### Define the Avro schema

Create an Avro schema file named `User.asvc` under the `src/main/avro` directory:

```json
{
  "namespace": "example.avro",
  "type": "record",
  "name": "User1",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"}
  ]
}
```

Add the `avro-maven-plugin` plugin to generate the Java class from the Avro schema file above.

```xml
      <plugin>
        <groupId>org.apache.avro</groupId>
        <artifactId>avro-maven-plugin</artifactId>
        <version>1.10.2</version>
        <executions>
          <execution>
            <phase>generate-sources</phase>
            <goals>
              <goal>schema</goal>
            </goals>
            <configuration>
              <sourceDirectory>${project.basedir}/src/main/avro</sourceDirectory>
              <includes>
                <include>User.avsc</include>
              </includes>
              <outputDirectory>${project.build.directory}/generated-sources</outputDirectory>
            </configuration>
          </execution>
        </executions>
      </plugin>
```

With the Maven plugin above, a `User` class that has a `String` field named `name` and an `int` field named `age` will be generated under the `example.avro` package. 

### Produce and consume messages with Confluent's Avro serializer

1. Add the following dependencies to use Kafka client 3.3.1 with Confluent's Avro serializer 7.3.1:

```xml
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-clients</artifactId>
      <version>3.3.1</version>
    </dependency>

    <dependency>
      <groupId>io.confluent</groupId>
      <artifactId>kafka-avro-serializer</artifactId>
      <version>7.3.1</version>
    </dependency>
```

2. Run the following producer application code:

```java
final Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8001");

final String topic = "my-avro-kafka-topic";
final KafkaProducer<String, User> producer = new KafkaProducer<>(props);
RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, new User("alice", 10))).get();
System.out.println("Sent to " + metadata);
producer.close();
```

You will see the following output:

```
Sent to my-avro-kafka-topic-0@0
```

3. Run the following consumer application:

```java
final Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8001");

final KafkaConsumer<String, User> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Collections.singleton("my-avro-kafka-topic"));
while (true) {
    ConsumerRecords<String, User> records = consumer.poll(Duration.ofSeconds(1));
    records.forEach(record -> System.out.println("Received " + record.value() + " from "
            + record.topic() + "-" + record.partition() + "@" + record.offset()));
    if (!records.isEmpty()) break;
}
consumer.close();
```

You will see the following output:

```
Received {"name": "alice", "age": 10} from my-avro-kafka-topic-0@0
```

### Query the created schema

Query the schema whose ID is 1 (i.e. the 1st schema):

```bash
$ curl -L http://localhost:8001/schemas/ids/1; echo
{
  "schema" : "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}"
}
```

Query the subjects:

```bash
$ curl -L http://localhost:8001/subjects; echo
[ "my-avro-kafka-topic-value" ]
$ curl -L http://localhost:8001/subjects/my-avro-kafka-topic-value/versions; echo
[ 1 ]
$ curl -L http://localhost:8001/subjects/my-avro-kafka-topic-value/versions/1; echo
{
  "id" : 1,
  "schema" : "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"example.avro\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}",
  "subject" : "my-avro-kafka-topic-value",
  "version" : 1,
  "type" : "AVRO"
}
```

For a quick start of Confluent's Schema concepts, See [Confluent documentation](https://docs.confluent.io/platform/current/schema-registry/schema_registry_tutorial.html#terminology-review) .
