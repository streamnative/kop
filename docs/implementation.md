# Implementation

This documents describes how to implement the KoP protocol handler.

## Topic

In Kafka, all topics are stored in one flat namespace. But in Pulsar, topics are organized in hierarchical multi-tenant namespaces. KoP introduces a setting `kafkaNamespace` in the broker configuration, which allows the administrator to map Kafka topics to Pulsar topics.

To let Kafka users leverage the multi-tenancy feature of Apache Pulsar, a Kafka user can specify a Pulsar tenant and namespace as the topic prefix like Pulsar topics:

| Kafka topic name | tenant | namespace | short topic name |
| :--------------- | :----- | :-------- | :--------------- |
| my-topic | `<kafkaTenant>` | `<kafkaNamespace>` | my-topic |
| my-tenant/my-ns/my-topic | my-tenant | my-ns | my-topic |
| persistent://my-tenant/my-ns/my-topic | my-tenant | my-ns | my-topic |

## Topic lookup

KoP uses the same topic lookup approach for the Kafka request handler and the Pulsar request handler. The request handler does topic discovery to look up all the ownerships for the requested topic partitions and responds with the ownership information as part of Kafka `TopicMetadata` back to Kafka clients.

## Message

Both a Kafka message and a Pulsar message have the key, value, timestamp, and
headers. (In Pulsar，The `headers` is called `properties`.) KoP converts these
fields automatically between Kafka messages and Pulsar messages. The mapping relationships are as below.

### Mapping relationship of converting message fields **from Pulsar to Kafka**

Pulsar | Kafka | Note
|---|---|---
`Ordering key` <br><br> `Partition key`  | `Key` | **Ordering key has higher priority**, which means:<br><br> - If a Pulsar message has an `ordering key`, the `ordering key` is converted to a `key`.<br><br> - If a Pulsar message does not have an `ordering key`, the `partition key` is converted to a `key`. <br><br> For example, <br><br> - If a Pulsar message has an `ordering key (xxx)` and a `partition key (yyy)`, the message key of the converted Kafka message is `xxx` rather than `yyy`.<br><br> - If a Pulsar message does not have an `ordering key` but have a `partition key (yyy)`, the message key of the converted Kafka message is `yyy`.
`Event time` <br><br> `Publish time` | `Timestamp` |  **Event time has higher priority**, which means: <br><br>- If a Pulsar message has an `event time`, the `event time` is converted to `timestamp`.<br><br>- If a Pulsar message does not have an `event time`, the `publish time` is converted to `timestamp`. <br><br>For example, <br><br> - If a Pulsar message has an `event time (1628826964820)` and a `publish time (1628826964821)`, the message key of the converted Kafka message is `1628826964820` rather than `1628826964821`.<br><br> - If a Pulsar message does not have an `event time` but has a `publish time (1628826964821)`, the message key of the converted Kafka message is `1628826964821`.
NULL value| NULL value |If the field of a Pulsar message is NULL, the converted Kafka message is NULL.

### Mapping relationship of converting message fields **from Kafka to Pulsar**

Kafka|Pulsar|Note
|---|---|---
`Key`|Ordering key and partition key |If a Kafka message has a `key`, both  `ordering key` and `partition key` are set for the converted Pulsar message.<br><br> If a Kafka message does not have a `key`, both `ordering key` and `partition key` are **not set** for the converted Pulsar message.
`Timestamp`|Event time and publish time |If a Kafka message has a `timestamp`, both  `event time` and `publish time` are set for the converted Pulsar message.<br><br> If a Kafka message does not have a `timestamp`, both `event time` and `publish time` are **not set** for the converted Pulsar message.
NULL value|NULL value| If the field of a Kafka message is NULL, the converted Pulsar message is NULL.

## Message ID and offset

In Kafka, each message is assigned with an offset once the message is successfully produced to a topic partition. In Pulsar, each message is assigned with a `MessageID`. The message ID consists of `ledger-id`, `entry-id`, and `batch-index` components. KoP uses the same approach in Pulsar-Kafka wrapper to convert a Pulsar `MessageID` to an offset and vice versa.

## Produce Messages

When the Kafka request handler receives produced messages from a Kafka client, it converts Kafka messages to Pulsar messages by mapping the fields (such as the key, value, timestamp and headers) one by one, and uses the ManagedLedger append API to append those converted Pulsar messages to BookKeeper. Converting Kafka messages to Pulsar messages allows existing Pulsar applications to consume messages produced by Kafka clients.

## Consume Messages

When the Kafka request handler receives a consumer request from a Kafka client, it opens a non-durable cursor to read the entries starting from the requested offset. The Kafka request handler converts the Pulsar messages back to Kafka messages to allow existing Kafka applications to consume the messages produced by Pulsar clients.

## Group coordinator & offset management

The most challenging part is to implement the group coordinator and offset management. Pulsar does not have a centralized group coordinator for assigning partitions to consumers of a consumer group or managing offsets for each consumer group. In Pulsar, the partition assignment is managed by the broker on a per-partition basis, and the offset management is done by storing the acknowledgements in cursors by the owner broker of that partition.

It is difficult to align the Pulsar model with the Kafka model. Therefore, to be fully compatible with Kafka clients, KoP implements the Kafka group coordinator by storing the coordinator group changes and offsets in a system topic called `public/__kafka/__consumer_offsets` in Pulsar. 

This bridges the gap between Pulsar and Kafka and allows people to use existing Pulsar tools and policies to manage subscriptions and monitor Kafka consumers. KoP adds a background thread in the implemented group coordinator to periodically synchronize offset updates from the system topic to Pulsar cursors. Therefore, a Kafka consumer group is effectively treated as a Pulsar subscription. All the existing Pulsar tools can be used for managing Kafka consumer groups as well.
