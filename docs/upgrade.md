# Upgrade

This document describes how to upgrade KoP.

## Upgrade Pulsar

KoP leverages many classes from Pulsar. However, these classes might be modified after Pulsar is upgraded. Therefore, you should upgrade Pulsar first.

Usually, KoP `x.y.z.m` is compatible with Apache Pulsar `x.y.z`. For example, you can either run KoP 2.8.1.19 or 2.8.1.0 on Apache Pulsar 2.8.1.

The exception cases are KoP 2.8.0.13, 2.8.0.14, 2.8.0.15 and 2.8.0.16. They're incompatible with Apache Pulsar 2.8.0, you can use [StreamNative Pulsar 2.8.0.13](https://github.com/streamnative/pulsar/releases/tag/v2.8.0.13) for these versions. See [KoP-768](https://github.com/streamnative/kop/issues/768) for details.

## For KoP < 2.8.0

There is a breaking change introduced in KoP 2.8.0. The following configuration must be configured for KoP 2.8.0 or higher.

```properties
brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor
```

Before 2.8.0, KoP performed a conversion between Pulsar's `MessageId` and Kafka's offset. However, there are many serious problems for this design. The most serious problem is that the offset is not continuous. Here is an example.

- Given a topic that has 2 messages,
  - The first message's offset should be 0. But it could also be 175921860444160 or another possible value.
  - If the current message's offset is `N`, the next message's offset should be `N+1`. But it could also be `N+4096` or `N+17592186044416` or another possible value.

Since 2.8.0, based on [PIP 70](https://github.com/apache/pulsar/wiki/PIP-70%3A-Introduce-lightweight-broker-entry-metadata), KoP could implement the continuous offset via `BrokerEntryMetadata` (an extra part of a BookKeeper Entry), which represents a batched message. For messages produced by KoP (< 2.8.0), these entries do not contain a `BrokerEntryMetadata`, so KoP (2.8.0 or higher) cannot recognize these messages. Another problem is that the offset topic (`__consumer_offsets`) stores metadata related to committed offsets. If these offsets were committed by KoP (< 2.8.0), the offsets cannot be recognized by KoP 2.8.0.

Therefore, before upgrading KoP (< 2.8.0) to KoP (2.8.0 or higher), you need to deal with the existing messages using one of the following ways:

- To delete the existing messages, perform these operations:

  - Delete the `__consumer_offsets` topic.
  - Do not access the topics that are produced from KoP (< 2.8.0), or just delete them.

- To skip the existing messages, add the following configuration:

  ```properties
  skipMessagesWithoutIndex=true
  ```
