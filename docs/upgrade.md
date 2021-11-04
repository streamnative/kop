# Upgrade

This document describes what should be noted when upgrading KoP.

## Upgrade Pulsar

KoP makes use of many classes from Pulsar. However, these classes might be modified after a Pulsar upgrade. So you should upgrade Pulsar first.

Usually, KoP `x.y.z.m` is compatible with Apache Pulsar `x.y.z`. For example, you can both run KoP 2.8.1.19 or 2.8.1.0 on Apache Pulsar 2.8.1.

The exception cases are KoP 2.8.0.13, 2.8.0.14, 2.8.0.15 and 2.8.0.16. They're incompatible with Apache Pulsar 2.8.0, you can use [StreamNative Pulsar 2.8.0.13](https://github.com/streamnative/pulsar/releases/tag/v2.8.0.13) for these versions. See [KoP-768](https://github.com/streamnative/kop/issues/768) for details.

## For KoP < 2.8.0

There's a breaking change introduced from KoP 2.8.0 that the following configuration must be configured.

```properties
brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor
```

Before 2.8.0, KoP performed a conversion between Pulsar's `MessageId` and Kafka's offset. However, there're many serious problems for this design. The most serious problem is that the offset is not continuous. There's no need to explain it in detail, just give an example.

- Given a topic that has 2 messages,
  - The first message's offset should be 0. But it could also be 175921860444160 or another possible value.
  - If the current message's offset is `N`, the next message's offset should be `N+1`. But it could also be `N+4096` or `N+17592186044416` or another possible value.

Since 2.8.0, benefit from [PIP 70](https://github.com/apache/pulsar/wiki/PIP-70%3A-Introduce-lightweight-broker-entry-metadata), KoP could implement the continuous offset via `BrokerEntryMetadata`, which is an extra part of a BookKeeper Entry, which represents a batched message. For messages produced by KoP < 2.8.0, these entries don't contain a `BrokerEntryMetadata`, so KoP >= 2.8.0 cannot recognize these messages. Another problem is the offset topic (`__consumer_offsets`) stores metadata related to committed offsets. If these offsets were committed by KoP < 2.8.0, the offsets cannot be recognized by KoP 2.8.0.

Therefore, the upgrade from KoP < 2.8.0 to KoP >= 2.8.0 is hard. You must:

- Delete the `__consumer_offsets` topic.
- Don't access the topics that are produced from KoP < 2.8.0, or just delete them.

