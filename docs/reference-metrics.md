---
id: reference-metrics
title: KoP Metrics
sidebar_label: KoP Metrics
---

<style type="text/css">
  table{
    font-size: 80%;
  }
</style>

KoP exposes the following metrics in Prometheus format. You can monitor your clusters with those metrics.

* [KoP](#KoP)

The following types of metrics are available:

- [Counter](https://prometheus.io/docs/concepts/metric_types/#counter): a cumulative metric that represents a single monotonically increasing counter. The value increases by default. You can reset the value to zero or restart your cluster.
- [Gauge](https://prometheus.io/docs/concepts/metric_types/#gauge): a metric that represents a single numerical value that can arbitrarily go up and down.
- [Histogram](https://prometheus.io/docs/concepts/metric_types/#histogram): a histogram samples observations (usually things like request durations or response sizes) and counts them in configurable buckets.
- [Summary](https://prometheus.io/docs/concepts/metric_types/#summary): similar to a histogram, a summary samples observations (usually things like request durations and response sizes). While it also provides a total count of observations and a sum of all observed values, it calculates configurable quantiles over a sliding time window.

## KoP metrics

The KoP metrics are exposed under "/metrics" at port `8000` along with Pulsar metrics. You can use a different port by configuring the `stats_server_port` system property.

### Request metrics

| Name | Type | Description |
|---|---|---|
| kop_server_ALIVE_CHANNEL_COUNT | Gauge | The number of alive request channel |
| kop_server_ACTIVE_CHANNEL_COUNT | Gauge | The number of active request channel |

### Request metrics

| Name | Type | Description |
|---|---|---|
| kop_server_REQUEST_QUEUE_SIZE | Gauge | The number of quest in kop request processing queue of total request channel. |
| kop_server_REQUEST_QUEUED_LATENCY | Summary | The requests queued latency calculated in milliseconds. <br> Available labels: *request* (ApiVersions, Metadata, Produce, FindCoordinator, ListOffsets, OffsetFetch, OffsetCommit, Fetch, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, DescribeGroups, ListGroups, DeleteGroups, SaslHandshake, SaslAuthenticate, CreateTopics, InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn, WriteTxnMarkers, DescribeConfigs, DeleteTopics). </br>|
| kop_server_REQUEST_PARSE_LATENCY | Summary | The requests parse latency from byteBuf to MemoryRecords calculated in milliseconds. |
| kop_server_REQUEST_LATENCY | Summary | The requests processing total latency for all Kafka Apis. <br> Available labels: *request* (ApiVersions, Metadata, Produce, FindCoordinator, ListOffsets, OffsetFetch, OffsetCommit, Fetch, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, DescribeGroups, ListGroups, DeleteGroups, SaslHandshake, SaslAuthenticate, CreateTopics, InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn, WriteTxnMarkers, DescribeConfigs, DeleteTopics). </br>|

### Response metrics

| Name | Type | Description |
|---|---|---|
| kop_server_RESPONSE_BLOCKED_TIMES | Counter | The response blocked times due to waiting for process complete |
| kop_server_RESPONSE_BLOCKED_LATENCY | Summary | The response blocked latency calculated in milliseconds|

### Producer metrics

| Name | Type | Description |
|---|---|---|
| kop_server_PRODUCE_ENCODE | Summary | The memory record encode latency |
| kop_server_MESSAGE_PUBLISH | Summary | The message publish latency to Pulsar ManagedLedger|
| kop_server_MESSAGE_QUEUED_LATENCY | Summary | The message queued latency in KoP message publish queue|
| kop_server_BYTES_IN | Counter | The producer bytes in stats. <br> Available labels: *topic*, *partition*. </br> <ul><li>*topic*: the topic name to produce.</li><li>*partition*: the partition id for the topic to produce</li></ul>|
| kop_server_MESSAGE_IN | Counter | The producer message in stats. <br> Available labels: *topic*, *partition*. </br> <ul><li>*topic*: the topic name to produce.</li><li>*partition*: the partition id for the topic to produce</li></ul>|
| kop_server_BATCH_COUNT_PER_MEMORYRECORDS | Gauge | The number of batches in each memory records|
| kop_server_PRODUCE_MESSAGE_CONVERSIONS | Counter | The producer message conversions in stats. <br> Available labels: *topic*, *partition*. </br> <ul><li>*topic*: the topic name to produce.</li><li>*partition*: the partition id for the topic to produce</li></ul>|

### Consumer metrics

| Name | Type | Description |
|---|---|---|
| kop_server_PREPARE_METADATA | Summary | The prepare metadata latency in milliseconds before starting fetch from Pulsar ManagedLedger |
| kop_server_TOTAL_MESSAGE_READ | Summary | The total message read latency in milliseconds in this fetch request|
| kop_server_MESSAGE_READ | Summary | The message read latency in milliseconds for one cursor read entry request|
| kop_server_FETCH_DECODE | Summary | The message decode latency in milliseconds|
| kop_server_BYTES_OUT | Counter | The consumer bytes out stats. <br> Available labels: *topic*, *partition*, *group*. </br> <ul><li>*topic*: the topic name to consume.</li><li>*partition*: the partition id for the topic to consume</li><li>*group*: the group id for consumer to consumer message from topic-partition</li></ul>|
| kop_server_MESSAGE_OUT | Counter | The consumer message out stats. <br> Available labels: *topic*, *partition*, *group*. </br> <ul><li>*topic*: the topic name to consume.</li><li>*partition*: the partition id for the topic to consume</li><li>*group*: the group id for consumer to consumer message from topic-partition</li></ul>|
| kop_server_ENTRIES_OUT | Counter | The consumer entries out stats. <br> Available labels: *topic*, *partition*, *group*. </br> <ul><li>*topic*: the topic name to consume.</li><li>*partition*: the partition id for the topic to consume</li><li>*group*: the group id for consumer to consumer message from topic-partition</li></ul>|
| kop_server_CONSUME_MESSAGE_CONVERSIONS | Counter | The consumer message conversions in stats. <br> Available labels: *topic*, *partition*. </br> <ul><li>*topic*: the topic name to consume.</li><li>*partition*: the partition id for the topic to consume</li></ul>|

### Kop event metrics

| Name | Type | Description |
|---|---|---|
| kop_server_KOP_EVENT_QUEUE_SIZE | Gauge | The total number of events in KoP event processing queue. |
| kop_server_KOP_EVENT_QUEUED_LATENCY | Summary | The events queued latency calculated in milliseconds. <br> Available labels: *event* (DeleteTopicsEvent, BrokersChangeEvent, ShutdownEventThread). </br>|
| kop_server_KOP_EVENT_LATENCY | Summary | The events processing total latency for all KoP event types. <br> Available labels: *event* (DeleteTopicsEvent, BrokersChangeEvent, ShutdownEventThread). </br>|