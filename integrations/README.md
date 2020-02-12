# Integration tests for KoP

# Produce and Consume support

## Golang

* [https://github.com/Shopify/sarama](https://github.com/Shopify/sarama)
* [https://github.com/confluentinc/confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)

## Rust

* [https://github.com/fede1024/rust-rdkafka](https://github.com/fede1024/rust-rdkafka)

## NodeJS

* [https://github.com/Blizzard/node-rdkafka](https://github.com/Blizzard/node-rdkafka)

# Partial support

### [kafka-node](https://www.npmjs.com/package/kafka-node)

Producing is working, but consuming is failing as the library is sending FETCH v0 regardless of API_VERSIONS responses:

```
DEBUG io.streamnative.pulsar.handlers.kop.KafkaCommandDecoder - Write kafka cmd response back to client. request: RequestHeader(apiKey=FETCH, apiVersion=0, clientId=kafka-node-client, correlationId=4)
INFO  io.streamnative.pulsar.handlers.kop.KafkaIntegrationTest - STDOUT: Error: Not a message set. Magic byte is 2
```
