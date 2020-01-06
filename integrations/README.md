# Integration tests for KoP

#  not working librairies

### Kafka-node

Producing is working, but consuming is failing as the library does not care about API_VERSIONS response and is sending FETCH v0.

```
DEBUG io.streamnative.kop.KafkaCommandDecoder - Write kafka cmd response back to client. request: RequestHeader(apiKey=FETCH, apiVersion=0, clientId=kafka-node-client, correlationId=4)
INFO  io.streamnative.kop.KafkaIntegrationTest - STDOUT: Error: Not a message set. Magic byte is 2
```