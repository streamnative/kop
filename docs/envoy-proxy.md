# Envoy proxy for KoP

[Envoy](https://www.envoyproxy.io/) is an optional proxy for KoP, which is used when direct connections between Kafka clients and Pulsar brokers are either infeasible or undesirable. For example, when you run KoP in a cloud environment, you can run Envoy proxy.

If you want to use Envoy proxy for KoP, follow the steps below.

## Prerequisites

- Pulsar: 2.7.0 or later.
- KoP: 2.7.0 or later.
- Envoy: 1.15.0 or later.
- For supported Kafka client versions, see [here](https://github.com/streamnative/kop/tree/master/integrations).

## Step

This example assumes that you have installed Pulsar 2.8.0, KoP 2.8.0, [Envoy 1.15.0](https://www.envoyproxy.io/docs/envoy/latest/start/install), and Kafka Java client 2.6.0.

1. Configure Envoy

    For each broker with KoP enabled, a dependent Envoy proxy is required. Assuming that you have `N` brokers whose internal hostname is `pulsar-broker-<i>`, where `i` is the broker ID that varies from `0` to `N-1`.
    
    The Envoy configuration file for broker 0 is as below.
    
    ```yaml
    static_resources:
      listeners:
      - address:
          socket_address:
            # See KoP config item `kafkaAdvertisedListeners`
            address: 0.0.0.0
            port_value: 19092
        filter_chains:
        - filters:
          - name: envoy.filters.network.kafka_broker
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.network.kafka_broker.v3.KafkaBroker
              stat_prefix: kop-metrics
          - name: envoy.filters.network.tcp_proxy
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy
              stat_prefix: tcp
              cluster: kop-cluster # It must be the same as the cluster name below
      clusters:
      - name: kop-cluster
        connect_timeout: 0.25s
        type: logical_dns
        lb_policy: round_robin
        load_assignment:
          cluster_name: some_service # It could be different with the cluster name above
          endpoints:
            - lb_endpoints:
              - endpoint:
                  address:
                    socket_address:
                      # See KoP config item `kafkaListeners`
                      address: pulsar-broker-0
                      port_value: 9092
    ```
    
    > #### Tips
    >
    > For the complete configurations and descriptions in the Envoy configuration file, see [listener](https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/listener/v3/listener.proto#config-listener-v3-listener) and [cluster](https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/cluster/v3/cluster.proto#config-cluster-v3-cluster).

2. [Run Envoy](https://www.envoyproxy.io/docs/envoy/latest/start/quick-start/run-envoy).

3. Configure KoP

    Configure KoP in the `conf/broker.conf`. The KoP configurations for broker 0 are as below.

    > #### Note
    >
    > -  `kafkaListeners` and `kafkaAdvertisedListeners` must be the same as the configurations in the Envoy configuration file.
    > -  The configurations in the following example are **required**. `brokerEntryMetadataInterceptors` is introduced in KoP 2.8.0 or later.

    ```properties
    # KoP listens at port 9092 in host "pulsar-broker-0"
    kafkaListeners=PLAINTEXT://pulsar-broker-0:9092
    # Expose the port 19092 as the external port that Kafka client connects to
    kafkaAdvertisedListeners=PLAINTEXT://0.0.0.0:19092
    # Other necessary configs
    messagingProtocols=kafka
    allowAutoTopicCreationType=partitioned
    brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor
    ```

    > #### Tips
    >
    > For the complete KoP configurations and their descriptions, see [here](configuration.md).

4. Run KoP.

5. Now the Kafka client can use the exposed address (0.0.0.0:19092) to access KoP. 

    ```java
    final Properties props = new Properties();
    // See KoP config item `kafkaAdvertisedListeners`
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:19092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    /* ... */
    ```

    
