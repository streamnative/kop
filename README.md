# kop

A "Kafka on Pulsar"(KOP) Broker, which understand and speak Kafka language but backed by Pulsar.

The operation and management for KOP Broker is the same as Pulsar Broker.

> NOTE: This broker is currently support Kafka Client version [2.0.0](https://kafka.apache.org/20/documentation.html) .

## Get started

This provides a step-by-step example how to use this broker to serve requests from Kafka client.



### Build KOP broker.

1. Git clone `kop`. Assume *KOP_HOME* is the home directory for your
   cloned `kop` repo for the remaining steps.
   ```
   $ git clone https://github.com/streamnative/kop
   ```

2. Build the source in `${KOP_HOME}` directory.
   ```
   mvn clean install -DskipTests
   ```
   After successfully built, the Kop binary is created under your target directory, and also installed in your maven.  

### Run KOP broker.

There is an execute-able [kop shell script](https://github.com/streamnative/kop/blob/master/bin/kop), you can run it in standalone mode or clusters mode.

In standalone mode, kop shell script uses configuration file [`kop_standalone.conf`](https://github.com/streamnative/kop/blob/master/conf/kop_standalone.conf),
while in cluster mode, kop shell script uses configuration file [`kop.conf`](https://github.com/streamnative/kop/blob/master/conf/kop.conf).

#### Run KOP in standalone mode.
Example command to start KOP in standalone mode.
```access transformers
cd ${KOP_HOME}
bin/kop standalone
```

#### Run KOP in Cluster mode.

This is similar to the [instructions to run a Pulsar Cluster](http://pulsar.apache.org/docs/en/deploy-bare-metal/)

1. Download Pulsar 2.4.0 release from [Pulsar website](http://pulsar.apache.org/en/download/), and placed it in each node.
   Assume *PULSAR_HOME* is the home directory for your Pulsar installation for the remaining steps.

2. start ZooKeeper
Following instructions to [deploy a ZooKeeper cluster](http://pulsar.apache.org/docs/en/deploy-bare-metal/#deploying-a-zookeeper-cluster)

An example command:
```access transformers
cd ${PULSAR_HOME}
bin/pulsar zookeeper
```

3. initialize cluster metadata
Once you've deployed ZooKeeper for your cluster, there is some metadata that needs to be written to ZooKeeper for each cluster in your instance. 
A detailed instructions is [here](http://pulsar.apache.org/docs/en/deploy-bare-metal/#initializing-cluster-metadata).
It only needs to be written **once**. 

An example command:
```access transformers
cd ${PULSAR_HOME}
bin/pulsar initialize-cluster-metadata \                    
  --cluster kafka-cluster \
  --zookeeper zkhost:2181 \
  --configuration-store zkhost:2181 \
  --web-service-url http://one.broker.host:8080 \
  --web-service-url-tls https://one.broker.host:8443 \
  --broker-service-url pulsar://one.broker.host:6650 \
  --broker-service-url-tls pulsar+ssl://one.broker.host:6651
```

4. start BookKeeper

Following instructions to [deploy a BookKeeper cluster](http://pulsar.apache.org/docs/en/deploy-bare-metal/#deploying-a-bookkeeper-cluster).

An example command to start bookie in the foreground:
```access transformers
cd ${PULSAR_HOME}
bin/bookkeeper bookie  
```

5. start KOP brokers

Following instructions to [deploy a KOP broker cluster](http://pulsar.apache.org/docs/en/deploy-bare-metal/#deploying-pulsar-brokers).
In broker configuration file [`kop.conf`](https://github.com/streamnative/kop/blob/master/conf/kop.conf), 
the parameter named `kafkaServicePort` indicates the port for serving Kafka requests, by default it is `9092`
all the other configuration is the same as [original Pulsar Broker configuration](http://pulsar.apache.org/docs/en/deploy-bare-metal/#configuring-brokers)

An example command to start KOP broker:
```access transformers
cd ${KOP_HOME}
bin/kop kafka-broker
```

#### log level config

KOP use log4j2 to handling logs, its config file is [log4j2.yaml](https://github.com/streamnative/kop/blob/master/conf/log4j2.yaml)

#### Run Kafka client examples to verify it.

1. Build Kafka client example.
```access transformers
cd ${KOP_HOME}/kafka-examples
mvn clean package
```

2. Run a unlimited consumer.
```
bin/java-consumer-demo.sh`
```

2. Run a unlimited producer.
```
bin/java-producer-demo.sh`
```
