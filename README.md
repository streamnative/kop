# KOP

KOP stands for Kafka on Pulsar. KOP broker supports Kafka protocols, and is backed by Pulsar.

The operation and management for KOP broker is the same as Pulsar broker.

> NOTE: This broker currently supports [Kafka Client 2.0.0](https://kafka.apache.org/20/documentation.html).

## Get started

In this guide, you will learn how to use the KOP broker to serve requests from Kafka client.

### Build KOP broker

1. Git clone `kop`.    
Assume *KOP_HOME* is the home directory for your cloned `kop` repository.
  
   ```
   $ git clone https://github.com/streamnative/kop
   ```

2. Build the source in the `${KOP_HOME}` directory.
   
   ```
   mvn clean install -DskipTests
   ```
   After building the source successfully, the KOP binary is created in your target directory, and installed in your maven.  

### Run KOP broker

You can run [kop shell script](https://github.com/streamnative/kop/blob/master/bin/kop) in standalone mode or in cluster mode.

- In standalone mode, kop shell script uses the [`kop_standalone.conf`](https://github.com/streamnative/kop/blob/master/conf/kop_standalone.conf) configuration file.
- In cluster mode, kop shell script uses [`kop.conf`](https://github.com/streamnative/kop/blob/master/conf/kop.conf) configuration file.

#### Run KOP in standalone mode
To start KOP in standalone mode, refer to the following command.

```access transformers
cd ${KOP_HOME}
bin/kop standalone
```

#### Run KOP in cluster mode

Starting KOP in cluster mode is similar to the [instructions to run a Pulsar Cluster](http://pulsar.apache.org/docs/en/deploy-bare-metal/).

1. Download [Pulsar 2.4.0](http://pulsar.apache.org/en/download/), and copy the package in each node.  
   Assume *PULSAR_HOME* is the home directory for your Pulsar installation.

2. Start ZooKeeper.  
Follow instructions to [deploy a ZooKeeper cluster](http://pulsar.apache.org/docs/en/deploy-bare-metal/#deploying-a-zookeeper-cluster).

Command example

```access transformers
cd ${PULSAR_HOME}
bin/pulsar zookeeper
```

3. Initialize cluster metadata.  
Once you have deployed ZooKeeper for your cluster, some metadata needs to be written to ZooKeeper for each cluster in your instance. 
A detailed instructions is [here](http://pulsar.apache.org/docs/en/deploy-bare-metal/#initializing-cluster-metadata).
It only needs to be written **once**. 

Command example

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

4. Start BookKeeper.

Follow instructions to [deploy a BookKeeper cluster](http://pulsar.apache.org/docs/en/deploy-bare-metal/#deploying-a-bookkeeper-cluster).

The following is a command example to start a bookie in the foreground.

```access transformers
cd ${PULSAR_HOME}
bin/bookkeeper bookie  
```

5. Start KOP brokers.

Follow instructions to [deploy a KOP broker cluster](http://pulsar.apache.org/docs/en/deploy-bare-metal/#deploying-pulsar-brokers).

In the [`kop.conf`](https://github.com/streamnative/kop/blob/master/conf/kop.conf) broker configuration file, the `kafkaServicePort` parameter indicates the port for serving Kafka requests, it is `9092` by default. All the other configuration is the same as [original Pulsar Broker configuration](http://pulsar.apache.org/docs/en/deploy-bare-metal/#configuring-brokers).

Command example

```access transformers
cd ${KOP_HOME}
bin/kop kafka-broker
```

#### log level config

KOP uses log4j2 to handle logs, the config file is [log4j2.yaml](https://github.com/streamnative/kop/blob/master/conf/log4j2.yaml).

#### Run Kafka client examples to verify

1. Build Kafka client example.

```access transformers
cd ${KOP_HOME}/kafka-examples
mvn clean package
```

2. Run a unlimited consumer.

```
bin/java-consumer-demo.sh`
```

3. Run a unlimited producer.

```
bin/java-producer-demo.sh`
```
