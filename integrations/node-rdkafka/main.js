/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
'use strict';
const Kafka = require('node-rdkafka');

var broker = process.env.KOP_BROKER || 'localhost:9092';
var topic = process.env.KOP_TOPIC || 'topic1';
var limit = parseInt(process.env.KOP_LIMIT || 10, 10);
var should_produce = process.env.KOP_PRODUCE || false;
var should_consume = process.env.KOP_CONSUME || false;

var counter = 0;

if (should_consume) {
  var consumer = new Kafka.KafkaConsumer({
    //'debug': 'all',
    'metadata.broker.list': broker,
    'group.id': 'node-rdkafka-consumer-flow-example',
    'enable.auto.commit': false
    }, {
        "auto.offset.reset": "earliest",
    });

  var topicName = topic;

  //logging debug messages, if debug is enabled
  consumer.on('event.log', function (log) {
    console.log(log);
  });

  //logging all errors
  consumer.on('event.error', function (err) {
    console.error('Error from consumer');
    console.error(err);
    process.exit(1);
  });


  consumer.on('ready', function (arg) {
    console.log('consumer ready.' + JSON.stringify(arg));
    console.log("starting to consume");

    consumer.subscribe([topicName]);
    //start consuming messages
    consumer.consume();
  });

  consumer.on('data', function (m) {

    console.log("received msg", m)

    console.log('calling commit');
    consumer.commit(m);

    // Output the actual message contents
    console.log(JSON.stringify(m));
    console.log(m.value.toString());

    counter++;

    if (counter === limit) {
      console.log("consumed all messages successfully");
      consumer.disconnect();
    }
  });

  consumer.on('disconnected', function (arg) {
    console.log('consumer disconnected. ' + JSON.stringify(arg));
      process.exit();

  });

  //starting the consumer
  consumer.connect();
}

if (should_produce) {
  var producer = new Kafka.Producer({
    // 'debug' : 'all',
    'metadata.broker.list': broker,
    'dr_cb': true  //delivery report callback
  });

  var topicName = topic;

  //logging debug messages, if debug is enabled
  producer.on('event.log', function (log) {
    console.log(log);
  });

  //logging all errors
  producer.on('event.error', function (err) {
    console.error('Error from producer');
    console.error(err);
    process.exit(1);
  });

  producer.on('delivery-report', function (err, report) {
    console.log('delivery-report: ' + JSON.stringify(report));
    counter++;
    console.log(counter, limit);
    if (counter === limit) {
      console.log("produced all messages successfully");
      producer.disconnect();
      process.exit();
    }
  });

  //Wait for the ready event before producing
  producer.on('ready', function (arg) {
    console.log('producer ready.' + JSON.stringify(arg));
    console.log('starting to produce');

    for (var i = 0; i < limit; i++) {
      var value = Buffer.from('value-' + i);
      var key = "key-" + i;
      // if partition is set to -1, librdkafka will use the default partitioner
      var partition = -1;
      producer.produce(topicName, partition, value, key);
    }

    //need to keep polling for a while to ensure the delivery reports are received
    var pollLoop = setInterval(function () {
      producer.poll();
    }, 1000);

  });

  producer.on('disconnected', function (arg) {
    console.log('producer disconnected. ' + JSON.stringify(arg));
  });

  //starting the producer
  producer.connect();
}

