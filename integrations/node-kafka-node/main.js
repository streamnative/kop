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

'use strict';

var kafka = require('kafka-node');
var HighLevelProducer = kafka.HighLevelProducer;
var Consumer = kafka.Consumer;
var broker = process.env.KOP_BROKER || 'localhost:9092';
var topic = process.env.KOP_TOPIC || 'topic1';
var limit = parseInt(process.env.KOP_LIMIT || 10, 10);
var should_produce = process.env.KOP_PRODUCE || false;
var should_consume = process.env.KOP_CONSUME || false;

var counter = 0;

var Client = kafka.KafkaClient;
var client = new Client({ kafkaHost: broker });

if (should_consume) {
    // if fetchMaxBytes is set too low the broker could start sending fetch responses in RecordBatch format instead of MessageSet :(
    // https://www.npmjs.com/package/kafka-node#error-not-a-message-set-magic-byte-is-2
    var consumer = new Consumer(client, [{ topic: topic, partition: 0, fetchMaxBytes: 1024 * 1024 * 1024 * 1024 }], {
        autoCommit: true
    });
    console.log('starting to consume');

    consumer.on("message", function (message) {
        console.log('received msg ' + message);
        counter++;
        if (counter == limit) {
            console.log("consumed all messages successfully");
            process.exit();
        };
    });

    consumer.on("error", function (err) {
        console.log(err);
        process.exit(1);
    });
}

if (should_produce) {
    var producer = new HighLevelProducer(client);
    producer.on('ready', function () {
        console.log('starting to produce');
        setInterval(send, 500);
    });

    producer.on('error', function (err) {
        console.log('error', err);
        process.exit(1);
    });
}

function send() {
    var message = new Date().toString();
    producer.send([{ topic: topic, messages: [message] }], function (err, data) {
        if (err) {
            console.log('error', err);
            process.exit(1);
        }
        counter++;
        if (counter == limit) {
            console.log("produced all messages successfully");
            process.exit();
        };
    });
}