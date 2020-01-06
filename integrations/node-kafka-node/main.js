
'use strict';

var kafka = require('kafka-node');
var HighLevelProducer = kafka.HighLevelProducer;
var Consumer = kafka.Consumer;
var broker = process.env.KOP_BROKER || 'localhost:9092';
var topic = process.env.KOP_TOPIC || 'topic1';
var limit = process.env.KOP_EXPECT_MESSAGES || 10;
var to_produced = process.env.KOP_NBR_MESSAGES || 10;
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
            console.log("limit reached, exiting");
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
            console.log("limit reached, exiting");
            process.exit();
        };
    });
}