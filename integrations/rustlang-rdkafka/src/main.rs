extern crate futures;
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
#[macro_use]
extern crate log;
extern crate rdkafka;
extern crate tokio;

use std::env;

use futures::StreamExt;
use rdkafka::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::TopicPartitionList;

use crate::futures::FutureExt;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let brokers = env::var("KOP_BROKER").unwrap_or_else(|_| String::from("localhost:9092"));
    let topic = env::var("KOP_TOPIC").unwrap_or_else(|_| String::from("rustlang"));
    let limit: i8 = env::var("KOP_LIMIT")
        .unwrap_or_else(|_| String::from("10"))
        .parse()
        .unwrap_or(10);
    let should_produce: bool = env::var("KOP_PRODUCE")
        .unwrap_or_else(|_| String::from("false"))
        .parse()
        .unwrap_or(false);
    let should_consume: bool = env::var("KOP_CONSUME")
        .unwrap_or_else(|_| String::from("false"))
        .parse()
        .unwrap_or(false);

    println!(
        "produce={}, consume={}, limit={}",
        should_produce, should_consume, limit
    );

    if should_produce {
        println!("starting to produce");
        produce(brokers.as_ref(), topic.as_str(), limit).await?;
    }
    if should_consume {
        println!("starting to consume");
        consume_and_print(
            brokers.as_ref(),
            "rustlang-librdkafka",
            vec![topic.as_str()],
            limit,
        ).await?;
    }
    println!("exiting normally");

    Ok(())
}

async fn produce(brokers: &str, topic_name: &str, limit: i8) -> Result<(), std::io::Error> {
    println!("starting to produce");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    let futures = (0..limit)
        .map(|i| {
            // The send operation on the topic returns a future, that will be completed once the
            // result or failure from Kafka will be received.
            producer
                .send(
                    FutureRecord::to(topic_name)
                        .payload(&format!("Message {}", i))
                        .key(&format!("Key {}", i))
                        .headers(OwnedHeaders::new().add("header_key", "header_value")),
                    0,
                )
                .map(move |delivery_status| {
                    // This will be executed onw the result is received
                    println!("Delivery status for message {} received", i);
                    delivery_status
                })
        })
        .collect::<Vec<_>>();

    // This loop will wait until all delivery statuses have been received received.
    for future in futures {
        println!("Future completed. Result: {:?}", future.await);
    }
    println!(
        "produced all messages successfully ({})",
        limit
    );
    Ok(())
}

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        println!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

async fn consume_and_print(brokers: &str, group_id: &str, topics: Vec<&str>, limit: i8) -> Result<(), std::io::Error> {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(topics.as_slice())
        .expect("Can't subscribe to specified topics");

    // consumer.start() returns a stream. The stream can be used ot chain together expensive steps,
    // such as complex computations on a thread pool or asynchronous IO.
    let mut message_stream = consumer.start();

    let mut i = 0;

    while let Some(message) = message_stream.next().await {
        match message {
            Err(e) => panic!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        panic!("Error while deserializing message payload: {:?}", e);
                    }
                };
                println!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                         m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                if let Some(headers) = m.headers() {
                    for i in 0..headers.count() {
                        let header = headers.get(i).unwrap();
                        info!("  Header {:#?}: {:?}", header.0, header.1);
                    }
                }
                consumer.commit_message(&m, CommitMode::Sync).unwrap();
                i += 1;
                println!("received msg");
                if i == limit {
                    println!("consumed all messages successfully");
                    return Ok(());
                }
            }
        };
    }
    Ok(())
}
