//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

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

package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	limit, err := strconv.Atoi(getEnv("KOP_LIMIT", "10"))
	if err != nil {
		panic(err)
	}

	shouldProduce, err := strconv.ParseBool(getEnv("KOP_PRODUCE", "false"))
	if err != nil {
		panic(err)
	}

	shouldConsume, err := strconv.ParseBool(getEnv("KOP_CONSUME", "false"))
	if err != nil {
		panic(err)
	}

	brokers := []string{getEnv("KOP_BROKER", "localhost:9092")}
	topic := getEnv("KOP_TOPIC", "my-confluent-go-topic")
	topics := []string{topic}

	if shouldProduce {

		fmt.Println("starting to produce")

		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": getEnv("KOP_BROKER", "localhost:9092")})
		if err != nil {
			panic(err)
		}
		defer p.Close()

		// Delivery report handler for produced messages
		go func() {
			for e := range p.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
					} else {
						fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
					}
				}
			}
		}()

		for i := 0; i < limit; i++ {
			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte("hello from confluent go"),
			}, nil)
			fmt.Println("send a message")

		}
		fmt.Printf("produced all messages successfully (%d) \n", limit)
		// Wait for message deliveries before shutting down
		p.Flush(15 * 1000)

	}

	if shouldConsume {
		fmt.Println("starting to consume")

		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":       strings.Join(brokers, ","),
			"group.id":                "myGroup",
			"auto.offset.reset":       "earliest",
			"broker.version.fallback": "2.0.0",
			"debug":                   "all",
		})
		if err != nil {
			panic(err)
		}

		c.SubscribeTopics(topics, nil)

		counter := 0

		for counter < limit {
			msg, err := c.ReadMessage(-1)
			if err == nil {
				fmt.Println("received msg")
				fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
				counter++
			} else {
				// The client will automatically try to recover from all errors.
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
				panic(err)
			}
		}
		fmt.Println("consumed all messages successfully")
	}

	fmt.Println("exiting normally")
}

func getEnv(key, fallback string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		value = fallback
	}
	return value
}
