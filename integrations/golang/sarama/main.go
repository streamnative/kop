package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
)

type exampleConsumerGroupHandler struct {
	counter int
	limit   int
	wg      *sync.WaitGroup
}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		sess.MarkMessage(msg, "")
		h.counter++
		fmt.Printf("received msg %d/%d\n", h.counter, h.limit)
		if h.counter == h.limit {
			fmt.Println("limit reached, exiting")
			h.wg.Done()
			return nil
		}
	}
	return nil
}

func main() {

	fmt.Println("starting...")

	nbrMessages, err := strconv.Atoi(getEnv("KOP_NBR_MESSAGES", "10"))
	if err != nil {
		panic(err)
	}
	limit, err := strconv.Atoi(getEnv("KOP_EXPECT_MESSAGES", "10"))
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

	// Init config, specify appropriate version
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	config.Metadata.Retry.Max = 0
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Producer.Return.Successes = true
	brokers := []string{getEnv("KOP_BROKER", "localhost:9092")}
	topic := getEnv("KOP_TOPIC", "my-sarama-topic")
	topics := []string{topic}
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	fmt.Println("connecting to", brokers)

	// Start with a client
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		panic(err)
	}
	defer func() { _ = client.Close() }()

	var waitgroup sync.WaitGroup
	if shouldConsume {
		waitgroup.Add(1)

		// Start a new consumer group
		group, err := sarama.NewConsumerGroupFromClient("sarama-consumer", client)
		if err != nil {
			panic(err)
		}
		defer func() { _ = group.Close() }()

		fmt.Println("ready to consume")

		// Iterate over consumer sessions.
		ctx := context.Background()
		handler := exampleConsumerGroupHandler{counter: 0, limit: limit, wg: &waitgroup}

		err = group.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}

	}

	if shouldProduce {
		syncProducer, err := sarama.NewSyncProducerFromClient(client)
		if err != nil {
			panic(err)
		}
		defer func() { _ = syncProducer.Close() }()

		fmt.Println("starting to produce")

		for i := 0; i < nbrMessages; i++ {
			msg := &sarama.ProducerMessage{
				Topic:    topic,
				Value:    sarama.StringEncoder("hello from sarama"),
				Metadata: "test",
			}

			fmt.Println("send a message")

			_, _, err := syncProducer.SendMessage(msg)
			if err != nil {
				panic(err)
			}
		}
		fmt.Printf("produced all messages successfully (%d) \n", nbrMessages)

	}
	waitgroup.Wait()
	fmt.Println("exiting normally")

}

func getEnv(key, fallback string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		value = fallback
	}
	return value
}
