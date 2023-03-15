package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func main() {
	brokers := []string{"localhost:9092", "localhost:9093"}
	topic := "my-topic"
	group := "my-group"

	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalf("Failed to close consumer group: %s", err)
		}
	}()

	go func() {
		for {
			if err := consumer.Consume(ctx, []string{topic}, &messageHandler{}); err != nil {
				log.Fatalf("Failed to consume messages: %s", err)
			}
			if ctx.Err() != nil {
				return // context was cancelled
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt)
	<-sigterm

	log.Println("Shutting down consumer...")
	cancel()
}

type messageHandler struct{}

func (*messageHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (*messageHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (*messageHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Received message: %s\n", string(msg.Value))
		sess.MarkMessage(msg, "")
	}
	return nil
}
