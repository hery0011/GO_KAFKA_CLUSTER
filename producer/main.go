package main

import (
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

func main() {
	brokers := []string{"0.0.0.0:9092", "0.0.0.0:9093"}
	topic := "my-topic"

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("Failed to close producer: %s", err)
		}
	}()

	// Listen for successes and errors
	go func() {
		for {
			select {
			case msg := <-producer.Successes():
				fmt.Printf("Produced message to topic %s partition %d offset %d\n",
					msg.Topic, msg.Partition, msg.Offset)
			case err := <-producer.Errors():
				log.Fatalf("Failed to produce message: %s", err)
			}
		}
	}()

	// Send some messages
	for i := 0; i < 10; i++ {
		value := []byte(fmt.Sprintf("Hello, world! %d", i))
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(value),
		}
		producer.Input() <- msg
	}
}
