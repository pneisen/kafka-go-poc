package main

import (
	"log"
	"os"

	"github.com/Shopify/sarama"
)

func main() {
	args := os.Args[1:]

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	partitionConsumer, err := consumer.ConsumePartition("poc2", 0, sarama.OffsetNewest)
	if err != nil {
		consumer.Close()
		log.Fatalln(err)
	}

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumer %s got message %s\n", args[0], string(msg.Value))
		}
	}
}
