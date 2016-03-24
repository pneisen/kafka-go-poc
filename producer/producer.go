package main

import (
	"log"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
)

func main() {

	var wg sync.WaitGroup
	producer, _ := sarama.NewAsyncProducer([]string{"localhost:9092"}, nil)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
		}
	}()

	for i := 0; i < 100; i++ {
		message := &sarama.ProducerMessage{Topic: "poc4", Value: sarama.StringEncoder(strconv.Itoa(i))}
		producer.Input() <- message
	}

	producer.AsyncClose()
	wg.Wait()

}
