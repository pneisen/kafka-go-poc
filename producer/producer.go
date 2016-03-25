package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	rand.Seed(time.Now().Unix())
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println("Usage: producer <topic> <number of messages>")
		os.Exit(0)
	}

	var wg sync.WaitGroup
	producer, _ := sarama.NewAsyncProducer([]string{"localhost:9092"}, nil)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
		}
	}()

	numberOfMessages, _ := strconv.Atoi(args[1])
	for i := 0; i < numberOfMessages; i++ {
		message := &sarama.ProducerMessage{Topic: args[0], Value: sarama.StringEncoder(strconv.Itoa(i))}
		producer.Input() <- message
		time.Sleep(time.Duration(rand.Int31n(10)) * time.Millisecond)
	}

	producer.AsyncClose()
	wg.Wait()

}
