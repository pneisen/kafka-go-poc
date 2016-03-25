package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/bsm/sarama-cluster"
)

func main() {
	rand.Seed(time.Now().Unix())
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println("Usage: clusterConsumer <name> <group name> <topic>")
		os.Exit(0)
	}

	consumer, err := cluster.NewConsumer([]string{"localhost:9092"}, args[1], []string{args[2]}, nil)
	if err != nil {
		log.Fatalln(err)
	}

	for {
		select {
		case msg := <-consumer.Messages():
			log.Printf("Consumer %s got message %s\n", args[0], string(msg.Value))
			consumer.MarkOffset(msg, "OK")
		}

		time.Sleep(time.Duration(rand.Int31n(1000)) * time.Millisecond)
	}
}
