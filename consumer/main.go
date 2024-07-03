package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

func main() {
	addr := []string{"localhost:9094"}

	sarama.Logger = log.New(os.Stdout, "[sarama]", log.LstdFlags)
	cfg := sarama.NewConfig()
	cfg.ClientID = "kafka"

	cfg.Consumer.Return.Errors = true

	csm, err := sarama.NewConsumer(addr, cfg)

	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}

	part, err := csm.ConsumePartition("initial", 0, sarama.OffsetOldest)

	if err != nil {
		log.Fatalf("failed to consume partition: %v", err)
	}

	log.Println("Succesfully connected to consumer")
	defer csm.Close()

	// Handle signals for graceful shutdown
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, os.Interrupt)

	for {
		select {
		case msg := <-part.Messages():
			log.Println("Received a message from producer: ", string(msg.Value))
		case er := <-part.Errors():
			log.Println("Received an error from producer: ", er)
		}
	}
}
