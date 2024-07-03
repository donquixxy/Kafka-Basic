package main

import (
	"log"

	"github.com/IBM/sarama"
)

func main() {

	addr := []string{"localhost:9094"}

	cfg := sarama.NewConfig()

	cfg.ClientID = "kafka"

	cfg.Producer.Return.Errors = true
	cfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(addr, cfg)

	if err != nil {
		log.Fatal(err)
	}

	p, p2, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: "initial",
		Value: sarama.StringEncoder("Hello, kafka BUT THIS TIME BETTER !!!"),
	})

	if err != nil {
		log.Fatal(err)
	}

	log.Print(p, p2)
}
