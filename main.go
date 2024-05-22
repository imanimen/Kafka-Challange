package main

import "github.com/segmentio/kafka-go"

type Message struct {
	Text  string `json:"text"`
	State string `json:"state"`
}

func main() {
	// kafka configuration
	w := &kafka.Writer{
		Addr:  kafka.TCP("guided-gazelle-11787-us1-kafka.upstash.io:9092"),
		Topic: "kafka-1",
	}

	// define states
}
