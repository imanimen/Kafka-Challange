package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"time"
)

type Message struct {
	Text  string `json:"test"`
	State string `json:"state"`
}

func Producer() {
	// configure the kafka
	mechanism, err := scram.Mechanism(scram.SHA256,
		"temp-username",
		"temp-password")
	if err != nil {
		fmt.Println("Error creating SASL mechanism:", err)
		return
	}
	w := &kafka.Writer{
		Addr:  kafka.TCP("guided-gazelle-11787-us1-kafka.upstash.io:9092"),
		Topic: "kafka-final",
		Transport: &kafka.Transport{
			SASL: mechanism,
			TLS:  &tls.Config{},
		},
		BatchSize:  10,
		BatchBytes: int64(10 * time.Millisecond),
	}

	// define states
	states := []string{"failed", "completed", "in_progress"}

	// create messages
	messages := make([]Message, 10)
	for i := 0; i < 10; i++ {
		state := states[i%len(states)]
		message := Message{
			Text:  fmt.Sprintf("message %d - state: %s", i, state),
			State: state,
		}
		messages[i] = message
	}

	// encode messages to JSON bytes
	var data []kafka.Message
	for _, message := range messages {
		msgBytes, err := json.Marshal(message)
		if err != nil {
			fmt.Println("error marshalling message:", err)
			continue
		}
		data = append(data, kafka.Message{Value: msgBytes})
	}

	// write messages in batches
	err = w.WriteMessages(context.Background(), data...)
	if err != nil {
		fmt.Println("Error writing messages to Kafka:", err)
		return
	}
	fmt.Println("Successfully produced messages to Kafka")

	if err := w.Close(); err != nil {
		fmt.Println("Error closing connection:", err)
	}
}

func main() {
	Producer()
}
