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
	Text  string `json:"text"`
	State string `json:"state"`
}

type PersistentStore struct {
	// You can use any persistent storage solution, e.g., a database, a file, or an in-memory data structure
	failedMessages     []Message
	inProgressMessages []Message
	completedMessages  []Message
}

func Consumer() {
	// configure the kafka
	mechanism, err := scram.Mechanism(scram.SHA256,
		"Z3VpZGVkLWdhemVsbGUtMTE3ODckNB1tfbKadjljFIv71Hm05KJEC1-IzwvmiCM",
		"OTQxNmExZTUtNTliYS00MjU5LTkzNGUtYTcyYTJlMjg5MGZh")
	if err != nil {
		fmt.Println("Error creating SASL mechanism:", err)
		return
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"guided-gazelle-11787-us1-kafka.upstash.io:9092"},
		Topic:    "kafka-final",
		GroupID:  "my-group",
		MaxWait:  time.Second * 1,
		MinBytes: 1,
		MaxBytes: 10e6, // 10MB
		Dialer: &kafka.Dialer{
			SASLMechanism: mechanism,
			TLS:           &tls.Config{}, // Replace with your TLS configuration if needed
		},
	})

	defer r.Close()

	store := &PersistentStore{}

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("Error reading message:", err)
			continue
		}

		var msg Message
		err = json.Unmarshal(m.Value, &msg)
		if err != nil {
			fmt.Println("Error unmarshaling message:", err)
			continue
		}

		switch msg.State {
		case "failed":
			store.failedMessages = append(store.failedMessages, msg)
		case "in_progress":
			store.inProgressMessages = append(store.inProgressMessages, msg)
		case "completed":
			store.completedMessages = append(store.completedMessages, msg)
		default:
			fmt.Println("Unknown state:", msg.State)
		}

		fmt.Printf("Message at offset %d: %s\n", m.Offset, string(m.Value))
	}
}

func main() {
	Consumer()
}
