# Kafka-Challenge

# Challenge
Design the following system: 
a kafka producer that produces 1000 messages into a kafka topic as a batch.
The producer produces messages that have different states, some states are failed, some are completed and some are in progress. 
write a kafka consumer that reads data from this topic and parses these states and stores them into a persistent store.
The persistent store now has various messages that are either successful, in progress or failed.
Write a test suite that is able to correctly assert that there was no messages dropped and the messages were correctly parsed into
each bucket of successful, in progress or failed.
The system is not static but is a streaming system.

# Run
- Producer `go run producer/producer.go`
- Consumer `go run consumer/consumer.go`