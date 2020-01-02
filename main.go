package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/yushizhao/mercury/messenger"
)

var TOPIC = "test"

func main() {
	kafkaProducer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095",
		"security.protocol": "SASL_PLAINTEXT",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     "admin",
		"sasl.password":     "adminpassword",
	})
	topic := kafka.TopicPartition{Topic: &TOPIC, Partition: kafka.PartitionAny}
	defer kafkaProducer.Close()

	hermes, err := messenger.NewMessenger()
	if err != nil {
		log.Fatal(err)
	}
	defer hermes.Close()

	done := make(chan bool)
	// Delivery report handler for produced messages
	go func() {
		for e := range kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %s!!!\n%s: %s\n", ev.TopicPartition.Error.Error(), string(ev.Key), string(ev.Value))
				}
			default:
				log.Println(ev.String())
			}
		}
	}()

	go func() {
		for {
			select {
			case Message, ok := <-hermes.Messages:
				if !ok {
					return
				}
				// Produce messages to topic (asynchronously)
				kafkaProducer.Produce(&kafka.Message{
					TopicPartition: topic,
					Value:          Message.Msg,
					Key:            []byte(Message.Event.Name),
				}, nil)

				// log.Printf("%s: %s", Message.Event.Name, string(Message.Msg))

			case err, ok := <-hermes.Errors:
				if !ok {
					return
				}
				errMsg := fmt.Sprintf("Internal error: %s", err.Error())
				// Produce messages to topic (asynchronously)
				kafkaProducer.Produce(&kafka.Message{
					TopicPartition: topic,
					Value:          []byte(errMsg),
					Key:            []byte("Mercury"),
				}, nil)
			}
		}
	}()

	<-done

}
