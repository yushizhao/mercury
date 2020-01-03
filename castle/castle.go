package castle

import (
	"encoding/json"
	"io/ioutil"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const CONFIG = "kafka.json"

type config struct {
	Topic     string
	ConfigMap kafka.ConfigMap
}

func LoadKafkaJson() (kafka.TopicPartition, *kafka.Producer, error) {
	conf := config{}
	data, err := ioutil.ReadFile("kafka.json")
	if err != nil {
		return kafka.TopicPartition{}, nil, err
	}
	err = json.Unmarshal(data, &conf)
	if err != nil {
		return kafka.TopicPartition{}, nil, err
	}

	kafkaProducer, err := kafka.NewProducer(&conf.ConfigMap)
	if err != nil {
		return kafka.TopicPartition{}, nil, err
	}

	topic := kafka.TopicPartition{Topic: &conf.Topic, Partition: kafka.PartitionAny}
	return topic, kafkaProducer, nil
}
