package consumer

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func ConsumerCreate(group string) *kafka.Consumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     "172.18.0.2:32274",
		"broker.address.family": "v4",
		"group.id":              group,
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest",
		"security.protocol":     "SASL_SSL",
		"sasl.mechanism":        "SCRAM-SHA-512",
		"sasl.username":         "my-user",
		"sasl.password":         "sevdcTDEEgMj",
		"ssl.ca.location":       "/home/amruthpremjith/projects/kafka-producer-1/ca.crt",
		//"ssl.endpoint.identification.algorithm": "",
		//"ssl.truststore.password": "password",
	})

	if err != nil {
		fmt.Println("Could not create consumer: ", err)
	}

	fmt.Println("Consumer created")

	return c

}

func Subscribe(c *kafka.Consumer, topics []string) {
	err := c.SubscribeTopics(topics, nil)

	if err != nil {
		fmt.Println("Could not subcribe to topic")
	}

	fmt.Println("Subscribed to topic")

	run := true

	for run {
		ev := c.Poll(100)

		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("%% Message on %s:\n%s\n",
				e.TopicPartition, string(e.Value))
			if e.Headers != nil {
				fmt.Printf("%% Headers: %v\n", e.Headers)
			}
		case kafka.Error:
			// Errors should generally be considered
			// informational, the client will try to
			// automatically recover.
			// But in this example we choose to terminate
			// the application if all brokers are down.
			fmt.Println("Error while consuming", e.Code(), e)
			if e.Code() == kafka.ErrAllBrokersDown {
				run = false
			}
		}

	}
}
