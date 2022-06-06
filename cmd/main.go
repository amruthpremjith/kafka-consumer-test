package main

import "github.com/amruthpremjith/kafka-consumer-test/consumer"

func main() {
	c := consumer.ConsumerCreate("test-group-8")
	consumer.Subscribe(c, []string{"test"})

}
