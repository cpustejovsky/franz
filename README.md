# franz

![franz](./franz.png)

## Background

I've spent a lot of time with Kafka or at least enough to know how difficult it can be to use.

My ultimate goal with this repo is to provide code wrappers for Kafka producers and consumers that can be easily
configured and extended to any application.

This would also coincide with extensive

## Instructions

### Creating a Producer

The following code sample provides a template for how to set up and use the producer franz provides

```go
package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/cpustejovsky/franz"
	"log"
	"os"
)

func main() {

	//Provide a ConfigMap with the following variables
	producerCfg := kafka.ConfigMap{
		//Store your variables as environemnt variables, flags, AWS secrets, etc.
		"metadata.broker.list": os.Getenv("BOOSTRAP_SERVER"),
		"sasl.username":        os.Getenv("SASL_USERNAME"),
		"sasl.password":        os.Getenv("SASL_PASSWORD"),
		"security.protocol":    "SASL_SSL",
		"sasl.mechanisms":      "PLAIN",
	}
	//Provide a deliver channel of Events
	deliverEvents := make(chan kafka.Event, 1000)

	producer := franz.NewProducer(&producerCfg, deliverEvents)

	topic := "YOUR-TOPIC"

	//Create a message with the topic and value you are using converted to a byte slice.
	//A  more complex value like JSON or Protobuf will be marshalled into binary
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte("YOUR-VALUE-HERE"),
	}

	err := producer.Produce(&msg)
	if err != nil {
		log.Fatal(err)
	}
}
```

### Creating a Consumer

The following code sample provides a template for how to set up and use the consumer franz provides

You will need an event handler that follows the type set out by the package:
```go
type Handler func(ctx context.Context, m *kafka.Message) error
```


```go
package franz_test

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/cpustejovsky/franz"
	"os"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	errChan := make(chan error)
	doneChan := make(chan struct{})

	topic := "YOUR-TOPIC"
	consumerCfg := kafka.ConfigMap{
		"metadata.broker.list": os.Getenv("BOOSTRAP_SERVER"),
		"sasl.username":        os.Getenv("SASL_USERNAME"),
		"sasl.password":        os.Getenv("SASL_PASSWORD"),
		"security.protocol":    "SASL_SSL",
		"sasl.mechanisms":      "PLAIN",
		"group.id":             "YOUR-GROUP-ID",
		"auto.offset.reset":    "earliest",
		"enable.auto.commit":   "false",
	}
	consumer := franz.NewConsumer(context.TODO(), &consumerCfg, 1000)


	go func() {
		//Pass in the context, topic, and event handler
		errChan <- consumer.Consume(ctx, topic, nil)
	}()

	select {
	case e := <-errChan:
		log.Println(e)
		os.Exit(1)
	case <-doneChan:
		os.Exit(0)
	}
}


```

## Next Steps

* ~~Add consumer code~~
* ~~Add producer code~~
* ~~Add basic integration tests~~
* Increase configurability
* Allow for greater orthogonality
* Increase integration tests to allow for a variety of configurations
