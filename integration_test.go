package franz_test

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/cpustejovsky/franz"
	"github.com/stretchr/testify/assert"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

var producer *franz.ConfluentProducer
var consumer *franz.ConfluentConsumer
var kafkaServers = os.Getenv("BOOSTRAP_SERVER")
var saslUser = os.Getenv("SASL_USERNAME")
var saslPW = os.Getenv("SASL_PASSWORD")

type stubEventHandler struct {
	count   int
	message string
}

func (s *stubEventHandler) Handle(ctx context.Context, message *kafka.Message) error {
	s.count++
	s.message = string(message.Value)
	return nil
}

func TestIntegration(t *testing.T) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	deliverEvents := make(chan kafka.Event)
	errChan := make(chan error)
	doneChan := make(chan struct{})

	producerCfg := kafka.ConfigMap{
		"metadata.broker.list": kafkaServers,
		"sasl.username":        saslUser,
		"sasl.password":        saslPW,
		"security.protocol":    "SASL_SSL",
		"sasl.mechanisms":      "PLAIN",
	}
	consumerCfg := kafka.ConfigMap{
		"bootstrap.servers":  kafkaServers,
		"sasl.username":      saslUser,
		"sasl.password":      saslPW,
		"security.protocol":  "SASL_SSL",
		"sasl.mechanisms":    "PLAIN",
		"group.id":           "integration_test",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	}
	producer = franz.NewProducer(&producerCfg, deliverEvents)
	consumer = franz.NewConsumer(context.TODO(), &consumerCfg, 1000)
	topic := "Test"

	val := "Hello, World"
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(val),
	}
	err := producer.Produce(&msg)
	assert.Nil(t, err)

	s := stubEventHandler{
		count:   0,
		message: "",
	}

	go func() {
		defer cancel()
		for s.count == 0 && s.message == "" {
		}
		doneChan <- struct{}{}
	}()

	go func() {
		errChan <- consumer.Consume(ctx, topic, s.Handle)
	}()

	select {
	case e := <-errChan:
		t.Log(e)
		t.Fail()
	case <-doneChan:
		assert.Equal(t, s.count, 1)
		assert.Equal(t, s.message, val)
	}
}
