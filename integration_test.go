package franz_test

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/cpustejovsky/franz"
)

var producer *franz.ConfluentProducer
var consumer *franz.ConfluentConsumer

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
	kafkaServers, ok := os.LookupEnv("BOOTSTRAP_SERVER")
	if !ok {
		t.Fatal("expected env var")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	deliverEvents := make(chan kafka.Event)
	errChan := make(chan error)
	doneChan := make(chan struct{})

	producerCfg := kafka.ConfigMap{
		"metadata.broker.list": kafkaServers,
	}
	consumerCfg := kafka.ConfigMap{
		"bootstrap.servers":  kafkaServers,
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
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
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
	expectCount := 1
	expectMessage := val
	select {
	case e := <-errChan:
		t.Log(e)
		t.Fail()
	case <-doneChan:
		if expectCount != s.count {
			t.Fatalf("expected %v, got %v", expectCount, s.count)
		}
		if expectMessage != s.message {
			t.Fatalf("expected %v, got %v", expectMessage, s.message)
		}
	}
}
