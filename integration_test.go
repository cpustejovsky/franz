package franz_test

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/cpustejovsky/franz"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
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

func NewConfigMapWithOptions(opts ...func(kafka.ConfigMap) kafka.ConfigMap) kafka.ConfigMap {
	m := make(kafka.ConfigMap)
	for _, opt := range opts {
		m = opt(m)
	}
	return m
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration")
	}
	// ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kafkaContainer, err := tckafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		tckafka.WithClusterID("test-cluster"),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		t.Errorf("failed to start container: %s", err)
		return
	}
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(brokers)
	deliverEvents := make(chan kafka.Event)
	errChan := make(chan error)
	doneChan := make(chan struct{})

	var brokerString, sep string
	for _, server := range brokers {
		brokerString = sep + server
		sep = ", "
	}
	producerCfg := kafka.ConfigMap{
		"bootstrap.servers": brokerString,
	}
	consumerCfg := kafka.ConfigMap{
		"bootstrap.servers":  brokerString,
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
	err = producer.Produce(&msg)
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
