package franz

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

type ConfluentConsumer struct {
	//Context to provide SIGINT and SIGTERM
	Ctx context.Context
	//Confluent Kafka Map of Config Values
	Config *kafka.ConfigMap
	//Timeout as int milliseconds
	PollTimeout int
}

type Handler func(ctx context.Context, m *kafka.Message) error

func New(ctx context.Context, cfg *kafka.ConfigMap, pollTimeout int) *ConfluentConsumer {
	l := ConfluentConsumer{
		Ctx:         ctx,
		Config:      cfg,
		PollTimeout: pollTimeout,
	}
	return &l
}

func (cc *ConfluentConsumer) Consume(ctx context.Context, topic string, handler Handler) error {
	run := true
	errChan := make(chan error, 1)
	evChan := make(chan kafka.Event)
	consumer, err := kafka.NewConsumer(cc.Config)
	if err != nil {
		return err
	}
	defer consumer.Close()
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		return err
	}
	go func() {
		defer close(evChan)
		for run {
			evChan <- consumer.Poll(cc.PollTimeout)
		}
	}()
	go func() {
		defer close(errChan)
		for ev := range evChan {
			switch event := ev.(type) {
			case *kafka.Message:
				//Handle Kafka Message
			case kafka.PartitionEOF:
				log.Println("Reached EOF:\t", ev)
			case kafka.Error:
				log.Println("ErrorL\t", event)
				errChan <- event
			default:
			}
		}
	}()
	select {
	case e := <-errChan:
		run = false
		return e
	case <-ctx.Done():
		run = false
		return ctx.Err()
	}
}
