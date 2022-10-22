package franz

import (
	"context"
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

type Handler func(ctx context.Context, m *kafka.Message) error

type ConfluentConsumer struct {
	//Context to provide SIGINT and SIGTERM
	Ctx context.Context
	//Confluent Kafka Map of Config Values
	Config *kafka.ConfigMap
	//Timeout as int milliseconds
	PollTimeout int
}

type consumerMessageConfig struct {
	evChan       chan kafka.Event
	errChan      chan error
	consumer     *kafka.Consumer
	eventHandler Handler
	ctx          context.Context
}

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
	msgCfg := consumerMessageConfig{
		evChan:       evChan,
		errChan:      errChan,
		consumer:     consumer,
		eventHandler: handler,
		ctx:          ctx,
	}
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
	go consumeMessage(msgCfg)
	select {
	case e := <-errChan:
		run = false
		return e
	case <-ctx.Done():
		run = false
		return ctx.Err()
	}
}

func consumeMessage(cfg consumerMessageConfig) {
	defer close(cfg.errChan)
	for ev := range cfg.evChan {
		switch event := ev.(type) {
		case *kafka.Message:
			msgValues := make(map[string]interface{})
			err := json.Unmarshal(event.Value, &msgValues)
			if err != nil {
				cfg.errChan <- err
			}
			for {
				err = cfg.eventHandler(cfg.ctx, event)
				if err != nil {
					//TODO Determine error handling; potentially have different
					cfg.errChan <- err
				} else {
					go func() {
						_, err := cfg.consumer.Commit()
						if err != nil {
							log.Println("Error committing offset", err)
						}
					}()
					break
				}
			}
		case kafka.PartitionEOF:
			log.Println("Reached EOF:\t", ev)
		case kafka.Error:
			log.Println("ErrorL\t", event)
			cfg.errChan <- event
		default:
		}
	}
}
