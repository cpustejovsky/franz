package franz

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ConfluentProducer struct {
	cfg         *kafka.ConfigMap
	deliverChan chan kafka.Event
}

func NewProducer(cfg *kafka.ConfigMap, deliverChan chan kafka.Event) *ConfluentProducer {
	cp := ConfluentProducer{
		deliverChan: deliverChan,
		cfg:         cfg,
	}
	return &cp
}

func (cp *ConfluentProducer) Produce(msg *kafka.Message) error {
	producer, err := kafka.NewProducer(cp.cfg)
	if err != nil {
		return fmt.Errorf("incorrect config %+v:\t%w", cp.cfg, err)
	}
	err = producer.Produce(msg,
		cp.deliverChan,
	)
	if err != nil {
		return err
	}

	e := <-cp.deliverChan
	m, ok := e.(*kafka.Message)
	if !ok {
		return fmt.Errorf("type coercion failed for %T", e)
	}
	if m.TopicPartition.Error != nil {
		return fmt.Errorf("delivery failed: %w\n", m.TopicPartition.Error)
	} else {
		log.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		return nil
	}
}
