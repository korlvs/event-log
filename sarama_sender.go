package outbox

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
)

type SaramaSender struct {
	producer sarama.SyncProducer
	topic    string
}

func NewSaramaSender(brokers []string, topic string) (*SaramaSender, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create sarama producer: %w", err)
	}
	return &SaramaSender{
		producer: producer,
		topic:    topic,
	}, nil
}

func (s *SaramaSender) Send(ctx context.Context, key string, encodedKey, encodedValue []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: s.topic,
		Key:   sarama.ByteEncoder(encodedKey),
		Value: sarama.ByteEncoder(encodedValue),
	}
	_, _, err := s.producer.SendMessage(msg)
	return err
}

func (s *SaramaSender) Close() error {
	return s.producer.Close()
}
