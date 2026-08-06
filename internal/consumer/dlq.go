package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IBM/sarama"

	"github.com/BennerG/exchange/internal/kafka"
)

const topicDLQ = "dlq"

// DeadLetterEntry records what's needed to understand, and eventually
// replay, a message that could not be processed. Payload holds the raw
// bytes exactly as they existed on the source topic, since a message that
// failed to unmarshal in the first place cannot be re-marshaled as a
// domain event.
type DeadLetterEntry struct {
	SourceTopic string    `json:"source_topic"`
	Partition   int32     `json:"partition"`
	Offset      int64     `json:"offset"`
	Reason      string    `json:"reason"`
	Payload     []byte    `json:"payload"`
	FailedAt    time.Time `json:"failed_at"`
}

type DeadLetterPublisher interface {
	PublishDeadLetter(ctx context.Context, entry DeadLetterEntry) error
}

// KafkaDeadLetterPublisher writes dead letter entries as JSON, not
// Protobuf. A dead letter entry is an operational artifact for a human to
// inspect, not a domain contract other services agree on, so it doesn't
// need schema registry involvement.
type KafkaDeadLetterPublisher struct {
	producer sarama.SyncProducer
}

func NewKafkaDeadLetterPublisher(brokers []string) (*KafkaDeadLetterPublisher, error) {
	p, err := kafka.NewIdempotentProducer(brokers)
	if err != nil {
		return nil, err
	}
	return &KafkaDeadLetterPublisher{producer: p}, nil
}

func (k *KafkaDeadLetterPublisher) PublishDeadLetter(_ context.Context, entry DeadLetterEntry) error {
	b, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal dead letter entry: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topicDLQ,
		Value: sarama.ByteEncoder(b),
	}

	if _, _, err := k.producer.SendMessage(msg); err != nil {
		return fmt.Errorf("send to dlq: %w", err)
	}
	return nil
}

func (k *KafkaDeadLetterPublisher) Close() error {
	return k.producer.Close()
}
