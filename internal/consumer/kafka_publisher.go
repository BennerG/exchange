package consumer

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/BennerG/exchange/internal/kafka"
)

const topicFills = "fills"

// KafkaPublisher implements Publisher, sending Filled events to the fills
// topic with the same idempotent producer configuration as the order
// producer, keyed by trade_id so every event for one trade stays ordered
// on one partition.
type KafkaPublisher struct {
	producer sarama.SyncProducer
}

func NewKafkaPublisher(brokers []string) (*KafkaPublisher, error) {
	p, err := kafka.NewIdempotentProducer(brokers)
	if err != nil {
		return nil, err
	}
	return &KafkaPublisher{producer: p}, nil
}

func (k *KafkaPublisher) Publish(_ context.Context, event *pb.Event) error {
	b, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	tradeID := ""
	if filled := event.GetFilled(); filled != nil {
		tradeID = filled.TradeId
	}

	msg := &sarama.ProducerMessage{
		Topic: topicFills,
		Key:   sarama.StringEncoder(tradeID),
		Value: sarama.ByteEncoder(b),
	}

	if _, _, err := k.producer.SendMessage(msg); err != nil {
		return fmt.Errorf("send to kafka: %w", err)
	}
	return nil
}

func (k *KafkaPublisher) Close() error {
	return k.producer.Close()
}
