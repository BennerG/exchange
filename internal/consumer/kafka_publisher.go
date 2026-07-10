package consumer

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
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
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Idempotent = true
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = 5

	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
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
