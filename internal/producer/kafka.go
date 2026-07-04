package producer

import (
	"context"
	"fmt"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

const topicOrders = "orders"

// KafkaPublisher implements Publisher using a sarama SyncProducer configured
// for idempotent delivery (enable.idempotence = true equivalent in sarama).
type KafkaPublisher struct {
	producer sarama.SyncProducer
}

func NewKafkaPublisher(brokers []string) (*KafkaPublisher, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0

	// Idempotent producer: Kafka deduplicates retries at the broker level.
	// Requires max.in.flight.requests.per.connection = 1 and acks = all.
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

	orderID := ""
	if orderSubmitted := event.GetOrderSubmitted(); orderSubmitted != nil {
		orderID = orderSubmitted.OrderId
	}

	msg := &sarama.ProducerMessage{
		Topic: topicOrders,
		Key:   sarama.StringEncoder(orderID),
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
