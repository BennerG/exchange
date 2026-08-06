package producer

import (
	"context"
	"fmt"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/BennerG/exchange/internal/kafka"
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
