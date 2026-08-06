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

// KafkaPublisher publishes Filled events transactionally. Matcher only
// ever calls Publish, through the plain Publisher interface, and has no
// awareness that a transaction is open around it. The transaction
// control methods below exist for TransactionalGroupHandler to call.
type KafkaPublisher struct {
	producer sarama.SyncProducer
}

func NewKafkaPublisher(brokers []string, transactionalID string) (*KafkaPublisher, error) {
	p, err := kafka.NewTransactionalProducer(brokers, transactionalID)
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

func (k *KafkaPublisher) BeginTxn() error  { return k.producer.BeginTxn() }
func (k *KafkaPublisher) CommitTxn() error { return k.producer.CommitTxn() }
func (k *KafkaPublisher) AbortTxn() error  { return k.producer.AbortTxn() }

func (k *KafkaPublisher) AddMessageToTxn(msg *sarama.ConsumerMessage, groupID string, metadata *string) error {
	return k.producer.AddMessageToTxn(msg, groupID, metadata)
}

func (k *KafkaPublisher) Close() error {
	return k.producer.Close()
}
