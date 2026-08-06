package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
)

// NewIdempotentProducer returns a sarama.SyncProducer configured for
// idempotent delivery. Every publisher in this project, orders, fills,
// and now the DLQ, is built on this same configuration, since a network
// retry should never be able to produce a duplicate message on any topic.
func NewIdempotentProducer(brokers []string) (sarama.SyncProducer, error) {
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
	return p, nil
}

// NewTransactionalProducer returns a sarama.SyncProducer configured for
// Kafka transactions, so consume, produce, and offset commit become one
// atomic unit. transactionalID must be unique and stable per running
// producer instance, Kafka uses it to fence out any other producer
// sharing the same ID, which is exactly what prevents two instances from
// ever committing conflicting halves of the same transaction stream.
// A fixed constant is correct for a single running matcher instance;
// scaling to multiple partitions later means deriving a distinct ID per
// instance, typically from the partition it owns.
func NewTransactionalProducer(brokers []string, transactionalID string) (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Idempotent = true
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Transaction.ID = transactionalID

	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, fmt.Errorf("create transactional kafka producer: %w", err)
	}
	return p, nil
}
