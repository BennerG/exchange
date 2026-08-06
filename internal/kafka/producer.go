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
