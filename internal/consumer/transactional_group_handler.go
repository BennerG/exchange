package consumer

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
)

// TransactionalEventHandler processes one event without finalizing
// anything. It returns whatever should be published if this attempt is
// ultimately kept, and two functions, commit and discard, exactly one of
// which must be called once the caller knows whether the surrounding
// Kafka transaction actually succeeded. Calling neither leaves whatever
// Prepare mutated internally permanently, silently unresolved.
type TransactionalEventHandler interface {
	Prepare(ctx context.Context, event *pb.Event) (toPublish []*pb.Event, commit func(), discard func(), err error)
}

// Publisher sends a serialized event to a Kafka topic.
type Publisher interface {
	Publish(ctx context.Context, event *pb.Event) error
}

// TransactionalPublisher extends Publisher with the transaction control
// methods sarama's transactional producer exposes. Matcher only ever
// depends on the plain Publisher, unaware a transaction exists at all;
// only this handler's own consume loop calls these directly.
type TransactionalPublisher interface {
	Publisher
	BeginTxn() error
	CommitTxn() error
	AbortTxn() error
	AddMessageToTxn(msg *sarama.ConsumerMessage, groupID string, metadata *string) error
}

// TransactionalGroupHandler wraps an EventHandler that also produces
// messages while processing, the matcher. Unlike GroupHandler, the
// consumed offset is not committed through session.MarkMessage and
// session.Commit. It is folded into the same Kafka transaction as
// whatever gets produced, via AddMessageToTxn and CommitTxn, so consume,
// match, produce, and offset commit succeed or fail as one atomic unit.
type TransactionalGroupHandler struct {
	handler TransactionalEventHandler
	pub     TransactionalPublisher
	dlq     DeadLetterPublisher
	groupID string
}

func NewTransactionalGroupHandler(handler TransactionalEventHandler, pub TransactionalPublisher, dlq DeadLetterPublisher, groupID string) *TransactionalGroupHandler {
	return &TransactionalGroupHandler{handler: handler, pub: pub, dlq: dlq, groupID: groupID}
}

func (h *TransactionalGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *TransactionalGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *TransactionalGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			var event pb.Event
			if err := proto.Unmarshal(message.Value, &event); err != nil {
				log.Error().Err(err).
					Str("topic", message.Topic).
					Int64("offset", message.Offset).
					Msg("failed to unmarshal event, routing to dead letter queue")
				deadLetterAndCommit(h.dlq, session, message, fmt.Sprintf("unmarshal failed: %v", err))
				continue
			}

			if err := h.processInTransaction(session, message, &event); err != nil {
				log.Error().Err(err).
					Str("topic", message.Topic).
					Int64("offset", message.Offset).
					Msg("transaction aborted, offset not committed")
				return fmt.Errorf("process in transaction: %w", err)
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *TransactionalGroupHandler) processInTransaction(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage, event *pb.Event) error {
	toPublish, commit, discard, err := h.handler.Prepare(session.Context(), event)
	if err != nil {
		return fmt.Errorf("prepare event: %w", err)
	}

	if err := h.pub.BeginTxn(); err != nil {
		discard()
		return fmt.Errorf("begin txn: %w", err)
	}

	for _, event := range toPublish {
		if err := h.pub.Publish(session.Context(), event); err != nil {
			discard()
			if abortErr := h.pub.AbortTxn(); abortErr != nil {
				log.Error().Err(abortErr).Msg("failed to abort transaction")
			}
			return fmt.Errorf("publish: %w", err)
		}
	}

	if err := h.pub.AddMessageToTxn(message, h.groupID, nil); err != nil {
		discard()
		if abortErr := h.pub.AbortTxn(); abortErr != nil {
			log.Error().Err(abortErr).Msg("failed to abort transaction")
		}
		return fmt.Errorf("add message to txn: %w", err)
	}

	if err := h.pub.CommitTxn(); err != nil {
		// CommitTxn itself failed, and the book is updated to reverse
		// anything that took place in the transaction.
		discard()
		return fmt.Errorf("commit txn: %w", err)
	}

	// Only now, with the Kafka transaction genuinely committed, is it
	// safe to make the incoming order's match real in memory.
	commit()
	return nil
}
