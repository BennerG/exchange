package consumer

import (
	"fmt"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
)

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
	handler EventHandler
	pub     TransactionalPublisher
	dlq     DeadLetterPublisher
	groupID string
}

func NewTransactionalGroupHandler(handler EventHandler, pub TransactionalPublisher, dlq DeadLetterPublisher, groupID string) *TransactionalGroupHandler {
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
				// Nothing was, or ever will be, produced for a message
				// that never decoded, so there is no transaction to open.
				// A direct commit is enough, same as GroupHandler.
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
	if err := h.pub.BeginTxn(); err != nil {
		return fmt.Errorf("begin txn: %w", err)
	}

	if err := h.handler.HandleEvent(session.Context(), event); err != nil {
		if abortErr := h.pub.AbortTxn(); abortErr != nil {
			log.Error().Err(abortErr).Msg("failed to abort transaction")
		}
		return fmt.Errorf("handle event: %w", err)
	}

	if err := h.pub.AddMessageToTxn(message, h.groupID, nil); err != nil {
		if abortErr := h.pub.AbortTxn(); abortErr != nil {
			log.Error().Err(abortErr).Msg("failed to abort transaction")
		}
		return fmt.Errorf("add message to txn: %w", err)
	}

	return h.pub.CommitTxn()
}
