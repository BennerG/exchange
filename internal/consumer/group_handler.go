package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/BennerG/exchange/internal/store"
)

// EventHandler processes one decoded event. Matcher and Settler both
// satisfy this already, since both declare HandleEvent with this exact
// signature, without either needing to reference this interface directly.
type EventHandler interface {
	HandleEvent(ctx context.Context, event *pb.Event) error
}

// GroupHandler adapts an EventHandler to sarama's consumer group interface.
// It commits a message's offset only after the handler successfully
// processes it, so a failure leaves the offset where it is and the message
// is redelivered on the next session.
type GroupHandler struct {
	handler EventHandler
	dlq     DeadLetterPublisher
}

func NewGroupHandler(handler EventHandler, dlq DeadLetterPublisher) *GroupHandler {
	return &GroupHandler{handler: handler, dlq: dlq}
}

func (h *GroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *GroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *GroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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

			err := h.handler.HandleEvent(session.Context(), &event)
			if err == nil {
				session.MarkMessage(message, "")
				session.Commit()
				continue
			}

			if errors.Is(err, store.ErrPermanent) {
				log.Error().Err(err).
					Str("topic", message.Topic).
					Int64("offset", message.Offset).
					Msg("permanent failure, routing to dead letter queue")
				deadLetterAndCommit(h.dlq, session, message, err.Error())
				continue
			}

			log.Error().Err(err).
				Str("topic", message.Topic).
				Int64("offset", message.Offset).
				Msg("failed to handle event, offset not committed")
			return fmt.Errorf("handle event: %w", err)

		case <-session.Context().Done():
			return nil
		}
	}
}

func (h *GroupHandler) deadLetter(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage, reason string) {
	entry := DeadLetterEntry{
		SourceTopic: message.Topic,
		Partition:   message.Partition,
		Offset:      message.Offset,
		Reason:      reason,
		Payload:     message.Value,
		FailedAt:    time.Now(),
	}
	if err := h.dlq.PublishDeadLetter(session.Context(), entry); err != nil {
		// The DLQ publish itself failed. Leaving the offset uncommitted
		// means this message redelivers from the top, including a fresh
		// attempt at the DLQ, rather than being silently lost entirely.
		log.Error().Err(err).Msg("failed to publish to dead letter queue, leaving offset uncommitted")
		return
	}
	session.MarkMessage(message, "")
	session.Commit()
}
