package consumer

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
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
}

func NewGroupHandler(handler EventHandler) *GroupHandler {
	return &GroupHandler{handler: handler}
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
				// A malformed message will fail to unmarshal identically on
				// every redelivery. Without DLQ routing in place yet, the
				// least-bad option is to skip it rather than block the
				// partition on a message that can never succeed.
				log.Error().Err(err).
					Str("topic", message.Topic).
					Int64("offset", message.Offset).
					Msg("failed to unmarshal event, skipping")
				session.MarkMessage(message, "")
				session.Commit()
				continue
			}

			if err := h.handler.HandleEvent(session.Context(), &event); err != nil {
				log.Error().Err(err).
					Str("topic", message.Topic).
					Int64("offset", message.Offset).
					Msg("failed to handle event, offset not committed")
				return fmt.Errorf("handle event: %w", err)
			}

			session.MarkMessage(message, "")
			session.Commit()

		case <-session.Context().Done():
			return nil
		}
	}
}
