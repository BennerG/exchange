package consumer

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/BennerG/exchange/internal/store"
	"github.com/rs/zerolog/log"
)

// Settler settles Filled events into the ledger. Other event types on the
// same topic are outside its concern and are ignored rather than treated
// as errors.
type Settler struct {
	store store.Store
}

func NewSettler(st store.Store) *Settler {
	return &Settler{store: st}
}

func (s *Settler) HandleEvent(ctx context.Context, event *pb.Event) error {
	filled := event.GetFilled()
	if filled == nil {
		log.Debug().Msg("ignoring non-Filled event on fills topic")
		return nil
	}
	return s.HandleFilled(ctx, filled)
}

func (s *Settler) HandleFilled(ctx context.Context, filled *pb.Filled) error {
	trade := store.Trade{
		TradeID:      filled.TradeId,
		BuyOrderID:   filled.BuyOrderId,
		SellOrderID:  filled.SellOrderId,
		BuyerUserID:  filled.BuyerUserId,
		SellerUserID: filled.SellerUserId,
		Quantity:     filled.Quantity,
		PriceCents:   filled.Price.AmountCents,
	}

	err := s.store.SettleFill(ctx, trade)
	if err != nil {
		if errors.Is(err, store.ErrAlreadySettled) {
			log.Info().Str("trade_id", trade.TradeID).Msg("duplicate fill, already settled")
			return nil
		}
		return fmt.Errorf("error handling trade: %w", err)
	}
	return nil
}
