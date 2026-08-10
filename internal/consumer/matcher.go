package consumer

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/BennerG/exchange/internal/orderbook"
	"github.com/BennerG/exchange/internal/store"
)

type Matcher struct {
	book    *orderbook.Book
	pending store.PendingOrderStore
}

// NewMatcher reconstructs the order book from persisted pending orders
// before returning, so a restarted matcher resumes with the same resting
// orders it had before it stopped, rather than an empty book.
func NewMatcher(ctx context.Context, pending store.PendingOrderStore) (*Matcher, error) {
	book := orderbook.New()

	rows, err := pending.LoadAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("load pending orders: %w", err)
	}
	for _, row := range rows {
		book.Restore(&orderbook.Order{
			ID:          row.OrderID,
			UserID:      row.UserID,
			Side:        sideFromString(row.Side),
			Quantity:    row.QuantityRequested,
			Filled:      row.QuantityFilled,
			PriceCents:  row.PriceCents,
			SubmittedAt: row.CreatedAt,
		})
	}

	return &Matcher{book: book, pending: pending}, nil
}

func (m *Matcher) Prepare(ctx context.Context, event *pb.Event) ([]*pb.Event, func() error, func(), error) {
	orderSubmitted := event.GetOrderSubmitted()
	if orderSubmitted == nil {
		return nil, func() error { return nil }, func() {}, nil
	}

	order := &orderbook.Order{
		ID:          orderSubmitted.GetOrderId(),
		UserID:      orderSubmitted.GetUserId(),
		Side:        toOrderbookSide(orderSubmitted.GetSide()),
		Quantity:    orderSubmitted.GetQuantity(),
		PriceCents:  orderSubmitted.GetPricePerShare().GetAmountCents(),
		SubmittedAt: orderSubmitted.GetSubmittedAt().AsTime(),
	}

	outcome := m.book.Match(order)

	toPublish := make([]*pb.Event, 0, len(outcome.Fills))
	for _, fill := range outcome.Fills {
		toPublish = append(toPublish, &pb.Event{
			Payload: &pb.Event_Filled{
				Filled: &pb.Filled{
					TradeId:      uuid.New().String(),
					BuyOrderId:   fill.BuyOrderID,
					SellOrderId:  fill.SellOrderID,
					BuyerUserId:  fill.BuyerID,
					SellerUserId: fill.SellerID,
					Quantity:     fill.Quantity,
					Price:        &pb.Money{AmountCents: fill.PriceCents, Currency: "USD"},
					FilledAt:     timestamppb.Now(),
				},
			},
		})
	}

	commit := func() error {
		m.book.Apply(outcome)
		return m.persistMatch(ctx, order, outcome)
	}
	discard := func() { m.book.Discard(outcome) }

	return toPublish, commit, discard, nil
}

func (m *Matcher) persistMatch(ctx context.Context, incoming *orderbook.Order, outcome *orderbook.MatchOutcome) error {
	var updates []store.PendingUpdate
	var deletes []string

	for _, fill := range outcome.Fills {
		var counterpartyID string
		var remaining int64
		if incoming.Side == orderbook.Buy {
			counterpartyID, remaining = fill.SellOrderID, fill.SellRemaining
		} else {
			counterpartyID, remaining = fill.BuyOrderID, fill.BuyRemaining
		}
		if remaining == 0 {
			deletes = append(deletes, counterpartyID)
		} else {
			updates = append(updates, store.PendingUpdate{OrderID: counterpartyID, Remaining: remaining})
		}
	}

	var insert *store.PendingOrder
	if incoming.Remaining() > 0 {
		insert = &store.PendingOrder{
			OrderID:           incoming.ID,
			UserID:            incoming.UserID,
			Side:              sideToString(incoming.Side),
			QuantityRequested: incoming.Quantity,
			QuantityFilled:    incoming.Filled,
			PriceCents:        incoming.PriceCents,
			CreatedAt:         incoming.SubmittedAt,
		}
	}

	if len(updates) == 0 && len(deletes) == 0 && insert == nil {
		return nil
	}
	return m.pending.CommitMatch(ctx, updates, deletes, insert)
}

func toOrderbookSide(side pb.OrderSide) orderbook.Side {
	if side == pb.OrderSide_SELL {
		return orderbook.Sell
	}
	return orderbook.Buy
}

func sideToString(s orderbook.Side) string {
	if s == orderbook.Sell {
		return "SELL"
	}
	return "BUY"
}

func sideFromString(s string) orderbook.Side {
	if s == "SELL" {
		return orderbook.Sell
	}
	return orderbook.Buy
}
