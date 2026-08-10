package consumer

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/BennerG/exchange/internal/orderbook"
	"github.com/BennerG/exchange/internal/store"
)

type Matcher struct {
	book     *orderbook.Book
	pending  store.PendingOrderStore
	snapshot atomic.Pointer[[]store.PendingOrder]
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
	m := &Matcher{book: book, pending: pending}
	m.refreshSnapshot()
	return m, nil
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
		err := m.persistMatch(ctx, order, outcome)
		m.refreshSnapshot()
		return err
	}
	discard := func() {
		m.book.Discard(outcome)
		m.refreshSnapshot()
	}

	return toPublish, commit, discard, nil
}

// Snapshot returns the most recently published view of every currently
// resting order. Safe to call from any goroutine.
func (m *Matcher) Snapshot() []store.PendingOrder {
	p := m.snapshot.Load()
	if p == nil {
		return nil
	}
	return *p
}

func (m *Matcher) refreshSnapshot() {
	resting := m.book.Snapshot()
	out := make([]store.PendingOrder, 0, len(resting))
	for _, o := range resting {
		out = append(out, store.PendingOrder{
			OrderID:           o.ID,
			UserID:            o.UserID,
			Side:              sideToString(o.Side),
			QuantityRequested: o.Quantity,
			QuantityFilled:    o.Filled,
			PriceCents:        o.PriceCents,
			CreatedAt:         o.SubmittedAt,
		})
	}
	m.snapshot.Store(&out)
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

	var inserts []store.PendingOrder
	if incoming.Remaining() > 0 {
		inserts = []store.PendingOrder{{
			OrderID:           incoming.ID,
			UserID:            incoming.UserID,
			Side:              sideToString(incoming.Side),
			QuantityRequested: incoming.Quantity,
			QuantityFilled:    incoming.Filled,
			PriceCents:        incoming.PriceCents,
			CreatedAt:         incoming.SubmittedAt,
		}}
	}

	if len(updates) == 0 && len(deletes) == 0 && inserts == nil {
		return nil
	}
	return m.pending.CommitMatch(ctx, updates, deletes, inserts)
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
