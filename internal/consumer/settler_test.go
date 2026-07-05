package consumer_test

import (
	"context"
	"errors"
	"testing"

	"github.com/BennerG/exchange/internal/consumer"
	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/BennerG/exchange/internal/store"
)

// stubStore records the last trade passed to SettleFill and returns a
// configurable error, letting tests exercise both the happy path and every
// failure mode without a real database.
type stubStore struct {
	lastTrade store.Trade
	calls     int
	err       error
}

func (s *stubStore) SettleFill(_ context.Context, trade store.Trade) error {
	s.calls++
	s.lastTrade = trade
	return s.err
}

func filledEvent(tradeID, buyOrderID, sellOrderID, buyerID, sellerID string, qty, priceCents int64) *pb.Event {
	return &pb.Event{
		Payload: &pb.Event_Filled{
			Filled: &pb.Filled{
				TradeId:      tradeID,
				BuyOrderId:   buyOrderID,
				SellOrderId:  sellOrderID,
				BuyerUserId:  buyerID,
				SellerUserId: sellerID,
				Quantity:     qty,
				Price:        &pb.Money{AmountCents: priceCents, Currency: "USD"},
			},
		},
	}
}

// TestHandleEventSettlesFilled verifies a Filled event is translated into a
// store.Trade with every field mapped correctly and passed to SettleFill.
func TestHandleEventSettlesFilled(t *testing.T) {
	st := &stubStore{}
	s := consumer.NewSettler(st)

	err := s.HandleEvent(context.Background(), filledEvent("trade-1", "buy-1", "sell-1", "user-b", "user-s", 100, 15000))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.calls != 1 {
		t.Fatalf("want 1 call to SettleFill, got %d", st.calls)
	}

	want := store.Trade{
		TradeID:      "trade-1",
		BuyOrderID:   "buy-1",
		SellOrderID:  "sell-1",
		BuyerUserID:  "user-b",
		SellerUserID: "user-s",
		Quantity:     100,
		PriceCents:   15000,
	}
	if st.lastTrade != want {
		t.Errorf("trade: want %+v, got %+v", want, st.lastTrade)
	}
}

// TestHandleEventIgnoresNonFilledPayloads verifies that OrderSubmitted events
// pass through without touching the store, since the settler only settles
// fills, and other event types on this topic are meant for other readers.
func TestHandleEventIgnoresNonFilledPayloads(t *testing.T) {
	st := &stubStore{}
	s := consumer.NewSettler(st)

	orderSubmitted := &pb.Event{
		Payload: &pb.Event_OrderSubmitted{
			OrderSubmitted: &pb.OrderSubmitted{OrderId: "order-1"},
		},
	}

	if err := s.HandleEvent(context.Background(), orderSubmitted); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.calls != 0 {
		t.Errorf("expected no store calls for a non-Filled event, got %d", st.calls)
	}
}

// TestHandleFilledDuplicateIsNotAnError verifies that a store reporting
// ErrAlreadySettled is treated as success by the settler, since a duplicate
// signals a safe redelivery rather than a genuine failure.
func TestHandleFilledDuplicateIsNotAnError(t *testing.T) {
	st := &stubStore{err: store.ErrAlreadySettled}
	s := consumer.NewSettler(st)

	err := s.HandleFilled(context.Background(), &pb.Filled{
		TradeId:      "trade-1",
		BuyOrderId:   "buy-1",
		SellOrderId:  "sell-1",
		BuyerUserId:  "user-b",
		SellerUserId: "user-s",
		Quantity:     100,
		Price:        &pb.Money{AmountCents: 15000, Currency: "USD"},
	})
	if err != nil {
		t.Fatalf("want nil error on duplicate settlement, got %v", err)
	}
}

// TestHandleFilledPropagatesGenuineStoreErrors verifies that a real store
// failure, distinct from ErrAlreadySettled, is returned to the caller.
func TestHandleFilledPropagatesGenuineStoreErrors(t *testing.T) {
	dbErr := errors.New("connection refused")
	st := &stubStore{err: dbErr}
	s := consumer.NewSettler(st)

	err := s.HandleFilled(context.Background(), &pb.Filled{
		TradeId:      "trade-1",
		BuyOrderId:   "buy-1",
		SellOrderId:  "sell-1",
		BuyerUserId:  "user-b",
		SellerUserId: "user-s",
		Quantity:     100,
		Price:        &pb.Money{AmountCents: 15000, Currency: "USD"},
	})
	if err == nil {
		t.Fatal("want error to propagate, got nil")
	}
	if !errors.Is(err, dbErr) {
		t.Errorf("want wrapped dbErr, got %v", err)
	}
}
