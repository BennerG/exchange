package consumer_test

import (
	"context"
	"errors"
	"testing"

	"github.com/BennerG/exchange/internal/consumer"
	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
)

// stubPublisher records every event published across a single HandleEvent
// call, since one order can produce several Filled events at once.
type stubPublisher struct {
	published []*pb.Event
	err       error
}

func (s *stubPublisher) Publish(_ context.Context, event *pb.Event) error {
	if s.err != nil {
		return s.err
	}
	s.published = append(s.published, event)
	return nil
}

func orderSubmitted(orderID, userID string, side pb.OrderSide, qty, priceCents int64) *pb.Event {
	return &pb.Event{
		Payload: &pb.Event_OrderSubmitted{
			OrderSubmitted: &pb.OrderSubmitted{
				OrderId:  orderID,
				UserId:   userID,
				Quantity: qty,
				PricePerShare: &pb.Money{
					AmountCents: priceCents,
					Currency:    "USD",
				},
				Side: side,
			},
		},
	}
}

// TestHandleEventMatchesRestingOrder verifies that a crossing order produces
// exactly one Filled event, priced at the resting order's limit.
func TestHandleEventMatchesRestingOrder(t *testing.T) {
	pub := &stubPublisher{}
	m := consumer.NewMatcher(pub)

	if err := m.HandleEvent(context.Background(), orderSubmitted("sell-1", "user-s", pb.OrderSide_SELL, 100, 15000)); err != nil {
		t.Fatalf("seed sell: unexpected error: %v", err)
	}
	if err := m.HandleEvent(context.Background(), orderSubmitted("buy-1", "user-b", pb.OrderSide_BUY, 100, 15000)); err != nil {
		t.Fatalf("submit buy: unexpected error: %v", err)
	}

	if len(pub.published) != 1 {
		t.Fatalf("want 1 published event, got %d", len(pub.published))
	}
	filled := pub.published[0].GetFilled()
	if filled == nil {
		t.Fatal("want a Filled event")
	}
	if filled.TradeId == "" {
		t.Error("want a non-empty trade_id")
	}
	if filled.BuyOrderId != "buy-1" || filled.SellOrderId != "sell-1" {
		t.Errorf("order ids: want buy-1/sell-1, got %s/%s", filled.BuyOrderId, filled.SellOrderId)
	}
	if filled.BuyerUserId != "user-b" || filled.SellerUserId != "user-s" {
		t.Errorf("user ids: want user-b/user-s, got %s/%s", filled.BuyerUserId, filled.SellerUserId)
	}
	if filled.Quantity != 100 {
		t.Errorf("quantity: want 100, got %d", filled.Quantity)
	}
	if filled.Price.AmountCents != 15000 {
		t.Errorf("price: want 15000, got %d", filled.Price.AmountCents)
	}
}

// TestHandleEventNoMatchPublishesNothing verifies that an order which does
// not cross the spread simply rests, with no event published, since nothing
// has happened yet that needs settling.
func TestHandleEventNoMatchPublishesNothing(t *testing.T) {
	pub := &stubPublisher{}
	m := consumer.NewMatcher(pub)

	m.HandleEvent(context.Background(), orderSubmitted("sell-1", "user-s", pb.OrderSide_SELL, 100, 15100))
	if err := m.HandleEvent(context.Background(), orderSubmitted("buy-1", "user-b", pb.OrderSide_BUY, 100, 15000)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pub.published) != 0 {
		t.Errorf("want no published events, got %d", len(pub.published))
	}
}

// TestHandleEventMultipleFillsPublishesOneEventEach verifies that one large
// order sweeping several resting orders produces a separate Filled event per
// counterparty matched, each with its own trade_id.
func TestHandleEventMultipleFillsPublishesOneEventEach(t *testing.T) {
	pub := &stubPublisher{}
	m := consumer.NewMatcher(pub)

	m.HandleEvent(context.Background(), orderSubmitted("sell-1", "user-s1", pb.OrderSide_SELL, 40, 15000))
	m.HandleEvent(context.Background(), orderSubmitted("sell-2", "user-s2", pb.OrderSide_SELL, 30, 15000))
	m.HandleEvent(context.Background(), orderSubmitted("sell-3", "user-s3", pb.OrderSide_SELL, 30, 15000))

	if err := m.HandleEvent(context.Background(), orderSubmitted("buy-1", "user-b", pb.OrderSide_BUY, 100, 15000)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pub.published) != 3 {
		t.Fatalf("want 3 published fills, got %d", len(pub.published))
	}

	seen := make(map[string]bool)
	var totalQty int64
	for _, event := range pub.published {
		filled := event.GetFilled()
		if filled == nil {
			t.Fatal("want every published event to be a Filled event")
		}
		if seen[filled.TradeId] {
			t.Errorf("trade_id %s published more than once", filled.TradeId)
		}
		seen[filled.TradeId] = true
		totalQty += filled.Quantity
	}
	if totalQty != 100 {
		t.Errorf("total filled quantity: want 100, got %d", totalQty)
	}
}

// TestHandleEventIgnoresNonOrderSubmittedPayloads verifies that events other
// than OrderSubmitted pass through without touching the order book, since
// this matcher is only responsible for matching new orders.
func TestHandleEventIgnoresNonOrderSubmittedPayloads(t *testing.T) {
	pub := &stubPublisher{}
	m := consumer.NewMatcher(pub)

	if err := m.HandleEvent(context.Background(), filledEvent("trade-1", "buy-1", "sell-1", "user-b", "user-s", 100, 15000)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.published) != 0 {
		t.Errorf("expected no publishes for a non-OrderSubmitted event, got %d", len(pub.published))
	}
}

// TestHandleEventPropagatesPublishError verifies that a publish failure is
// surfaced to the caller rather than silently dropped, since the caller
// needs this signal to decide whether the Kafka offset is safe to commit.
func TestHandleEventPropagatesPublishError(t *testing.T) {
	pub := &stubPublisher{}
	m := consumer.NewMatcher(pub)
	m.HandleEvent(context.Background(), orderSubmitted("sell-1", "user-s", pb.OrderSide_SELL, 100, 15000))

	publishErr := errors.New("broker unavailable")
	pub.err = publishErr

	err := m.HandleEvent(context.Background(), orderSubmitted("buy-1", "user-b", pb.OrderSide_BUY, 100, 15000))
	if !errors.Is(err, publishErr) {
		t.Errorf("want wrapped publishErr, got %v", err)
	}
}
