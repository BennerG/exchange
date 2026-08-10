package consumer_test

import (
	"context"
	"testing"

	"github.com/BennerG/exchange/internal/consumer"
	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/BennerG/exchange/internal/store"
)

// fakePendingOrderStore records the arguments of the last CommitMatch call
// and returns whatever LoadAll result or CommitMatch error a test configures,
// letting tests exercise persistence behavior without a real database.
type fakePendingOrderStore struct {
	loadAllResult []store.PendingOrder
	loadAllErr    error
	commitErr     error

	lastUpdates []store.PendingUpdate
	lastDeletes []string
	lastInsert  *store.PendingOrder
	commitCalls int
}

func (f *fakePendingOrderStore) CommitMatch(_ context.Context, updates []store.PendingUpdate, deletes []string, insert *store.PendingOrder) error {
	f.commitCalls++
	f.lastUpdates, f.lastDeletes, f.lastInsert = updates, deletes, insert
	return f.commitErr
}

func (f *fakePendingOrderStore) LoadAll(_ context.Context) ([]store.PendingOrder, error) {
	return f.loadAllResult, f.loadAllErr
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

// TestPrepareMatchesRestingOrder verifies that a crossing order produces
// exactly one fill to publish, priced at the resting order's limit, and
// that calling commit is what makes the incoming order visible to a
// later Prepare call.
func TestPrepareMatchesRestingOrder(t *testing.T) {
	m, err := consumer.NewMatcher(context.Background(), &fakePendingOrderStore{})
	if err != nil {
		t.Fatalf("construct matcher: unexpected error: %v", err)
	}

	_, commitSell, _, err := m.Prepare(context.Background(), orderSubmitted("sell-1", "user-s", pb.OrderSide_SELL, 100, 15000))
	if err != nil {
		t.Fatalf("seed sell: unexpected error: %v", err)
	}
	if err := commitSell(); err != nil {
		t.Fatalf("commit sell: unexpected error: %v", err)
	}

	toPublish, commitBuy, _, err := m.Prepare(context.Background(), orderSubmitted("buy-1", "user-b", pb.OrderSide_BUY, 100, 15000))
	if err != nil {
		t.Fatalf("submit buy: unexpected error: %v", err)
	}
	if err := commitBuy(); err != nil {
		t.Fatalf("commit buy: unexpected error: %v", err)
	}

	if len(toPublish) != 1 {
		t.Fatalf("want 1 event to publish, got %d", len(toPublish))
	}
	filled := toPublish[0].GetFilled()
	if filled == nil {
		t.Fatal("want a Filled event")
	}
	if filled.GetTradeId() == "" {
		t.Error("want a non-empty trade_id")
	}
	if filled.GetBuyOrderId() != "buy-1" || filled.GetSellOrderId() != "sell-1" {
		t.Errorf("order ids: want buy-1/sell-1, got %s/%s", filled.GetBuyOrderId(), filled.GetSellOrderId())
	}
	if filled.GetQuantity() != 100 {
		t.Errorf("quantity: want 100, got %d", filled.GetQuantity())
	}
	if filled.GetPrice().GetAmountCents() != 15000 {
		t.Errorf("price: want 15000, got %d", filled.GetPrice().GetAmountCents())
	}
}

// TestPrepareNoMatchReturnsNothingToPublish verifies that an order which
// does not cross the spread produces no events, since nothing has
// happened yet that needs publishing.
func TestPrepareNoMatchReturnsNothingToPublish(t *testing.T) {
	m, err := consumer.NewMatcher(context.Background(), &fakePendingOrderStore{})
	if err != nil {
		t.Fatalf("construct matcher: unexpected error: %v", err)
	}

	_, commitSell, _, _ := m.Prepare(context.Background(), orderSubmitted("sell-1", "user-s", pb.OrderSide_SELL, 100, 15100))
	if err := commitSell(); err != nil {
		t.Fatalf("commit sell: unexpected error: %v", err)
	}

	toPublish, commitBuy, _, err := m.Prepare(context.Background(), orderSubmitted("buy-1", "user-b", pb.OrderSide_BUY, 100, 15000))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := commitBuy(); err != nil {
		t.Fatalf("commit buy: unexpected error: %v", err)
	}

	if len(toPublish) != 0 {
		t.Errorf("want no events to publish, got %d", len(toPublish))
	}
}

// TestDiscardLeavesBookUnchangedForRetry verifies the exact scenario the
// Match, Apply, Discard split exists for: Prepare matches an order,
// discard is called instead of commit, exactly as TransactionalGroupHandler
// would on a failed publish, and an identical retry of the same order
// matches identically, not against an already-consumed book.
func TestDiscardLeavesBookUnchangedForRetry(t *testing.T) {
	m, err := consumer.NewMatcher(context.Background(), &fakePendingOrderStore{})
	if err != nil {
		t.Fatalf("construct matcher: unexpected error: %v", err)
	}

	_, commitSell, _, _ := m.Prepare(context.Background(), orderSubmitted("sell-1", "user-s", pb.OrderSide_SELL, 100, 15000))
	if err := commitSell(); err != nil {
		t.Fatalf("commit sell: unexpected error: %v", err)
	}

	firstAttempt, _, discardBuy, err := m.Prepare(context.Background(), orderSubmitted("buy-1", "user-b", pb.OrderSide_BUY, 40, 15000))
	if err != nil {
		t.Fatalf("first attempt: unexpected error: %v", err)
	}
	if len(firstAttempt) != 1 {
		t.Fatalf("want 1 fill on first attempt, got %d", len(firstAttempt))
	}
	discardBuy()

	retry, commitRetry, _, err := m.Prepare(context.Background(), orderSubmitted("buy-1", "user-b", pb.OrderSide_BUY, 40, 15000))
	if err != nil {
		t.Fatalf("retry: unexpected error: %v", err)
	}
	if err := commitRetry(); err != nil {
		t.Fatalf("commit retry: unexpected error: %v", err)
	}

	if len(retry) != 1 {
		t.Fatalf("want 1 fill on retry, got %d", len(retry))
	}
	if retry[0].GetFilled().GetQuantity() != 40 {
		t.Errorf("retry quantity: want 40, got %d", retry[0].GetFilled().GetQuantity())
	}
	if retry[0].GetFilled().GetSellerUserId() != "user-s" {
		t.Errorf("retry should match against the same, restored seller, got %s", retry[0].GetFilled().GetSellerUserId())
	}
}

// TestPrepareMultipleFillsOneEventEach verifies that one large order
// sweeping several resting counterparties produces a separate event per
// counterparty matched, each with its own trade_id.
func TestPrepareMultipleFillsOneEventEach(t *testing.T) {
	m, err := consumer.NewMatcher(context.Background(), &fakePendingOrderStore{})
	if err != nil {
		t.Fatalf("construct matcher: unexpected error: %v", err)
	}

	for i, id := range []string{"sell-1", "sell-2", "sell-3"} {
		_, commit, _, _ := m.Prepare(context.Background(), orderSubmitted(id, id+"-user", pb.OrderSide_SELL, 30+int64(i*10), 15000))
		if err := commit(); err != nil {
			t.Fatalf("commit %s: unexpected error: %v", id, err)
		}
	}

	toPublish, commit, _, err := m.Prepare(context.Background(), orderSubmitted("buy-1", "user-b", pb.OrderSide_BUY, 100, 15000))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := commit(); err != nil {
		t.Fatalf("commit buy: unexpected error: %v", err)
	}

	if len(toPublish) != 3 {
		t.Fatalf("want 3 events to publish, got %d", len(toPublish))
	}

	seen := make(map[string]bool)
	var totalQty int64
	for _, event := range toPublish {
		filled := event.GetFilled()
		if filled == nil {
			t.Fatal("want every event to be a Filled event")
		}
		if seen[filled.GetTradeId()] {
			t.Errorf("trade_id %s published more than once", filled.GetTradeId())
		}
		seen[filled.GetTradeId()] = true
		totalQty += filled.GetQuantity()
	}
	if totalQty != 100 {
		t.Errorf("total matched quantity: want 100, got %d", totalQty)
	}
}

// TestPrepareIgnoresNonOrderSubmittedPayloads verifies that events other
// than OrderSubmitted return nothing to publish and no-op commit and
// discard functions, safe to call either one.
func TestPrepareIgnoresNonOrderSubmittedPayloads(t *testing.T) {
	m, err := consumer.NewMatcher(context.Background(), &fakePendingOrderStore{})
	if err != nil {
		t.Fatalf("construct matcher: unexpected error: %v", err)
	}

	toPublish, commit, discard, err := m.Prepare(context.Background(), filledEvent("trade-1", "buy-1", "sell-1", "user-b", "user-s", 100, 15000))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(toPublish) != 0 {
		t.Errorf("want no events to publish, got %d", len(toPublish))
	}
	if err := commit(); err != nil {
		t.Errorf("want commit to no-op cleanly, got %v", err)
	}
	discard()
}

// TestPrepareCommitPersistsNewRestingOrder verifies that when an order
// rests, commit passes exactly one insert, with no updates or deletes, to
// the pending order store.
func TestPrepareCommitPersistsNewRestingOrder(t *testing.T) {
	pending := &fakePendingOrderStore{}
	m, err := consumer.NewMatcher(context.Background(), pending)
	if err != nil {
		t.Fatalf("construct matcher: unexpected error: %v", err)
	}

	_, commit, _, err := m.Prepare(context.Background(), orderSubmitted("buy-1", "user-b", pb.OrderSide_BUY, 100, 15000))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := commit(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pending.commitCalls != 1 {
		t.Fatalf("want 1 CommitMatch call, got %d", pending.commitCalls)
	}
	if pending.lastInsert == nil || pending.lastInsert.OrderID != "buy-1" {
		t.Errorf("want an insert for buy-1, got %+v", pending.lastInsert)
	}
	if len(pending.lastUpdates) != 0 || len(pending.lastDeletes) != 0 {
		t.Error("want no updates or deletes for a fresh resting order")
	}
}
