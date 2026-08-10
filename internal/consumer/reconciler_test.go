package consumer_test

import (
	"context"
	"testing"
	"time"

	"github.com/BennerG/exchange/internal/consumer"
	"github.com/BennerG/exchange/internal/store"
)

func pendingOrder(id string, filled int64) store.PendingOrder {
	return store.PendingOrder{
		OrderID: id, UserID: "user-x", Side: "BUY",
		QuantityRequested: 100, QuantityFilled: filled, PriceCents: 15000,
		CreatedAt: time.Now(),
	}
}

// TestReconcileNoDriftMakesNoCorrections verifies that identical memory
// and persisted state results in no CommitMatch call at all.
func TestReconcileNoDriftMakesNoCorrections(t *testing.T) {
	memory := []store.PendingOrder{pendingOrder("order-1", 0)}
	pending := &fakePendingOrderStore{loadAllResult: memory}
	r := consumer.NewReconciler(func() []store.PendingOrder { return memory }, pending, time.Second)

	r.ReconcileOnceForTest(context.Background())

	if pending.commitCalls != 0 {
		t.Errorf("want no corrections for matching state, got %d", pending.commitCalls)
	}
}

// TestReconcileInsertsMissingOrder verifies an order resting in memory
// but absent from persisted state is inserted.
func TestReconcileInsertsMissingOrder(t *testing.T) {
	memory := []store.PendingOrder{pendingOrder("order-1", 0)}
	pending := &fakePendingOrderStore{loadAllResult: nil}
	r := consumer.NewReconciler(func() []store.PendingOrder { return memory }, pending, time.Second)

	r.ReconcileOnceForTest(context.Background())

	if pending.commitCalls != 1 || len(pending.lastInserts) != 1 || pending.lastInserts[0].OrderID != "order-1" {
		t.Errorf("want 1 insert for order-1, got %+v", pending.lastInserts)
	}
}

// TestReconcileCorrectsDriftedQuantity verifies a mismatched
// quantity_filled is updated to match memory.
func TestReconcileCorrectsDriftedQuantity(t *testing.T) {
	memory := []store.PendingOrder{pendingOrder("order-1", 40)}
	persisted := []store.PendingOrder{pendingOrder("order-1", 0)}
	pending := &fakePendingOrderStore{loadAllResult: persisted}
	r := consumer.NewReconciler(func() []store.PendingOrder { return memory }, pending, time.Second)

	r.ReconcileOnceForTest(context.Background())

	if len(pending.lastUpdates) != 1 || pending.lastUpdates[0].Remaining != 60 {
		t.Errorf("want 1 update with remaining=60, got %+v", pending.lastUpdates)
	}
}

// TestReconcileDeletesStaleOrder verifies a persisted row with no
// corresponding resting order in memory is deleted.
func TestReconcileDeletesStaleOrder(t *testing.T) {
	persisted := []store.PendingOrder{pendingOrder("order-1", 0)}
	pending := &fakePendingOrderStore{loadAllResult: persisted}
	r := consumer.NewReconciler(func() []store.PendingOrder { return nil }, pending, time.Second)

	r.ReconcileOnceForTest(context.Background())

	if len(pending.lastDeletes) != 1 || pending.lastDeletes[0] != "order-1" {
		t.Errorf("want 1 delete for order-1, got %v", pending.lastDeletes)
	}
}
