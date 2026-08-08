package orderbook_test

import (
	"testing"
	"time"

	"github.com/BennerG/exchange/internal/orderbook"
)

// baseTime is a fixed anchor so tests that care about insertion order have a
// deterministic starting point.
var baseTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func order(id, userID string, side orderbook.Side, qty, priceCents int64, offsetSec int) *orderbook.Order {
	return &orderbook.Order{
		ID:          id,
		UserID:      userID,
		Side:        side,
		Quantity:    qty,
		PriceCents:  priceCents,
		SubmittedAt: baseTime.Add(time.Duration(offsetSec) * time.Second),
	}
}

// TestExactMatch verifies the most basic case: one buy meets one sell at the same
// price and both sides are fully filled in one shot.
func TestExactMatch(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-1", "user-s", orderbook.Sell, 100, 15000, 0))
	fills := b.Add(order("buy-1", "user-b", orderbook.Buy, 100, 15000, 1))

	if len(fills) != 1 {
		t.Fatalf("expected 1 fill, got %d", len(fills))
	}
	f := fills[0]
	if f.Quantity != 100 {
		t.Errorf("quantity: want 100, got %d", f.Quantity)
	}
	if f.PriceCents != 15000 {
		t.Errorf("price: want 15000, got %d", f.PriceCents)
	}
	if f.BuyerID != "user-b" {
		t.Errorf("buyer: want user-b, got %s", f.BuyerID)
	}
	if f.SellerID != "user-s" {
		t.Errorf("seller: want user-s, got %s", f.SellerID)
	}
}

// TestNoMatch verifies that a buy below the best ask produces zero fills and
// both orders stay resting in the book.
func TestNoMatch(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-1", "user-s", orderbook.Sell, 100, 15100, 0))
	fills := b.Add(order("buy-1", "user-b", orderbook.Buy, 100, 15000, 1))

	if len(fills) != 0 {
		t.Fatalf("expected no fills, got %d", len(fills))
	}
}

// TestPartialFill verifies that when a buy order is larger than the resting sell,
// the sell is fully consumed and the buy order retains its remaining quantity.
func TestPartialFill(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-1", "user-s", orderbook.Sell, 40, 15000, 0))
	buyOrder := order("buy-1", "user-b", orderbook.Buy, 100, 15000, 1)
	fills := b.Add(buyOrder)

	if len(fills) != 1 {
		t.Fatalf("expected 1 fill, got %d", len(fills))
	}
	if fills[0].Quantity != 40 {
		t.Errorf("fill quantity: want 40, got %d", fills[0].Quantity)
	}
	if buyOrder.Remaining() != 60 {
		t.Errorf("remaining on buy: want 60, got %d", buyOrder.Remaining())
	}
}

// TestMultiplePartialFills verifies that one large buy order drains multiple
// resting sells in sequence, producing one FillResult per counterparty.
func TestMultiplePartialFills(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-1", "user-s1", orderbook.Sell, 40, 15000, 0))
	b.Add(order("sell-2", "user-s2", orderbook.Sell, 30, 15000, 1))
	b.Add(order("sell-3", "user-s3", orderbook.Sell, 30, 15000, 2))

	buyOrder := order("buy-1", "user-b", orderbook.Buy, 100, 15000, 3)
	fills := b.Add(buyOrder)

	if len(fills) != 3 {
		t.Fatalf("expected 3 fills, got %d", len(fills))
	}

	totalFilled := int64(0)
	for _, f := range fills {
		totalFilled += f.Quantity
	}
	if totalFilled != 100 {
		t.Errorf("total filled: want 100, got %d", totalFilled)
	}
	if buyOrder.Remaining() != 0 {
		t.Errorf("buy should be fully filled, remaining: %d", buyOrder.Remaining())
	}
}

// TestPriceTimePriority verifies that when two resting orders sit at the same
// price, the one submitted earlier fills first.
//
// This is the "time" part of price-time priority. Without it, a newer order
// could jump the queue, which is unfair and violates exchange semantics.
func TestPriceTimePriority(t *testing.T) {
	b := orderbook.New()
	// sell-early arrived first and should fill before sell-late.
	b.Add(order("sell-early", "user-early", orderbook.Sell, 50, 15000, 0))
	b.Add(order("sell-late", "user-late", orderbook.Sell, 50, 15000, 5))

	fills := b.Add(order("buy-1", "user-b", orderbook.Buy, 50, 15000, 10))

	if len(fills) != 1 {
		t.Fatalf("expected 1 fill, got %d", len(fills))
	}
	if fills[0].SellOrderID != "sell-early" {
		t.Errorf("expected sell-early to fill first, got %s", fills[0].SellOrderID)
	}
}

// TestPricePriority verifies that a sell order at a lower ask price fills before
// a sell at a higher price, regardless of insertion order.
//
// This is the "price" part of price-time priority. The buyer always gets the
// best available price.
func TestPricePriority(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-high", "user-sh", orderbook.Sell, 50, 15100, 0))
	b.Add(order("sell-low", "user-sl", orderbook.Sell, 50, 15000, 1))

	fills := b.Add(order("buy-1", "user-b", orderbook.Buy, 50, 15100, 2))

	if len(fills) != 1 {
		t.Fatalf("expected 1 fill, got %d", len(fills))
	}
	if fills[0].SellOrderID != "sell-low" {
		t.Errorf("expected sell-low (better price) to fill, got %s", fills[0].SellOrderID)
	}
	if fills[0].PriceCents != 15000 {
		t.Errorf("fill price: want 15000, got %d", fills[0].PriceCents)
	}
}

// TestSellAgainstBids mirrors TestExactMatch but from the sell side, ensuring
// symmetry in the matching logic.
func TestSellAgainstBids(t *testing.T) {
	b := orderbook.New()
	b.Add(order("buy-1", "user-b", orderbook.Buy, 100, 15000, 0))
	fills := b.Add(order("sell-1", "user-s", orderbook.Sell, 100, 15000, 1))

	if len(fills) != 1 {
		t.Fatalf("expected 1 fill, got %d", len(fills))
	}
	if fills[0].Quantity != 100 {
		t.Errorf("quantity: want 100, got %d", fills[0].Quantity)
	}
}

// TestCancelPendingOrder verifies that a resting order can be cancelled before
// it matches, and that subsequent matching attempts ignore the cancelled order.
func TestCancelPendingOrder(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-1", "user-s", orderbook.Sell, 100, 15000, 0))

	removed := b.Cancel("sell-1")
	if !removed {
		t.Fatal("expected Cancel to return true for an existing order")
	}

	fills := b.Add(order("buy-1", "user-b", orderbook.Buy, 100, 15000, 1))
	if len(fills) != 0 {
		t.Errorf("expected no fills after cancel, got %d", len(fills))
	}
}

// TestCancelNonexistentOrder verifies that cancelling an order that does not
// exist in the book returns false without panicking.
func TestCancelNonexistentOrder(t *testing.T) {
	b := orderbook.New()
	removed := b.Cancel("ghost-order")
	if removed {
		t.Error("expected Cancel to return false for a missing order")
	}
}

// TestCancelPartiallyFilledOrder verifies that when an order has been partially
// filled, the remaining quantity can still be cancelled.
func TestCancelPartiallyFilledOrder(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-1", "user-s", orderbook.Sell, 100, 15000, 0))
	b.Add(order("buy-small", "user-b1", orderbook.Buy, 40, 15000, 1))

	// sell-1 still has 60 shares resting; cancel the remainder.
	removed := b.Cancel("sell-1")
	if !removed {
		t.Fatal("expected Cancel to find the partially filled sell order")
	}

	fills := b.Add(order("buy-2", "user-b2", orderbook.Buy, 100, 15000, 2))
	if len(fills) != 0 {
		t.Errorf("expected no fills after cancelling remainder, got %d", len(fills))
	}
}

// TestBuyBelowAskDoesNotMatch confirms the boundary condition: a buy at exactly
// one cent below the ask produces no fill.
func TestBuyBelowAskDoesNotMatch(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-1", "user-s", orderbook.Sell, 100, 15000, 0))
	fills := b.Add(order("buy-1", "user-b", orderbook.Buy, 100, 14999, 1))

	if len(fills) != 0 {
		t.Errorf("expected no fills, got %d", len(fills))
	}
}

// TestBuyAboveAskFillsAtAskPrice verifies that when a buyer bids above the ask,
// the fill price is the ask (seller's limit), not the buyer's bid.
//
// This is standard exchange behavior: the seller's resting limit is the contract
// price. The buyer's willingness to pay more does not change what the seller
// receives.
func TestBuyAboveAskFillsAtAskPrice(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-1", "user-s", orderbook.Sell, 100, 15000, 0))
	fills := b.Add(order("buy-1", "user-b", orderbook.Buy, 100, 15500, 1))

	if len(fills) != 1 {
		t.Fatalf("expected 1 fill, got %d", len(fills))
	}
	if fills[0].PriceCents != 15000 {
		t.Errorf("fill price should be ask price 15000, got %d", fills[0].PriceCents)
	}
}

// TestMatchDoesNotRestIncomingOrder verifies that Match alone, without a
// following Apply, never makes the incoming order visible to a future
// match or cancellable, since resting is deliberately deferred to Apply.
func TestMatchDoesNotRestIncomingOrder(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-1", "user-s", orderbook.Sell, 50, 15000, 0))

	buyOrder := order("buy-1", "user-b", orderbook.Buy, 100, 15000, 1)
	outcome := b.Match(buyOrder)

	if len(outcome.Fills) != 1 {
		t.Fatalf("expected 1 fill, got %d", len(outcome.Fills))
	}
	if b.Cancel("buy-1") {
		t.Error("incoming order should not be cancellable before Apply, it was never resting")
	}
}

// TestDiscardFullyReversesAMatch verifies that Match followed by Discard
// leaves the book in a state indistinguishable, by external behavior,
// from Match never having been called: a retry with a fresh Order, the
// same shape a redelivered event would produce, matches identically.
func TestDiscardFullyReversesAMatch(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-1", "user-s", orderbook.Sell, 100, 15000, 0))

	firstAttempt := order("buy-1", "user-b", orderbook.Buy, 40, 15000, 1)
	outcome := b.Match(firstAttempt)
	if len(outcome.Fills) != 1 {
		t.Fatalf("expected 1 fill on first attempt, got %d", len(outcome.Fills))
	}
	b.Discard(outcome)

	retry := order("buy-1", "user-b", orderbook.Buy, 40, 15000, 1)
	fills := b.Add(retry)

	if len(fills) != 1 {
		t.Fatalf("expected 1 fill on retry, got %d", len(fills))
	}
	if fills[0].Quantity != 40 {
		t.Errorf("retry quantity: want 40, got %d", fills[0].Quantity)
	}
	if fills[0].SellRemaining != 60 {
		t.Errorf("seller remaining after retry: want 60, got %d", fills[0].SellRemaining)
	}
}

// TestDiscardRestoresAFullyConsumedCounterparty verifies that when Match
// fully consumes and pops a counterparty, Discard pushes it back onto
// the book, available again, not left permanently gone.
func TestDiscardRestoresAFullyConsumedCounterparty(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-1", "user-s", orderbook.Sell, 40, 15000, 0))

	attempt := order("buy-1", "user-b", orderbook.Buy, 40, 15000, 1)
	outcome := b.Match(attempt)
	if len(outcome.Fills) != 1 {
		t.Fatalf("expected 1 fill, got %d", len(outcome.Fills))
	}
	b.Discard(outcome)

	retry := order("buy-2", "user-b2", orderbook.Buy, 40, 15000, 2)
	fills := b.Add(retry)
	if len(fills) != 1 {
		t.Fatalf("expected sell-1 to be available again, got %d fills", len(fills))
	}
	if fills[0].SellOrderID != "sell-1" {
		t.Errorf("expected sell-1 to be the counterparty again, got %s", fills[0].SellOrderID)
	}
}

// TestDiscardAcrossMultipleCounterparties verifies that a match sweeping
// several resting orders is fully reversible: every popped counterparty
// and the one partially-filled counterparty that ended the loop all
// return to exactly their prior state.
func TestDiscardAcrossMultipleCounterparties(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-1", "user-s1", orderbook.Sell, 40, 15000, 0))
	b.Add(order("sell-2", "user-s2", orderbook.Sell, 30, 15000, 1))
	b.Add(order("sell-3", "user-s3", orderbook.Sell, 30, 15000, 2))

	attempt := order("buy-1", "user-b", orderbook.Buy, 100, 15000, 3)
	outcome := b.Match(attempt)
	if len(outcome.Fills) != 3 {
		t.Fatalf("expected 3 fills, got %d", len(outcome.Fills))
	}
	b.Discard(outcome)

	retry := order("buy-2", "user-b2", orderbook.Buy, 100, 15000, 4)
	fills := b.Add(retry)

	var totalQty int64
	for _, f := range fills {
		totalQty += f.Quantity
	}
	if totalQty != 100 {
		t.Errorf("expected the full 100 to match again identically, got %d", totalQty)
	}
	if len(fills) != 3 {
		t.Errorf("expected the same 3 counterparties again, got %d fills", len(fills))
	}
}

// TestApplyRestsIncomingOrderWithRemainder verifies the ordinary success
// path: Match followed by Apply rests whatever quantity is left over,
// exactly as Add always has.
func TestApplyRestsIncomingOrderWithRemainder(t *testing.T) {
	b := orderbook.New()
	b.Add(order("sell-1", "user-s", orderbook.Sell, 40, 15000, 0))

	buyOrder := order("buy-1", "user-b", orderbook.Buy, 100, 15000, 1)
	outcome := b.Match(buyOrder)
	b.Apply(outcome)

	if !b.Cancel("buy-1") {
		t.Error("expected buy-1 to be resting and cancellable after Apply")
	}
}
