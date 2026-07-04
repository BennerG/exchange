package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/BennerG/exchange/internal/store"
)

func trade(id, buyOrderID, sellOrderID, buyerID, sellerID string, qty, priceCents int64) store.Trade {
	return store.Trade{
		TradeID:      id,
		BuyOrderID:   buyOrderID,
		SellOrderID:  sellOrderID,
		BuyerUserID:  buyerID,
		SellerUserID: sellerID,
		Quantity:     qty,
		PriceCents:   priceCents,
	}
}

// TestSettleFillUpdatesBalances verifies that a single trade debits the
// buyer's cash, credits the buyer's shares, and does the mirror image for
// the seller, in one settlement call.
func TestSettleFillUpdatesBalances(t *testing.T) {
	s := store.NewMemoryStore()
	err := s.SettleFill(context.Background(), trade("trade-1", "buy-1", "sell-1", "user-b", "user-s", 100, 15000))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buyerCash, buyerShares := s.Balance("user-b")
	if buyerCash != -1_500_000 {
		t.Errorf("buyer cash: want -1500000, got %d", buyerCash)
	}
	if buyerShares != 100 {
		t.Errorf("buyer shares: want 100, got %d", buyerShares)
	}

	sellerCash, sellerShares := s.Balance("user-s")
	if sellerCash != 1_500_000 {
		t.Errorf("seller cash: want 1500000, got %d", sellerCash)
	}
	if sellerShares != -100 {
		t.Errorf("seller shares: want -100, got %d", sellerShares)
	}
}

// TestSettleFillIsIdempotent verifies that redelivering the same trade ID
// is treated as a safe no-op rather than double applying the balances.
func TestSettleFillIsIdempotent(t *testing.T) {
	s := store.NewMemoryStore()
	tr := trade("trade-1", "buy-1", "sell-1", "user-b", "user-s", 100, 15000)

	if err := s.SettleFill(context.Background(), tr); err != nil {
		t.Fatalf("first settle: unexpected error: %v", err)
	}

	err := s.SettleFill(context.Background(), tr)
	if !errors.Is(err, store.ErrAlreadySettled) {
		t.Fatalf("second settle: want ErrAlreadySettled, got %v", err)
	}

	buyerCash, _ := s.Balance("user-b")
	if buyerCash != -1_500_000 {
		t.Errorf("buyer cash after duplicate: want -1500000, got %d", buyerCash)
	}
}

// TestSettleFillConcurrentTrades settles many distinct trades from multiple
// goroutines at once, confirming the mutex prevents corruption under -race.
func TestSettleFillConcurrentTrades(t *testing.T) {
	s := store.NewMemoryStore()
	const trades = 100

	var wg sync.WaitGroup
	for i := 0; i < trades; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "trade-" + string(rune('A'+i%26)) + string(rune(i))
			s.SettleFill(context.Background(), trade(id, "buy", "sell", "user-b", "user-s", 1, 100))
		}(i)
	}
	wg.Wait()

	_, buyerShares := s.Balance("user-b")
	if buyerShares != trades {
		t.Errorf("buyer shares after concurrent trades: want %d, got %d", trades, buyerShares)
	}
}
