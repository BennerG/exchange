package store_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/BennerG/exchange/internal/store"
)

var testStore *store.PostgresStore

func TestMain(m *testing.M) {
	ctx := context.Background()

	migrationPath, err := filepath.Abs(filepath.Join("..", "..", "migrations", "001_initial_schema.sql"))
	if err != nil {
		panic(err)
	}

	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("exchange_test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithInitScripts(migrationPath),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		panic(err)
	}

	connString, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic(err)
	}

	s, err := store.NewPostgresStore(ctx, connString)
	if err != nil {
		panic(err)
	}
	testStore = s

	code := m.Run()

	testStore.Close()
	container.Terminate(ctx)
	os.Exit(code)
}

func newTrade(buyerID, sellerID string, qty, priceCents int64) store.Trade {
	return store.Trade{
		TradeID:      uuid.New().String(),
		BuyOrderID:   uuid.New().String(),
		SellOrderID:  uuid.New().String(),
		BuyerUserID:  buyerID,
		SellerUserID: sellerID,
		Quantity:     qty,
		PriceCents:   priceCents,
	}
}

// TestSettleFillPersistsBalances verifies a settled trade is durably
// reflected in both accounts, not just accepted without error.
func TestSettleFillPersistsBalances(t *testing.T) {
	ctx := context.Background()
	buyer, seller := uuid.New().String(), uuid.New().String()

	tr := newTrade(buyer, seller, 100, 15000)
	if err := testStore.SettleFill(ctx, tr); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	buyerCash, buyerShares, err := testStore.Balance(ctx, buyer)
	if err != nil {
		t.Fatalf("balance lookup: %v", err)
	}
	if buyerCash != -1_500_000 || buyerShares != 100 {
		t.Errorf("buyer: want cash -1500000 shares 100, got cash %d shares %d", buyerCash, buyerShares)
	}

	sellerCash, sellerShares, err := testStore.Balance(ctx, seller)
	if err != nil {
		t.Fatalf("balance lookup: %v", err)
	}
	if sellerCash != 1_500_000 || sellerShares != -100 {
		t.Errorf("seller: want cash 1500000 shares -100, got cash %d shares %d", sellerCash, sellerShares)
	}
}

// TestPostgresSettleFillIsIdempotent verifies the unique constraint on trade_id
// causes a redelivered trade to return ErrAlreadySettled without applying
// the balance change a second time.
func TestPostgresSettleFillIsIdempotent(t *testing.T) {
	ctx := context.Background()
	buyer, seller := uuid.New().String(), uuid.New().String()
	tr := newTrade(buyer, seller, 50, 20000)

	if err := testStore.SettleFill(ctx, tr); err != nil {
		t.Fatalf("first settle: unexpected error: %v", err)
	}

	err := testStore.SettleFill(ctx, tr)
	if !errors.Is(err, store.ErrAlreadySettled) {
		t.Fatalf("second settle: want ErrAlreadySettled, got %v", err)
	}

	buyerCash, _, err := testStore.Balance(ctx, buyer)
	if err != nil {
		t.Fatalf("balance lookup: %v", err)
	}
	if buyerCash != -1_000_000 {
		t.Errorf("buyer cash after duplicate: want -1000000, got %d", buyerCash)
	}
}

// TestSettleFillConcurrentDuplicate settles the exact same trade from two
// goroutines at once. Unlike MemoryStore's concurrency test, this is not
// proving the Go mutex works, there is none here, it is proving the
// database's own unique constraint is what actually prevents a genuine
// race between two processes from double settling the same trade.
func TestSettleFillConcurrentDuplicate(t *testing.T) {
	ctx := context.Background()
	buyer, seller := uuid.New().String(), uuid.New().String()
	tr := newTrade(buyer, seller, 10, 30000)

	var wg sync.WaitGroup
	results := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = testStore.SettleFill(ctx, tr)
		}(i)
	}
	wg.Wait()

	successes, duplicates := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, store.ErrAlreadySettled):
			duplicates++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if successes != 1 || duplicates != 1 {
		t.Errorf("want 1 success and 1 duplicate, got %d successes and %d duplicates", successes, duplicates)
	}

	buyerCash, _, err := testStore.Balance(ctx, buyer)
	if err != nil {
		t.Fatalf("balance lookup: %v", err)
	}
	if buyerCash != -300_000 {
		t.Errorf("buyer cash after concurrent duplicate: want -300000, got %d", buyerCash)
	}
}
