package store

import (
	"context"
	"errors"
	"sync"
)

// ErrAlreadySettled is returned when a trade's ID has already been recorded.
// Callers should treat this as a successful no-op, not a failure, since it
// signals a safe redelivery rather than a genuine settlement problem.
var ErrAlreadySettled = errors.New("trade already settled")

// Trade is one completed match between a buyer and a seller, ready to be
// recorded against both accounts atomically.
type Trade struct {
	TradeID      string
	BuyOrderID   string
	SellOrderID  string
	BuyerUserID  string
	SellerUserID string
	Quantity     int64
	PriceCents   int64
}

// Store settles trades into the ledger with exactly-once semantics per TradeID.
type Store interface {
	SettleFill(ctx context.Context, trade Trade) error
}

type account struct {
	cashCents int64
	sharesQQQ int64
}

// MemoryStore is an in-memory Store used for fast tests without a live database.
// It implements the same contract the Postgres implementation will honor.
type MemoryStore struct {
	mu       sync.Mutex
	accounts map[string]*account
	settled  map[string]struct{}
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		accounts: make(map[string]*account),
		settled:  make(map[string]struct{}),
	}
}

func (s *MemoryStore) SettleFill(_ context.Context, trade Trade) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.settled[trade.TradeID]; ok {
		return ErrAlreadySettled
	}

	amountCents := trade.Quantity * trade.PriceCents

	buyer := s.accountFor(trade.BuyerUserID)
	seller := s.accountFor(trade.SellerUserID)

	buyer.cashCents -= amountCents
	buyer.sharesQQQ += trade.Quantity
	seller.cashCents += amountCents
	seller.sharesQQQ -= trade.Quantity

	s.settled[trade.TradeID] = struct{}{}
	return nil
}

func (s *MemoryStore) accountFor(userID string) *account {
	a, ok := s.accounts[userID]
	if !ok {
		a = &account{}
		s.accounts[userID] = a
	}
	return a
}

// Balance returns a snapshot of one account's holdings, for tests and debugging.
func (s *MemoryStore) Balance(userID string) (cashCents, sharesQQQ int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.accounts[userID]
	if !ok {
		return 0, 0
	}
	return a.cashCents, a.sharesQQQ
}
