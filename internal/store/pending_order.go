package store

import (
	"context"
	"time"
)

type PendingOrder struct {
	OrderID           string
	UserID            string
	Side              string
	QuantityRequested int64
	QuantityFilled    int64
	PriceCents        int64
	CreatedAt         time.Time
}

type PendingUpdate struct {
	OrderID   string
	Remaining int64
}

type PendingOrderStore interface {
	CommitMatch(ctx context.Context, updates []PendingUpdate, deletes []string, insert *PendingOrder) error
	LoadAll(ctx context.Context) ([]PendingOrder, error)
}
