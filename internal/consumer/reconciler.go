package consumer

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/BennerG/exchange/internal/store"
)

// Reconciler periodically compares a matcher's in-memory resting orders
// against what is persisted, correcting drift in Postgres. Memory is
// always treated as correct: it already reflects every Kafka transaction
// that has actually committed. Postgres is the copy being kept honest.
type Reconciler struct {
	snapshotFunc func() []store.PendingOrder
	pending      store.PendingOrderStore
	interval     time.Duration
}

func NewReconciler(snapshotFunc func() []store.PendingOrder, pending store.PendingOrderStore, interval time.Duration) *Reconciler {
	return &Reconciler{snapshotFunc: snapshotFunc, pending: pending, interval: interval}
}

func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.reconcileOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (r *Reconciler) reconcileOnce(ctx context.Context) {
	memory := r.snapshotFunc()
	persisted, err := r.pending.LoadAll(ctx)
	if err != nil {
		log.Error().Err(err).Msg("reconciliation: failed to load persisted pending orders")
		return
	}

	memByID := make(map[string]store.PendingOrder, len(memory))
	for _, o := range memory {
		memByID[o.OrderID] = o
	}
	persistedByID := make(map[string]store.PendingOrder, len(persisted))
	for _, o := range persisted {
		persistedByID[o.OrderID] = o
	}

	var inserts []store.PendingOrder
	var updates []store.PendingUpdate
	var deletes []string

	for id, m := range memByID {
		p, ok := persistedByID[id]
		switch {
		case !ok:
			inserts = append(inserts, m)
			log.Warn().Str("order_id", id).Msg("reconciliation: resting order missing from pending_orders, inserting")
		case p.QuantityFilled != m.QuantityFilled:
			updates = append(updates, store.PendingUpdate{
				OrderID:   id,
				Remaining: m.QuantityRequested - m.QuantityFilled,
			})
			log.Warn().Str("order_id", id).Msg("reconciliation: quantity_filled drift found, correcting")
		}
	}
	for id := range persistedByID {
		if _, ok := memByID[id]; !ok {
			deletes = append(deletes, id)
			log.Warn().Str("order_id", id).Msg("reconciliation: stale pending_orders row no longer resting, deleting")
		}
	}

	if len(inserts) == 0 && len(updates) == 0 && len(deletes) == 0 {
		return
	}
	if err := r.pending.CommitMatch(ctx, updates, deletes, inserts); err != nil {
		log.Error().Err(err).Msg("reconciliation: failed to apply corrections")
	}
}
