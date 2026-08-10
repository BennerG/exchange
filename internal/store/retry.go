package store

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// RetryingPendingOrderStore wraps a PendingOrderStore and retries
// CommitMatch with exponential backoff before giving up, on the
// assumption this failure is transient, a momentary database blip, not
// a structural one, since CommitMatch's writes never violate a
// constraint the way a malformed settlement could.
type RetryingPendingOrderStore struct {
	inner       PendingOrderStore
	maxAttempts int
	baseBackoff time.Duration
}

func NewRetryingPendingOrderStore(inner PendingOrderStore, maxAttempts int, baseBackoff time.Duration) *RetryingPendingOrderStore {
	return &RetryingPendingOrderStore{inner: inner, maxAttempts: maxAttempts, baseBackoff: baseBackoff}
}

func (r *RetryingPendingOrderStore) CommitMatch(ctx context.Context, updates []PendingUpdate, deletes []string, inserts []PendingOrder) error {
	backoff := r.baseBackoff
	var lastErr error

	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		lastErr = r.inner.CommitMatch(ctx, updates, deletes, inserts)
		if lastErr == nil {
			return nil
		}
		log.Warn().Err(lastErr).Int("attempt", attempt).Int("max_attempts", r.maxAttempts).
			Msg("pending order persistence failed, retrying")

		if attempt == r.maxAttempts {
			break
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during retry backoff: %w", ctx.Err())
		}
		backoff *= 2
	}

	return fmt.Errorf("pending order persistence failed after %d attempts: %w", r.maxAttempts, lastErr)
}

func (r *RetryingPendingOrderStore) LoadAll(ctx context.Context) ([]PendingOrder, error) {
	return r.inner.LoadAll(ctx)
}
