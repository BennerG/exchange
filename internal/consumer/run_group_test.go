package consumer

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/IBM/sarama"
)

// consumeFunc adapts a function to sessionRunner, like http.HandlerFunc.
type consumeFunc func(context.Context, []string, sarama.ConsumerGroupHandler) error

func (f consumeFunc) Consume(ctx context.Context, topics []string, h sarama.ConsumerGroupHandler) error {
	return f(ctx, topics, h)
}

func msList(n ...int) []time.Duration {
	out := make([]time.Duration, len(n))
	for i, v := range n {
		out[i] = time.Duration(v) * time.Millisecond
	}
	return out
}

// TestNextBackoff verifies the backoff schedule as plain arithmetic: doubling,
// the cap, the reset after a healthy session, and both sides of the one second
// boundary.
func TestNextBackoff(t *testing.T) {
	tests := []struct {
		name     string
		current  time.Duration
		session  time.Duration
		wantWait time.Duration
		wantNext time.Duration
	}{
		{"first short session waits the initial value", 200 * time.Millisecond, 50 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond},
		{"later short session doubles", 400 * time.Millisecond, 100 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond},
		{"doubling stops at the cap", 4 * time.Second, 100 * time.Millisecond, 4 * time.Second, 5 * time.Second},
		{"stays at the cap", 5 * time.Second, 100 * time.Millisecond, 5 * time.Second, 5 * time.Second},
		{"healthy session resets without waiting", 2 * time.Second, 10 * time.Second, 0, 200 * time.Millisecond},
		{"exactly the healthy threshold counts as healthy", 2 * time.Second, time.Second, 0, 200 * time.Millisecond},
		{"just under the threshold still backs off", 2 * time.Second, 999 * time.Millisecond, 2 * time.Second, 4 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wait, next := nextBackoff(tt.current, tt.session)
			if wait != tt.wantWait || next != tt.wantNext {
				t.Errorf("nextBackoff(%v, %v) = (%v, %v), want (%v, %v)",
					tt.current, tt.session, wait, next, tt.wantWait, tt.wantNext)
			}
		})
	}
}

// TestRunGroupWaits verifies the pauses RunGroup actually takes between
// sessions, measured on synctest's fake clock so no real time passes. The
// final pause is cut short by the cancel that ends each scenario, so it is
// not observed.
func TestRunGroupWaits(t *testing.T) {
	tests := []struct {
		name      string
		sessions  []time.Duration
		err       error
		wantWaits []time.Duration
	}{
		{"short sessions double the wait", msList(10, 10, 10, 10), nil, msList(200, 400, 800)},
		{"a healthy session resets the schedule and skips the pause", msList(10, 10, 5000, 10), nil, msList(200, 400, 0)},
		{"waits are capped", msList(10, 10, 10, 10, 10, 10, 10, 10), nil, msList(200, 400, 800, 1600, 3200, 5000, 5000)},
		{"a Consume error does not change the schedule", msList(10, 10, 10), errors.New("boom"), msList(200, 400)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				var starts []time.Time
				runner := consumeFunc(func(context.Context, []string, sarama.ConsumerGroupHandler) error {
					starts = append(starts, time.Now())
					time.Sleep(tt.sessions[len(starts)-1])
					if len(starts) == len(tt.sessions) {
						cancel()
					}
					return tt.err
				})

				RunGroup(ctx, runner, []string{"orders"}, nil)

				if len(starts) != len(tt.sessions) {
					t.Fatalf("Consume called %d times, want %d", len(starts), len(tt.sessions))
				}
				var waits []time.Duration
				for i := 1; i < len(starts); i++ {
					waits = append(waits, starts[i].Sub(starts[i-1])-tt.sessions[i-1])
				}
				if !slices.Equal(waits, tt.wantWaits) {
					t.Errorf("waits = %v, want %v", waits, tt.wantWaits)
				}
			})
		})
	}
}

// TestRunGroupDoesNotConsumeWhenAlreadyCancelled verifies that a context
// cancelled before the first session never starts one.
func TestRunGroupDoesNotConsumeWhenAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	runner := consumeFunc(func(context.Context, []string, sarama.ConsumerGroupHandler) error {
		calls++
		return nil
	})
	RunGroup(ctx, runner, []string{"orders"}, nil)

	if calls != 0 {
		t.Errorf("Consume called %d times, want 0", calls)
	}
}

// TestRunGroupStopsWhenGroupClosed verifies that a closed group ends the loop
// at once, wrapped or not, instead of retrying a group that can never recover.
// The cancel after three calls only keeps a regression from looping forever.
func TestRunGroupStopsWhenGroupClosed(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"closed group", sarama.ErrClosedConsumerGroup},
		{"wrapped closed group", fmt.Errorf("consume: %w", sarama.ErrClosedConsumerGroup)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				calls := 0
				begin := time.Now()
				runner := consumeFunc(func(context.Context, []string, sarama.ConsumerGroupHandler) error {
					calls++
					if calls == 3 {
						cancel()
					}
					return tt.err
				})

				RunGroup(ctx, runner, []string{"orders"}, nil)

				if calls != 1 {
					t.Errorf("Consume called %d times, want 1", calls)
				}
				if elapsed := time.Since(begin); elapsed != 0 {
					t.Errorf("waited %v before returning, want 0", elapsed)
				}
			})
		})
	}
}

// TestSleepCtx verifies that sleepCtx waits the full duration when left alone
// and returns the moment its context is cancelled. On the fake clock, a missed
// cancel shows up as extra elapsed time instead of a hung test.
func TestSleepCtx(t *testing.T) {
	tests := []struct {
		name        string
		cancelAfter time.Duration
		wantElapsed time.Duration
	}{
		{"runs to completion when never cancelled", -1, time.Hour},
		{"cancel mid wait returns at the cancel", 10 * time.Minute, 10 * time.Minute},
		{"cancel at the start returns at once", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				start := time.Now()
				done := make(chan struct{})
				go func() {
					sleepCtx(ctx, time.Hour)
					close(done)
				}()

				if tt.cancelAfter >= 0 {
					time.Sleep(tt.cancelAfter)
					cancel()
				}
				<-done

				if elapsed := time.Since(start); elapsed != tt.wantElapsed {
					t.Errorf("elapsed = %v, want %v", elapsed, tt.wantElapsed)
				}
			})
		})
	}
}
