package consumer

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

// scriptedRunner plays back session lengths on a fake clock and cancels the
// context after the last one, so the loop under test terminates on its own.
type scriptedRunner struct {
	clock    *fakeClock
	sessions []time.Duration
	err      error
	cancel   context.CancelFunc
	calls    int
}

func (r *scriptedRunner) Consume(context.Context, []string, sarama.ConsumerGroupHandler) error {
	r.clock.t = r.clock.t.Add(r.sessions[r.calls])
	r.calls++
	if r.calls == len(r.sessions) {
		r.cancel()
	}
	return r.err
}

func msList(n ...int) []time.Duration {
	out := make([]time.Duration, len(n))
	for i, v := range n {
		out[i] = time.Duration(v) * time.Millisecond
	}
	return out
}

// Pure schedule logic, including both sides of the healthy-session boundary.
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

// Drives the real loop with a fake clock and a recording sleep, so the
// waits it asks for are asserted without any real time passing.
func TestRunGroupWaits(t *testing.T) {
	tests := []struct {
		name      string
		sessions  []time.Duration
		err       error
		wantWaits []time.Duration
	}{
		{"short sessions double the wait", msList(10, 10, 10, 10), nil, msList(200, 400, 800, 1600)},
		{"a healthy session resets the schedule and skips the sleep", []time.Duration{10 * time.Millisecond, 10 * time.Millisecond, 5 * time.Second, 10 * time.Millisecond}, nil, msList(200, 400, 200)},
		{"waits are capped", msList(10, 10, 10, 10, 10, 10, 10, 10), nil, msList(200, 400, 800, 1600, 3200, 5000, 5000, 5000)},
		{"a Consume error does not change the schedule", msList(10, 10), errors.New("boom"), msList(200, 400)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			clock := &fakeClock{t: time.Unix(0, 0)}
			runner := &scriptedRunner{clock: clock, sessions: tt.sessions, err: tt.err, cancel: cancel}

			var waits []time.Duration
			sleep := func(ctx context.Context, d time.Duration) bool {
				waits = append(waits, d)
				return ctx.Err() == nil
			}

			runGroup(ctx, runner, []string{"orders"}, nil, clock.now, sleep)

			if runner.calls != len(tt.sessions) {
				t.Errorf("Consume called %d times, want %d", runner.calls, len(tt.sessions))
			}
			if !slices.Equal(waits, tt.wantWaits) {
				t.Errorf("waits = %v, want %v", waits, tt.wantWaits)
			}
		})
	}
}

// A context cancelled before the first session must not start one.
func TestRunGroupDoesNotConsumeWhenAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	runner := &scriptedRunner{clock: &fakeClock{t: time.Unix(0, 0)}, cancel: cancel}
	runGroup(ctx, runner, []string{"orders"}, nil, time.Now, sleepCtx)

	if runner.calls != 0 {
		t.Errorf("Consume called %d times, want 0", runner.calls)
	}
}

// Shutdown must interrupt a long wait instead of blocking for it.
func TestSleepCtx(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		ctx  context.Context
		d    time.Duration
		want bool
	}{
		{"completes when the context stays live", context.Background(), time.Millisecond, true},
		{"returns false at once when cancelled", cancelled, time.Hour, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sleepCtx(tt.ctx, tt.d); got != tt.want {
				t.Errorf("sleepCtx = %v, want %v", got, tt.want)
			}
		})
	}
}
