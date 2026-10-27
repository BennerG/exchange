package consumer

import (
	"context"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
)

const (
	initialBackoff = 200 * time.Millisecond
	maxBackoff     = 5 * time.Second
	healthySession = time.Second
)

// sessionRunner is the one method of sarama.ConsumerGroup that RunGroup needs.
type sessionRunner interface {
	Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error
}

// RunGroup joins the group and consumes until ctx is cancelled, rejoining
// after each session ends. Rapid session failures back off exponentially.
func RunGroup(ctx context.Context, group sessionRunner, topics []string, handler sarama.ConsumerGroupHandler) {
	runGroup(ctx, group, topics, handler, time.Now, sleepCtx)
}

// Sarama reports a failing ConsumeClaim through the Errors channel and
// Consume returns nil, so a short session is the only failure signal here.
func runGroup(ctx context.Context, group sessionRunner, topics []string, handler sarama.ConsumerGroupHandler,
	now func() time.Time, sleep func(context.Context, time.Duration) bool) {
	backoff := initialBackoff
	for ctx.Err() == nil {
		start := now()
		if err := group.Consume(ctx, topics, handler); err != nil {
			log.Error().Err(err).Msg("consumer group session ended")
		}

		var wait time.Duration
		wait, backoff = nextBackoff(backoff, now().Sub(start))
		if wait > 0 && !sleep(ctx, wait) {
			return
		}
	}
}

// nextBackoff returns how long to wait now and the backoff to carry forward.
func nextBackoff(current, sessionLen time.Duration) (wait, next time.Duration) {
	if sessionLen >= healthySession {
		return 0, initialBackoff
	}
	return current, min(current*2, maxBackoff)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
