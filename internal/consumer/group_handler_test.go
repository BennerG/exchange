package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"

	"github.com/BennerG/exchange/internal/consumer"
	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/BennerG/exchange/internal/store"
)

// fakeEventHandler always returns the same configured error, letting each
// test control exactly which of GroupHandler's three routing branches fires.
type fakeEventHandler struct {
	err error
}

func (h *fakeEventHandler) HandleEvent(_ context.Context, _ *pb.Event) error {
	return h.err
}

// fakeDeadLetterPublisher records every entry it receives, or returns a
// configured error, so tests can assert on exactly what got dead-lettered.
type fakeDeadLetterPublisher struct {
	entries []consumer.DeadLetterEntry
	err     error
}

func (d *fakeDeadLetterPublisher) PublishDeadLetter(_ context.Context, entry consumer.DeadLetterEntry) error {
	if d.err != nil {
		return d.err
	}
	d.entries = append(d.entries, entry)
	return nil
}

// fakeSession implements sarama.ConsumerGroupSession, recording every
// MarkMessage and Commit call so tests can verify whether an offset was
// actually advanced, without a real Kafka broker involved at all.
type fakeSession struct {
	ctx     context.Context
	marked  int
	commits int
}

func (s *fakeSession) Claims() map[string][]int32                  { return nil }
func (s *fakeSession) MemberID() string                            { return "test-member" }
func (s *fakeSession) GenerationID() int32                         { return 0 }
func (s *fakeSession) MarkOffset(string, int32, int64, string)     {}
func (s *fakeSession) ResetOffset(string, int32, int64, string)    {}
func (s *fakeSession) MarkMessage(*sarama.ConsumerMessage, string) { s.marked++ }
func (s *fakeSession) Commit()                                     { s.commits++ }
func (s *fakeSession) Context() context.Context                    { return s.ctx }

// fakeClaim implements sarama.ConsumerGroupClaim over a channel the test
// controls directly, standing in for sarama's real, broker-fed channel.
type fakeClaim struct {
	topic     string
	partition int32
	messages  chan *sarama.ConsumerMessage
}

func (c *fakeClaim) Topic() string                            { return c.topic }
func (c *fakeClaim) Partition() int32                         { return c.partition }
func (c *fakeClaim) InitialOffset() int64                     { return 0 }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.messages }

func validEventBytes(t *testing.T) []byte {
	t.Helper()
	b, err := proto.Marshal(&pb.Event{})
	if err != nil {
		t.Fatalf("marshal test event: %v", err)
	}
	return b
}

func newClaimWithMessages(messages ...*sarama.ConsumerMessage) *fakeClaim {
	ch := make(chan *sarama.ConsumerMessage, len(messages))
	for _, m := range messages {
		ch <- m
	}
	close(ch)
	return &fakeClaim{topic: "orders", partition: 0, messages: ch}
}

// TestConsumeClaimCommitsOnSuccess verifies the ordinary path: a valid
// event, a handler that succeeds, results in the offset being marked and
// committed, with nothing routed to the dead letter queue.
func TestConsumeClaimCommitsOnSuccess(t *testing.T) {
	handler := &fakeEventHandler{err: nil}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewGroupHandler(handler, dlq)

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "orders", Offset: 0, Value: validEventBytes(t),
	})

	if err := gh.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if session.marked != 1 || session.commits != 1 {
		t.Errorf("want 1 mark and 1 commit, got %d marks and %d commits", session.marked, session.commits)
	}
	if len(dlq.entries) != 0 {
		t.Errorf("want no dead letter entries, got %d", len(dlq.entries))
	}
}

// TestConsumeClaimRoutesUnmarshalFailureToDLQ verifies that bytes which do
// not decode as a valid Event are dead-lettered and committed past, since
// no amount of redelivery would ever make them decode successfully.
func TestConsumeClaimRoutesUnmarshalFailureToDLQ(t *testing.T) {
	handler := &fakeEventHandler{}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewGroupHandler(handler, dlq)

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "orders", Partition: 0, Offset: 42, Value: []byte{0xFF, 0xFF, 0xFF},
	})

	if err := gh.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if session.marked != 1 || session.commits != 1 {
		t.Errorf("want the offset committed past the bad message, got %d marks and %d commits", session.marked, session.commits)
	}
	if len(dlq.entries) != 1 {
		t.Fatalf("want 1 dead letter entry, got %d", len(dlq.entries))
	}
	if dlq.entries[0].Offset != 42 || dlq.entries[0].SourceTopic != "orders" {
		t.Errorf("dead letter entry missing correct topic or offset: %+v", dlq.entries[0])
	}
}

// TestConsumeClaimRoutesPermanentErrorToDLQ verifies that a HandleEvent
// failure wrapping store.ErrPermanent is dead-lettered and committed past,
// the same as an unmarshal failure, since both are guaranteed to fail
// identically on every retry.
func TestConsumeClaimRoutesPermanentErrorToDLQ(t *testing.T) {
	permErr := fmt.Errorf("insert transaction: %w: invalid input syntax for type uuid", store.ErrPermanent)
	handler := &fakeEventHandler{err: permErr}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewGroupHandler(handler, dlq)

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "fills", Offset: 7, Value: validEventBytes(t),
	})

	if err := gh.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if session.commits != 1 {
		t.Errorf("want the offset committed past the permanent failure, got %d commits", session.commits)
	}
	if len(dlq.entries) != 1 {
		t.Fatalf("want 1 dead letter entry, got %d", len(dlq.entries))
	}
}

// TestConsumeClaimLeavesTransientErrorUncommitted verifies that a
// HandleEvent failure not wrapping store.ErrPermanent is left uncommitted
// and returned as an error, so sarama redelivers it rather than routing a
// possibly-recoverable failure straight to the DLQ.
func TestConsumeClaimLeavesTransientErrorUncommitted(t *testing.T) {
	transientErr := errors.New("connection refused")
	handler := &fakeEventHandler{err: transientErr}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewGroupHandler(handler, dlq)

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "fills", Offset: 3, Value: validEventBytes(t),
	})

	err := gh.ConsumeClaim(session, claim)
	if err == nil {
		t.Fatal("want an error returned for a transient failure")
	}
	if session.marked != 0 || session.commits != 0 {
		t.Errorf("want offset left uncommitted, got %d marks and %d commits", session.marked, session.commits)
	}
	if len(dlq.entries) != 0 {
		t.Errorf("want no dead letter entries for a transient failure, got %d", len(dlq.entries))
	}
}

// TestConsumeClaimLeavesOffsetUncommittedWhenDLQPublishFails verifies that
// if publishing to the dead letter queue itself fails, the original
// offset stays uncommitted, so the message redelivers and gets a fresh
// attempt at reaching the DLQ, rather than being silently lost.
func TestConsumeClaimLeavesOffsetUncommittedWhenDLQPublishFails(t *testing.T) {
	handler := &fakeEventHandler{}
	dlq := &fakeDeadLetterPublisher{err: errors.New("dlq broker unreachable")}
	gh := consumer.NewGroupHandler(handler, dlq)

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "orders", Offset: 1, Value: []byte{0xFF, 0xFF, 0xFF},
	})

	if err := gh.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if session.marked != 0 || session.commits != 0 {
		t.Errorf("want offset left uncommitted when dlq publish fails, got %d marks and %d commits", session.marked, session.commits)
	}
}
