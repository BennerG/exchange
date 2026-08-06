package consumer_test

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"

	"github.com/BennerG/exchange/internal/consumer"
	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
)

// fakeTransactionalPublisher records every transaction control call it
// receives, or returns a configured error for whichever one a test wants
// to fail, letting tests assert on exactly which calls fired and in what
// combination, without a real Kafka transaction coordinator involved.
type fakeTransactionalPublisher struct {
	publishErr    error
	beginErr      error
	addMessageErr error
	commitErr     error
	abortErr      error

	published                        []*pb.Event
	begun, added, committed, aborted int
}

func (f *fakeTransactionalPublisher) Publish(_ context.Context, event *pb.Event) error {
	if f.publishErr != nil {
		return f.publishErr
	}
	f.published = append(f.published, event)
	return nil
}

func (f *fakeTransactionalPublisher) BeginTxn() error {
	f.begun++
	return f.beginErr
}

func (f *fakeTransactionalPublisher) AddMessageToTxn(_ *sarama.ConsumerMessage, _ string, _ *string) error {
	f.added++
	return f.addMessageErr
}

func (f *fakeTransactionalPublisher) CommitTxn() error {
	f.committed++
	return f.commitErr
}

func (f *fakeTransactionalPublisher) AbortTxn() error {
	f.aborted++
	return f.abortErr
}

// TestTransactionalConsumeClaimCommitsOnSuccess verifies the ordinary path:
// BeginTxn, a successful HandleEvent, AddMessageToTxn folding the consumed
// offset in, then CommitTxn, with no AbortTxn and, critically, no direct
// session.MarkMessage or session.Commit call at all, since the offset
// commit happens entirely through the transaction, not through the group
// coordinator directly.
func TestTransactionalConsumeClaimCommitsOnSuccess(t *testing.T) {
	handler := &fakeEventHandler{err: nil}
	pub := &fakeTransactionalPublisher{}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewTransactionalGroupHandler(handler, pub, dlq, "exchange-matcher")

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "orders", Offset: 0, Value: validEventBytes(t),
	})

	if err := gh.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pub.begun != 1 || pub.added != 1 || pub.committed != 1 || pub.aborted != 0 {
		t.Errorf("want begin=1 add=1 commit=1 abort=0, got begin=%d add=%d commit=%d abort=%d",
			pub.begun, pub.added, pub.committed, pub.aborted)
	}
	if session.marked != 0 || session.commits != 0 {
		t.Errorf("want no direct session commit for the transactional path, got %d marks and %d commits",
			session.marked, session.commits)
	}
}

// TestTransactionalConsumeClaimUnmarshalFailureSkipsTransaction verifies
// that a message which never decodes goes straight to the dead letter
// queue via the same direct session commit GroupHandler uses, without
// ever opening a transaction, since nothing was, or ever will be,
// produced for a message that was never successfully read.
func TestTransactionalConsumeClaimUnmarshalFailureSkipsTransaction(t *testing.T) {
	handler := &fakeEventHandler{}
	pub := &fakeTransactionalPublisher{}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewTransactionalGroupHandler(handler, pub, dlq, "exchange-matcher")

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "orders", Offset: 9, Value: []byte{0xFF, 0xFF, 0xFF},
	})

	if err := gh.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pub.begun != 0 {
		t.Errorf("want no transaction opened for an unmarshal failure, got begun=%d", pub.begun)
	}
	if len(dlq.entries) != 1 {
		t.Fatalf("want 1 dead letter entry, got %d", len(dlq.entries))
	}
	if session.marked != 1 || session.commits != 1 {
		t.Errorf("want offset committed directly past the bad message, got %d marks and %d commits",
			session.marked, session.commits)
	}
}

// TestTransactionalConsumeClaimAbortsOnHandleEventFailure verifies that a
// HandleEvent failure aborts the open transaction, never calls
// AddMessageToTxn or CommitTxn, and leaves the offset uncommitted so the
// message redelivers.
func TestTransactionalConsumeClaimAbortsOnHandleEventFailure(t *testing.T) {
	handler := &fakeEventHandler{err: errors.New("publish to fills failed")}
	pub := &fakeTransactionalPublisher{}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewTransactionalGroupHandler(handler, pub, dlq, "exchange-matcher")

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "orders", Offset: 1, Value: validEventBytes(t),
	})

	if err := gh.ConsumeClaim(session, claim); err == nil {
		t.Fatal("want an error returned when HandleEvent fails inside the transaction")
	}
	if pub.begun != 1 || pub.aborted != 1 {
		t.Errorf("want begin=1 abort=1, got begin=%d abort=%d", pub.begun, pub.aborted)
	}
	if pub.added != 0 || pub.committed != 0 {
		t.Errorf("want AddMessageToTxn and CommitTxn never called, got added=%d committed=%d",
			pub.added, pub.committed)
	}
	if session.marked != 0 || session.commits != 0 {
		t.Errorf("want offset left uncommitted, got %d marks and %d commits", session.marked, session.commits)
	}
}

// TestTransactionalConsumeClaimAbortsOnAddMessageFailure verifies that a
// failure folding the consumed offset into the transaction, after
// HandleEvent already succeeded, still aborts cleanly rather than
// committing a transaction with an incomplete offset record.
func TestTransactionalConsumeClaimAbortsOnAddMessageFailure(t *testing.T) {
	handler := &fakeEventHandler{err: nil}
	pub := &fakeTransactionalPublisher{addMessageErr: errors.New("add offsets to txn failed")}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewTransactionalGroupHandler(handler, pub, dlq, "exchange-matcher")

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "orders", Offset: 2, Value: validEventBytes(t),
	})

	if err := gh.ConsumeClaim(session, claim); err == nil {
		t.Fatal("want an error returned when AddMessageToTxn fails")
	}
	if pub.aborted != 1 || pub.committed != 0 {
		t.Errorf("want abort=1 commit=0, got abort=%d commit=%d", pub.aborted, pub.committed)
	}
}

// TestTransactionalConsumeClaimReturnsErrorOnCommitFailure documents the
// current, deliberate behavior when CommitTxn itself fails: the error
// propagates directly, with no explicit AbortTxn call, since the
// transaction is no longer in a state where beginning a fresh abort makes
// sense. The offset stays uncommitted either way, so the message still
// redelivers.
func TestTransactionalConsumeClaimReturnsErrorOnCommitFailure(t *testing.T) {
	handler := &fakeEventHandler{err: nil}
	pub := &fakeTransactionalPublisher{commitErr: errors.New("broker unavailable during commit")}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewTransactionalGroupHandler(handler, pub, dlq, "exchange-matcher")

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "orders", Offset: 3, Value: validEventBytes(t),
	})

	if err := gh.ConsumeClaim(session, claim); err == nil {
		t.Fatal("want an error returned when CommitTxn fails")
	}
	if pub.added != 1 || pub.committed != 1 || pub.aborted != 0 {
		t.Errorf("want added=1 committed=1 aborted=0, got added=%d committed=%d aborted=%d",
			pub.added, pub.committed, pub.aborted)
	}
}
