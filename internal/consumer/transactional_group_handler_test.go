package consumer_test

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"

	"github.com/BennerG/exchange/internal/consumer"
	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
)

// fakeTransactionalEventHandler lets each test control exactly what
// Prepare returns, and records whether commit or discard actually ran,
// which is the entire behavior this handler exists to get right.
type fakeTransactionalEventHandler struct {
	toPublish []*pb.Event
	err       error
	commitErr error

	committed bool
	discarded bool
}

func (f *fakeTransactionalEventHandler) Prepare(_ context.Context, _ *pb.Event) ([]*pb.Event, func() error, func(), error) {
	if f.err != nil {
		return nil, nil, nil, f.err
	}
	commit := func() error { f.committed = true; return f.commitErr }
	discard := func() { f.discarded = true }
	return f.toPublish, commit, discard, nil
}

// fakeTransactionalPublisher records every transaction control call it
// receives, or returns a configured error for whichever one a test wants
// to fail.
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

func fillPayload() *pb.Event {
	return &pb.Event{Payload: &pb.Event_Filled{Filled: &pb.Filled{TradeId: "trade-1"}}}
}

// TestTransactionalConsumeClaimCommitsOnSuccess verifies the ordinary
// path: BeginTxn, every event published, AddMessageToTxn, CommitTxn, and
// only then commit, never discard.
func TestTransactionalConsumeClaimCommitsOnSuccess(t *testing.T) {
	handler := &fakeTransactionalEventHandler{toPublish: []*pb.Event{fillPayload()}}
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
	if len(pub.published) != 1 {
		t.Errorf("want 1 event published, got %d", len(pub.published))
	}
	if !handler.committed || handler.discarded {
		t.Error("want the matcher's commit called, not discard")
	}
}

// TestTransactionalConsumeClaimDiscardsOnPublishFailure verifies that a
// publish failure discards the matcher's in-memory match, aborts the
// Kafka transaction, and never calls commit.
func TestTransactionalConsumeClaimDiscardsOnPublishFailure(t *testing.T) {
	handler := &fakeTransactionalEventHandler{toPublish: []*pb.Event{fillPayload()}}
	pub := &fakeTransactionalPublisher{publishErr: errors.New("broker unavailable")}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewTransactionalGroupHandler(handler, pub, dlq, "exchange-matcher")

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "orders", Offset: 1, Value: validEventBytes(t),
	})

	if err := gh.ConsumeClaim(session, claim); err == nil {
		t.Fatal("want an error when publish fails")
	}
	if pub.aborted != 1 || pub.committed != 0 {
		t.Errorf("want abort=1 commit=0, got abort=%d commit=%d", pub.aborted, pub.committed)
	}
	if !handler.discarded || handler.committed {
		t.Error("want the matcher's discard called, not commit")
	}
}

// TestTransactionalConsumeClaimDiscardsOnCommitTxnFailure verifies the
// specific scenario this whole redesign closes: every publish succeeds,
// but CommitTxn itself fails, and the matcher's discard still runs, so
// the book is never left reflecting a match Kafka never finalized.
func TestTransactionalConsumeClaimDiscardsOnCommitTxnFailure(t *testing.T) {
	handler := &fakeTransactionalEventHandler{toPublish: []*pb.Event{fillPayload()}}
	pub := &fakeTransactionalPublisher{commitErr: errors.New("broker unavailable during commit")}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewTransactionalGroupHandler(handler, pub, dlq, "exchange-matcher")

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "orders", Offset: 2, Value: validEventBytes(t),
	})

	if err := gh.ConsumeClaim(session, claim); err == nil {
		t.Fatal("want an error when CommitTxn fails")
	}
	if !handler.discarded || handler.committed {
		t.Error("want discard called even though every publish succeeded, since the commit itself failed")
	}
	if pub.aborted != 0 {
		t.Errorf("want no AbortTxn call after a failed CommitTxn, got %d", pub.aborted)
	}
}

// TestTransactionalConsumeClaimDiscardsOnPrepareThenBeginTxnFailure
// verifies that a BeginTxn failure still discards, even though nothing
// Kafka-related ever started, since Prepare's internal mutation already
// happened before BeginTxn was ever called.
func TestTransactionalConsumeClaimDiscardsOnBeginTxnFailure(t *testing.T) {
	handler := &fakeTransactionalEventHandler{toPublish: []*pb.Event{fillPayload()}}
	pub := &fakeTransactionalPublisher{beginErr: errors.New("producer fenced")}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewTransactionalGroupHandler(handler, pub, dlq, "exchange-matcher")

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "orders", Offset: 3, Value: validEventBytes(t),
	})

	if err := gh.ConsumeClaim(session, claim); err == nil {
		t.Fatal("want an error when BeginTxn fails")
	}
	if !handler.discarded || handler.committed {
		t.Error("want discard called even though BeginTxn never succeeded")
	}
	if pub.published != nil {
		t.Error("want nothing published when BeginTxn fails first")
	}
}

// TestTransactionalConsumeClaimUnmarshalFailureNeverCallsPrepare verifies
// that a message which never decodes goes straight to the dead letter
// queue without ever reaching Prepare, so there is nothing to commit or
// discard at all.
func TestTransactionalConsumeClaimUnmarshalFailureNeverCallsPrepare(t *testing.T) {
	handler := &fakeTransactionalEventHandler{}
	pub := &fakeTransactionalPublisher{}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewTransactionalGroupHandler(handler, pub, dlq, "exchange-matcher")

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{
		Topic: "orders", Offset: 4, Value: []byte{0xFF, 0xFF, 0xFF},
	})

	if err := gh.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handler.committed || handler.discarded {
		t.Error("want neither commit nor discard called, Prepare should never have been reached")
	}
	if pub.begun != 0 {
		t.Errorf("want no transaction opened for an unmarshal failure, got begun=%d", pub.begun)
	}
	if len(dlq.entries) != 1 {
		t.Fatalf("want 1 dead letter entry, got %d", len(dlq.entries))
	}
}

// TestTransactionalConsumeClaimCommitFailureAfterCommitTxnDoesNotReturnError
// verifies the exact scenario this design exists for: CommitTxn succeeds,
// the persistence call inside commit then fails, and ConsumeClaim still
// returns nil, since Kafka has already committed and there is nothing
// left to retry.
func TestTransactionalConsumeClaimCommitFailureAfterCommitTxnDoesNotReturnError(t *testing.T) {
	handler := &fakeTransactionalEventHandler{
		toPublish: []*pb.Event{fillPayload()},
		commitErr: errors.New("pending order write failed"),
	}
	pub := &fakeTransactionalPublisher{}
	dlq := &fakeDeadLetterPublisher{}
	gh := consumer.NewTransactionalGroupHandler(handler, pub, dlq, "exchange-matcher")

	session := &fakeSession{ctx: context.Background()}
	claim := newClaimWithMessages(&sarama.ConsumerMessage{Topic: "orders", Offset: 5, Value: validEventBytes(t)})

	if err := gh.ConsumeClaim(session, claim); err != nil {
		t.Fatalf("want no error even though commit's persistence failed, got %v", err)
	}
	if pub.committed != 1 || pub.aborted != 0 {
		t.Errorf("want the kafka transaction to have committed successfully regardless, got committed=%d aborted=%d", pub.committed, pub.aborted)
	}
}
