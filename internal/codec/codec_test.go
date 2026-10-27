package codec_test

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/BennerG/exchange/internal/codec"
	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
)

var testTime = timestamppb.New(time.Unix(1_700_000_000, 0))

// One case per oneof arm, so a wrong arm mapping in any codec shows up here.
func TestPlainRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		event *pb.Event
	}{
		{"order submitted", &pb.Event{Payload: &pb.Event_OrderSubmitted{OrderSubmitted: &pb.OrderSubmitted{
			OrderId: "o-1", UserId: "u-1", Quantity: 10,
			PricePerShare: &pb.Money{AmountCents: 12345, Currency: "USD"},
			Side:          pb.OrderSide_SELL, SubmittedAt: testTime,
		}}}},
		{"filled", &pb.Event{Payload: &pb.Event_Filled{Filled: &pb.Filled{
			TradeId: "t-1", BuyOrderId: "o-1", SellOrderId: "o-2",
			BuyerUserId: "u-1", SellerUserId: "u-2", Quantity: 5,
			Price: &pb.Money{AmountCents: 100, Currency: "USD"}, FilledAt: testTime,
		}}}},
		{"rejected", &pb.Event{Payload: &pb.Event_Rejected{Rejected: &pb.Rejected{
			OrderId: "o-3", Reason: pb.RejectionReason_INSUFFICIENT_SHARES,
			Message: "not enough shares", RejectedAt: testTime,
		}}}},
		{"order cancelled", &pb.Event{Payload: &pb.Event_OrderCancelled{OrderCancelled: &pb.OrderCancelled{
			OrderId: "o-4", UserId: "u-4", CancelledQuantity: 3, CancelledAt: testTime,
		}}}},
	}

	c := codec.Plain{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := c.Encode("orders", tt.event)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			got, err := c.Decode("orders", b)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if !proto.Equal(tt.event, got) {
				t.Errorf("round trip mismatch:\nwant %v\ngot  %v", tt.event, got)
			}
		})
	}
}

// Each case is a different way bytes can be unparseable. All must be
// permanent, since redelivering the same bytes can never succeed.
func TestPlainDecodeUnparseableIsPermanent(t *testing.T) {
	valid, err := codec.Plain{}.Encode("fills", &pb.Event{Payload: &pb.Event_Filled{
		Filled: &pb.Filled{TradeId: "trade-1"},
	}})
	if err != nil {
		t.Fatalf("encode fixture: %v", err)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{"garbage bytes", []byte{0xFF, 0xFF, 0xFF}},
		{"truncated mid field", valid[:len(valid)-1]},
		{"field number zero", []byte{0x00, 0x00}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := codec.Plain{}.Decode("fills", tt.data)
			if !errors.Is(err, codec.ErrPermanent) {
				t.Fatalf("want ErrPermanent, got %v", err)
			}
			if got != nil {
				t.Errorf("want nil event on error, got %v", got)
			}
		})
	}
}

// Existing handler tests use empty bytes as their "valid event" fixture, so
// Plain must keep treating them as a valid empty Event.
func TestPlainDecodeEmptyBytesIsEmptyEvent(t *testing.T) {
	got, err := codec.Plain{}.Decode("orders", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !proto.Equal(got, &pb.Event{}) {
		t.Errorf("want empty event, got %v", got)
	}
}

// Go treats a wrong wire type as an unknown field, not corrupt data.
func TestPlainDecodeMismatchedWireTypeIsLenient(t *testing.T) {
	got, err := codec.Plain{}.Decode("orders", []byte{0x08, 0x01})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.GetPayload() != nil {
		t.Errorf("want no payload set, got %v", got)
	}
}
