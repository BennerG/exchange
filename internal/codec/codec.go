package codec

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
)

// ErrPermanent marks bytes that fail to decode identically on every retry.
// Check with errors.Is. Any Decode error that does not wrap it is transient.
var ErrPermanent = errors.New("permanent decode failure")

// EventCodec converts between Events and Kafka message values.
type EventCodec interface {
	Encode(topic string, event *pb.Event) ([]byte, error)
	Decode(topic string, data []byte) (*pb.Event, error)
}

// Plain is raw Protobuf with no framing and no registry.
type Plain struct{}

var _ EventCodec = Plain{}

func (Plain) Encode(_ string, event *pb.Event) ([]byte, error) {
	b, err := proto.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal event: %w", err)
	}
	return b, nil
}

func (Plain) Decode(_ string, data []byte) (*pb.Event, error) {
	var event pb.Event
	if err := proto.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("unmarshal event: %w: %v", ErrPermanent, err)
	}
	return &event, nil
}
