package consumer

import (
	"context"
	"fmt"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/BennerG/exchange/internal/orderbook"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Publisher sends a serialized event to the fills topic.
type Publisher interface {
	Publish(ctx context.Context, event *pb.Event) error
}

// Matcher holds one stock's live order book and publishes a Filled event
// for every match produced when a new order is added.
type Matcher struct {
	book *orderbook.Book
	pub  Publisher
}

func NewMatcher(pub Publisher) *Matcher {
	return &Matcher{
		book: orderbook.New(),
		pub:  pub,
	}
}

func (m *Matcher) HandleEvent(ctx context.Context, event *pb.Event) error {
	orderSubmitted := event.GetOrderSubmitted()
	if orderSubmitted == nil {
		return nil
	}

	order := &orderbook.Order{
		ID:          orderSubmitted.OrderId,
		UserID:      orderSubmitted.UserId,
		Side:        toOrderbookSide(orderSubmitted.Side),
		Quantity:    orderSubmitted.Quantity,
		PriceCents:  orderSubmitted.PricePerShare.AmountCents,
		SubmittedAt: orderSubmitted.SubmittedAt.AsTime(),
	}

	fills := m.book.Add(order)

	for _, fill := range fills {
		fillEvent := &pb.Event{
			Payload: &pb.Event_Filled{
				Filled: &pb.Filled{
					TradeId:      uuid.New().String(),
					BuyOrderId:   fill.BuyOrderID,
					SellOrderId:  fill.SellOrderID,
					BuyerUserId:  fill.BuyerID,
					SellerUserId: fill.SellerID,
					Quantity:     fill.Quantity,
					Price: &pb.Money{
						AmountCents: fill.PriceCents,
						Currency:    "USD",
					},
					FilledAt: timestamppb.Now(),
				},
			},
		}

		if err := m.pub.Publish(ctx, fillEvent); err != nil {
			return fmt.Errorf("publish fill: %w", err)
		}
	}

	return nil
}

func toOrderbookSide(side pb.OrderSide) orderbook.Side {
	if side == pb.OrderSide_SELL {
		return orderbook.Sell
	}
	return orderbook.Buy
}
