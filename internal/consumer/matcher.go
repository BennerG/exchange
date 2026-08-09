package consumer

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/BennerG/exchange/internal/gen/proto/trading/events"
	"github.com/BennerG/exchange/internal/orderbook"
)

// Matcher holds one stock's live order book. It never publishes anything
// and never finalizes a match itself, both are deliberately left to
// whatever calls Prepare, so a match can be reversed if the caller
// later finds it cannot actually be published and committed.
type Matcher struct {
	book *orderbook.Book
}

func NewMatcher() *Matcher {
	return &Matcher{book: orderbook.New()}
}

func (m *Matcher) Prepare(_ context.Context, event *pb.Event) ([]*pb.Event, func(), func(), error) {
	orderSubmitted := event.GetOrderSubmitted()
	if orderSubmitted == nil {
		return nil, func() {}, func() {}, nil
	}

	order := &orderbook.Order{
		ID:          orderSubmitted.GetOrderId(),
		UserID:      orderSubmitted.GetUserId(),
		Side:        toOrderbookSide(orderSubmitted.GetSide()),
		Quantity:    orderSubmitted.GetQuantity(),
		PriceCents:  orderSubmitted.GetPricePerShare().GetAmountCents(),
		SubmittedAt: orderSubmitted.GetSubmittedAt().AsTime(),
	}

	outcome := m.book.Match(order)

	toPublish := make([]*pb.Event, 0, len(outcome.Fills))
	for _, fill := range outcome.Fills {
		toPublish = append(toPublish, &pb.Event{
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
		})
	}

	commit := func() { m.book.Apply(outcome) }
	discard := func() { m.book.Discard(outcome) }

	return toPublish, commit, discard, nil
}

func toOrderbookSide(side pb.OrderSide) orderbook.Side {
	if side == pb.OrderSide_SELL {
		return orderbook.Sell
	}
	return orderbook.Buy
}
