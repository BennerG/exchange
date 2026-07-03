package orderbook

import (
	"container/heap"
	"time"
)

type Side int

const (
	Buy Side = iota
	Sell
)

type Order struct {
	ID          string
	UserID      string
	Side        Side
	Quantity    int64
	Filled      int64
	PriceCents  int64
	SubmittedAt time.Time
	index       int // position within its heap slice, maintained by Push, Pop and Swap
}

func (o *Order) Remaining() int64 {
	return o.Quantity - o.Filled
}

// FillResult is the outcome of a single match between two orders.
type FillResult struct {
	BuyOrderID  string
	SellOrderID string
	BuyerID     string
	SellerID    string
	Quantity    int64
	PriceCents  int64
}

// buyHeap is a max-heap by price, then min by time (price-time priority).
type buyHeap []*Order

func (h buyHeap) Len() int { return len(h) }
func (h buyHeap) Less(i, j int) bool {
	if h[i].PriceCents != h[j].PriceCents {
		return h[i].PriceCents > h[j].PriceCents
	}
	return h[i].SubmittedAt.Before(h[j].SubmittedAt)
}
func (h buyHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *buyHeap) Push(x interface{}) {
	order := x.(*Order)
	order.index = len(*h)
	*h = append(*h, order)
}
func (h *buyHeap) Pop() interface{} {
	old := *h
	n := len(old)
	order := old[n-1]
	old[n-1] = nil
	order.index = -1
	*h = old[:n-1]
	return order
}

// sellHeap is a min-heap by price, then min by time (price-time priority).
type sellHeap []*Order

func (h sellHeap) Len() int { return len(h) }
func (h sellHeap) Less(i, j int) bool {
	if h[i].PriceCents != h[j].PriceCents {
		return h[i].PriceCents < h[j].PriceCents
	}
	return h[i].SubmittedAt.Before(h[j].SubmittedAt)
}
func (h sellHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *sellHeap) Push(x interface{}) {
	order := x.(*Order)
	order.index = len(*h)
	*h = append(*h, order)
}
func (h *sellHeap) Pop() interface{} {
	old := *h
	n := len(old)
	order := old[n-1]
	old[n-1] = nil
	order.index = -1
	*h = old[:n-1]
	return order
}

// Book holds the live bid and ask sides, plus a lookup of currently resting
// orders by ID so Cancel does not need to scan either heap.
type Book struct {
	bids    *buyHeap
	asks    *sellHeap
	resting map[string]*Order
}

func New() *Book {
	bids := &buyHeap{}
	asks := &sellHeap{}
	heap.Init(bids)
	heap.Init(asks)
	return &Book{bids: bids, asks: asks, resting: make(map[string]*Order)}
}

// Add inserts an order and immediately attempts to match it.
// Returns zero or more FillResults, one per counterparty matched against.
func (b *Book) Add(incoming *Order) []FillResult {
	var fills []FillResult

	switch incoming.Side {
	case Buy:
		fills = b.matchBuy(incoming)
		if incoming.Remaining() > 0 {
			heap.Push(b.bids, incoming)
			b.resting[incoming.ID] = incoming
		}
	case Sell:
		fills = b.matchSell(incoming)
		if incoming.Remaining() > 0 {
			heap.Push(b.asks, incoming)
			b.resting[incoming.ID] = incoming
		}
	}

	return fills
}

// Cancel removes a resting order by ID. Returns true if found and removed.
func (b *Book) Cancel(orderID string) bool {
	order, ok := b.resting[orderID]
	if !ok {
		return false
	}

	switch order.Side {
	case Buy:
		heap.Remove(b.bids, order.index)
	case Sell:
		heap.Remove(b.asks, order.index)
	}

	delete(b.resting, orderID)
	return true
}

func (b *Book) matchBuy(buy *Order) []FillResult {
	var fills []FillResult
	for b.asks.Len() > 0 && buy.Remaining() > 0 {
		best := (*b.asks)[0]
		if best.PriceCents > buy.PriceCents {
			break
		}
		qty := min(buy.Remaining(), best.Remaining())
		buy.Filled += qty
		best.Filled += qty
		fills = append(fills, FillResult{
			BuyOrderID:  buy.ID,
			SellOrderID: best.ID,
			BuyerID:     buy.UserID,
			SellerID:    best.UserID,
			Quantity:    qty,
			PriceCents:  best.PriceCents,
		})
		if best.Remaining() == 0 {
			heap.Pop(b.asks)
			delete(b.resting, best.ID)
		}
	}
	return fills
}

func (b *Book) matchSell(sell *Order) []FillResult {
	var fills []FillResult
	for b.bids.Len() > 0 && sell.Remaining() > 0 {
		best := (*b.bids)[0]
		if best.PriceCents < sell.PriceCents {
			break
		}
		qty := min(sell.Remaining(), best.Remaining())
		sell.Filled += qty
		best.Filled += qty
		fills = append(fills, FillResult{
			BuyOrderID:  best.ID,
			SellOrderID: sell.ID,
			BuyerID:     best.UserID,
			SellerID:    sell.UserID,
			Quantity:    qty,
			PriceCents:  best.PriceCents,
		})
		if best.Remaining() == 0 {
			heap.Pop(b.bids)
			delete(b.resting, best.ID)
		}
	}
	return fills
}
