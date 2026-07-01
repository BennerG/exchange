package orderbook

import (
	"container/heap"
	"time"
)

type Side int

const (
	Buy  Side = iota
	Sell Side = iota
)

type Order struct {
	ID          string
	UserID      string
	Side        Side
	Quantity    int64
	Filled      int64
	PriceCents  int64
	SubmittedAt time.Time
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
func (h buyHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *buyHeap) Push(x interface{}) { *h = append(*h, x.(*Order)) }
func (h *buyHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
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
func (h sellHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *sellHeap) Push(x interface{}) { *h = append(*h, x.(*Order)) }
func (h *sellHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// Book holds the live bid and ask sides.
type Book struct {
	bids *buyHeap
	asks *sellHeap
}

func New() *Book {
	bids := &buyHeap{}
	asks := &sellHeap{}
	heap.Init(bids)
	heap.Init(asks)
	return &Book{bids: bids, asks: asks}
}

// Add inserts an order and immediately attempts to match it.
// Returns zero or more FillResults (one per counterparty matched against).
func (b *Book) Add(incoming *Order) []FillResult {
	var fills []FillResult

	switch incoming.Side {
	case Buy:
		fills = b.matchBuy(incoming)
		if incoming.Remaining() > 0 {
			heap.Push(b.bids, incoming)
		}
	case Sell:
		fills = b.matchSell(incoming)
		if incoming.Remaining() > 0 {
			heap.Push(b.asks, incoming)
		}
	}

	return fills
}

// Cancel removes an order from the book by ID. Returns true if found and removed.
func (b *Book) Cancel(orderID string) bool {
	for i, o := range *b.bids {
		if o.ID == orderID {
			(*b.bids)[i] = (*b.bids)[b.bids.Len()-1]
			*b.bids = (*b.bids)[:b.bids.Len()-1]
			heap.Init(b.bids)
			return true
		}
	}
	for i, o := range *b.asks {
		if o.ID == orderID {
			(*b.asks)[i] = (*b.asks)[b.asks.Len()-1]
			*b.asks = (*b.asks)[:b.asks.Len()-1]
			heap.Init(b.asks)
			return true
		}
	}
	return false
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
		}
	}
	return fills
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
