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
	index       int
}

func (o *Order) Remaining() int64 {
	return o.Quantity - o.Filled
}

// FillResult is the outcome of a single match between two orders.
// BuyRemaining and SellRemaining record each side's remaining quantity
// immediately after this fill, which is what lets a caller, and Discard,
// tell without any further lookup whether a given side was fully
// consumed by this specific match.
type FillResult struct {
	BuyOrderID    string
	SellOrderID   string
	BuyerID       string
	SellerID      string
	Quantity      int64
	PriceCents    int64
	BuyRemaining  int64
	SellRemaining int64
}

// MatchOutcome is the result of Match: the fills it produced, plus enough
// bookkeeping to either finalize the incoming order via Apply or fully
// reverse every mutation Match made via Discard. Exactly one of Apply or
// Discard must be called for a given MatchOutcome. Calling neither leaves
// the book permanently, silently mutated by an attempt nothing ever
// finished deciding about.
type MatchOutcome struct {
	incoming     *Order
	Fills        []FillResult
	poppedOrders []*Order
}

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

// Add matches incoming against the book and, if it is not fully consumed,
// rests it immediately. It is equivalent to Match followed immediately by
// Apply, for callers with no need to inspect fills before committing
// them, which today means every existing test in this package.
func (b *Book) Add(incoming *Order) []FillResult {
	outcome := b.Match(incoming)
	b.Apply(outcome)
	return outcome.Fills
}

// Match runs incoming against the book. Every counterparty mutation
// happens for real, immediately: a partially filled counterparty has its
// Filled increased directly, and a fully consumed one is popped from its
// heap and removed from resting right here, not deferred. The incoming
// order itself is the one thing never touched beyond its own Filled
// field; it is not rested and not added to resting, that step is
// deliberately left to Apply, so a caller can publish downstream events
// for these fills first and only make the incoming order visible to
// future matches once that publishing has actually succeeded.
func (b *Book) Match(incoming *Order) *MatchOutcome {
	var fills []FillResult
	var popped []*Order

	switch incoming.Side {
	case Buy:
		fills, popped = b.matchBuy(incoming)
	case Sell:
		fills, popped = b.matchSell(incoming)
	}

	return &MatchOutcome{incoming: incoming, Fills: fills, poppedOrders: popped}
}

// Apply finalizes a MatchOutcome. The counterparty side of every fill
// already happened for real during Match, so the only remaining work is
// resting the incoming order itself, if it still has quantity left.
func (b *Book) Apply(outcome *MatchOutcome) {
	incoming := outcome.incoming
	if incoming.Remaining() > 0 {
		switch incoming.Side {
		case Buy:
			heap.Push(b.bids, incoming)
		case Sell:
			heap.Push(b.asks, incoming)
		}
		b.resting[incoming.ID] = incoming
	}
}

// Discard fully reverses a MatchOutcome, restoring the book to exactly
// the state it was in before Match ran: every counterparty's Filled is
// rolled back by the quantity it was matched for, every counterparty
// Match popped for being fully consumed is pushed back and re-registered
// in resting, and the incoming order's own Filled is reset to zero,
// since Match only ever mutated it, never rested it.
func (b *Book) Discard(outcome *MatchOutcome) {
	poppedByID := make(map[string]*Order, len(outcome.poppedOrders))
	for _, o := range outcome.poppedOrders {
		poppedByID[o.ID] = o
	}

	for _, fill := range outcome.Fills {
		var counterpartyID string
		switch outcome.incoming.Side {
		case Buy:
			counterpartyID = fill.SellOrderID
		case Sell:
			counterpartyID = fill.BuyOrderID
		}

		// The counterparty for this fill is either one Match popped for
		// being fully consumed, or, for at most one fill in the whole
		// outcome, the final match where the incoming order ran out
		// first and left this counterparty resting with quantity still
		// remaining. Both cases are real, expected outcomes of a match,
		// not error paths.
		counterparty, ok := poppedByID[counterpartyID]
		if !ok {
			counterparty, ok = b.resting[counterpartyID]
		}
		if !ok {
			continue
		}
		counterparty.Filled -= fill.Quantity
	}

	for _, popped := range outcome.poppedOrders {
		switch popped.Side {
		case Buy:
			heap.Push(b.bids, popped)
		case Sell:
			heap.Push(b.asks, popped)
		}
		b.resting[popped.ID] = popped
	}

	outcome.incoming.Filled = 0
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

func (b *Book) matchBuy(buy *Order) ([]FillResult, []*Order) {
	var fills []FillResult
	var popped []*Order
	for b.asks.Len() > 0 && buy.Remaining() > 0 {
		best := (*b.asks)[0]
		if best.PriceCents > buy.PriceCents {
			break
		}
		qty := min(buy.Remaining(), best.Remaining())
		buy.Filled += qty
		best.Filled += qty
		fills = append(fills, FillResult{
			BuyOrderID:    buy.ID,
			SellOrderID:   best.ID,
			BuyerID:       buy.UserID,
			SellerID:      best.UserID,
			Quantity:      qty,
			PriceCents:    best.PriceCents,
			BuyRemaining:  buy.Remaining(),
			SellRemaining: best.Remaining(),
		})
		if best.Remaining() == 0 {
			heap.Pop(b.asks)
			delete(b.resting, best.ID)
			popped = append(popped, best)
		}
	}
	return fills, popped
}

func (b *Book) matchSell(sell *Order) ([]FillResult, []*Order) {
	var fills []FillResult
	var popped []*Order
	for b.bids.Len() > 0 && sell.Remaining() > 0 {
		best := (*b.bids)[0]
		if best.PriceCents < sell.PriceCents {
			break
		}
		qty := min(sell.Remaining(), best.Remaining())
		sell.Filled += qty
		best.Filled += qty
		fills = append(fills, FillResult{
			BuyOrderID:    best.ID,
			SellOrderID:   sell.ID,
			BuyerID:       best.UserID,
			SellerID:      sell.UserID,
			Quantity:      qty,
			PriceCents:    best.PriceCents,
			BuyRemaining:  best.Remaining(),
			SellRemaining: sell.Remaining(),
		})
		if best.Remaining() == 0 {
			heap.Pop(b.bids)
			delete(b.resting, best.ID)
			popped = append(popped, best)
		}
	}
	return fills, popped
}
