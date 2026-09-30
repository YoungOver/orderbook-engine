// Package book implements a price-time priority limit order book.
//
// The book is not safe for concurrent use: the engine owns one book per symbol
// and mutates it from a single goroutine, which removes locks from the hot path.
package book

import (
	"container/heap"
	"errors"
	"sort"
)

type Side uint8

const (
	Buy Side = iota + 1
	Sell
)

func (s Side) String() string {
	if s == Buy {
		return "buy"
	}
	return "sell"
}

type Kind uint8

const (
	Limit Kind = iota + 1
	Market
	IOC // immediate-or-cancel limit order
)

var (
	ErrUnknownOrder  = errors.New("book: unknown order")
	ErrDuplicateID   = errors.New("book: duplicate order id")
	ErrInvalidQty    = errors.New("book: quantity must be positive")
	ErrInvalidPrice  = errors.New("book: price must be positive")
	ErrNoLiquidity   = errors.New("book: no liquidity for market order")
)

// Order is a resting or incoming order. Prices are integer ticks to avoid
// floating point drift; quantities are integer lots.
type Order struct {
	ID    uint64
	Side  Side
	Kind  Kind
	Price int64
	Qty   int64
	Seq   uint64

	level      *level
	prev, next *Order
}

// Trade is emitted for every fill.
type Trade struct {
	MakerID uint64
	TakerID uint64
	Price   int64
	Qty     int64
	Side    Side // taker side
	Seq     uint64
}

// level is a FIFO queue of orders at one price, stored as an intrusive list
// so that cancel is O(1).
type level struct {
	price      int64
	head, tail *Order
	volume     int64
	count      int
	heapIdx    int
}

func (l *level) push(o *Order) {
	o.level = l
	o.prev = l.tail
	o.next = nil
	if l.tail != nil {
		l.tail.next = o
	} else {
		l.head = o
	}
	l.tail = o
	l.volume += o.Qty
	l.count++
}

func (l *level) remove(o *Order) {
	if o.prev != nil {
		o.prev.next = o.next
	} else {
		l.head = o.next
	}
	if o.next != nil {
		o.next.prev = o.prev
	} else {
		l.tail = o.prev
	}
	l.volume -= o.Qty
	l.count--
	o.prev, o.next, o.level = nil, nil, nil
}

// priceHeap keeps the best price on top. For bids it is a max-heap, for asks a min-heap.
type priceHeap struct {
	levels []*level
	max    bool
}

func (h priceHeap) Len() int { return len(h.levels) }
func (h priceHeap) Less(i, j int) bool {
	if h.max {
		return h.levels[i].price > h.levels[j].price
	}
	return h.levels[i].price < h.levels[j].price
}
func (h priceHeap) Swap(i, j int) {
	h.levels[i], h.levels[j] = h.levels[j], h.levels[i]
	h.levels[i].heapIdx = i
	h.levels[j].heapIdx = j
}
func (h *priceHeap) Push(x any) {
	l := x.(*level)
	l.heapIdx = len(h.levels)
	h.levels = append(h.levels, l)
}
func (h *priceHeap) Pop() any {
	old := h.levels
	n := len(old)
	l := old[n-1]
	old[n-1] = nil
	h.levels = old[:n-1]
	l.heapIdx = -1
	return l
}

type sideBook struct {
	heap   priceHeap
	levels map[int64]*level
}

func newSide(max bool) *sideBook {
	return &sideBook{heap: priceHeap{max: max}, levels: make(map[int64]*level, 1024)}
}

func (s *sideBook) best() *level {
	if len(s.heap.levels) == 0 {
		return nil
	}
	return s.heap.levels[0]
}

func (s *sideBook) add(o *Order) {
	l, ok := s.levels[o.Price]
	if !ok {
		l = &level{price: o.Price}
		s.levels[o.Price] = l
		heap.Push(&s.heap, l)
	}
	l.push(o)
}

func (s *sideBook) dropIfEmpty(l *level) {
	if l.count == 0 {
		heap.Remove(&s.heap, l.heapIdx)
		delete(s.levels, l.price)
	}
}

// Book is a single-symbol order book.
type Book struct {
	Symbol string
	bids   *sideBook
	asks   *sideBook
	orders map[uint64]*Order
	seq    uint64
	pool   []*Order
}

func New(symbol string) *Book {
	return &Book{
		Symbol: symbol,
		bids:   newSide(true),
		asks:   newSide(false),
		orders: make(map[uint64]*Order, 1<<16),
	}
}

func (b *Book) alloc() *Order {
	if n := len(b.pool); n > 0 {
		o := b.pool[n-1]
		b.pool = b.pool[:n-1]
		return o
	}
	return &Order{}
}

func (b *Book) release(o *Order) {
	*o = Order{}
	if len(b.pool) < 1<<16 {
		b.pool = append(b.pool, o)
	}
}

// Submit matches an incoming order against the opposite side and rests the
// remainder for Limit orders. Trades are appended to dst to avoid allocations.
func (b *Book) Submit(id uint64, side Side, kind Kind, price, qty int64, dst []Trade) ([]Trade, error) {
	if qty <= 0 {
		return dst, ErrInvalidQty
	}
	if kind != Market && price <= 0 {
		return dst, ErrInvalidPrice
	}
	if _, dup := b.orders[id]; dup {
		return dst, ErrDuplicateID
	}
	b.seq++
	opp := b.asks
	if side == Sell {
		opp = b.bids
	}
	if kind == Market && opp.best() == nil {
		return dst, ErrNoLiquidity
	}

	remaining := qty
	for remaining > 0 {
		lvl := opp.best()
		if lvl == nil {
			break
		}
		if kind != Market {
			if side == Buy && lvl.price > price {
				break
			}
			if side == Sell && lvl.price < price {
				break
			}
		}
		for remaining > 0 && lvl.head != nil {
			maker := lvl.head
			fill := min(remaining, maker.Qty)
			b.seq++
			dst = append(dst, Trade{MakerID: maker.ID, TakerID: id, Price: lvl.price, Qty: fill, Side: side, Seq: b.seq})
			remaining -= fill
			maker.Qty -= fill
			lvl.volume -= fill
			if maker.Qty == 0 {
				lvl.remove(maker)
				delete(b.orders, maker.ID)
				b.release(maker)
			}
		}
		opp.dropIfEmpty(lvl)
	}

	if remaining > 0 && kind == Limit {
		o := b.alloc()
		o.ID, o.Side, o.Kind, o.Price, o.Qty, o.Seq = id, side, kind, price, remaining, b.seq
		b.orders[id] = o
		if side == Buy {
			b.bids.add(o)
		} else {
			b.asks.add(o)
		}
	}
	return dst, nil
}

// Cancel removes a resting order in O(1) plus heap fix-up when a level empties.
func (b *Book) Cancel(id uint64) (int64, error) {
	o, ok := b.orders[id]
	if !ok {
		return 0, ErrUnknownOrder
	}
	left := o.Qty
	l := o.level
	l.remove(o)
	if o.Side == Buy {
		b.bids.dropIfEmpty(l)
	} else {
		b.asks.dropIfEmpty(l)
	}
	delete(b.orders, id)
	b.release(o)
	return left, nil
}

// Level is an aggregated depth entry.
type Level struct {
	Price  int64 `json:"price"`
	Volume int64 `json:"volume"`
	Orders int   `json:"orders"`
}

// Depth returns up to n best levels per side. It copies and partially sorts,
// which is fine for snapshots taken a few times per second.
func (b *Book) Depth(n int) (bids, asks []Level) {
	return topN(b.bids, n, true), topN(b.asks, n, false)
}

func topN(s *sideBook, n int, desc bool) []Level {
	out := make([]Level, 0, len(s.levels))
	for _, l := range s.levels {
		out = append(out, Level{Price: l.price, Volume: l.volume, Orders: l.count})
	}
	sort.Slice(out, func(i, j int) bool {
		if desc {
			return out[i].Price > out[j].Price
		}
		return out[i].Price < out[j].Price
	})
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// BestBidAsk returns top of book; ok is false for an empty side.
func (b *Book) BestBidAsk() (bid, ask int64, okBid, okAsk bool) {
	if l := b.bids.best(); l != nil {
		bid, okBid = l.price, true
	}
	if l := b.asks.best(); l != nil {
		ask, okAsk = l.price, true
	}
	return
}

// Resting reports how many orders currently rest in the book.
func (b *Book) Resting() int { return len(b.orders) }
