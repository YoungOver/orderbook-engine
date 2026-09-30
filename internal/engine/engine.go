// Package engine runs one single-writer matching loop per symbol.
//
// Commands arrive through a bounded channel, are appended to the journal in
// batches (group commit) and applied to the book strictly in order. Readers
// never touch the book directly: they receive trades through the feed and
// depth snapshots through the loop itself.
package engine

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/YoungOver/orderbook-engine/internal/book"
	"github.com/YoungOver/orderbook-engine/internal/journal"
)

var ErrUnknownSymbol = errors.New("engine: unknown symbol")
var ErrOverloaded = errors.New("engine: queue full")

type Op uint8

const (
	OpSubmit Op = iota + 1
	OpCancel
	opDepth
)

type Command struct {
	Op    Op
	ID    uint64
	Side  book.Side
	Kind  book.Kind
	Price int64
	Qty   int64

	reply chan Result
	depth int
}

type Result struct {
	Trades    []book.Trade
	Cancelled int64
	Err       error
	Bids      []book.Level
	Asks      []book.Level
}

// Publisher receives every trade after it has been journaled and applied.
type Publisher interface {
	Publish(symbol string, trades []book.Trade)
}

type Stats struct {
	Commands atomic.Uint64
	Trades   atomic.Uint64
	Batches  atomic.Uint64
	Rejected atomic.Uint64
}

type shard struct {
	symbol string
	in     chan *Command
	book   *book.Book
}

type Engine struct {
	shards  map[string]*shard
	journal *journal.Writer
	pub     Publisher
	Stats   Stats
	replies sync.Pool
	wg      sync.WaitGroup
	nextID  atomic.Uint64
}

func New(symbols []string, j *journal.Writer, pub Publisher, queue int) *Engine {
	e := &Engine{shards: make(map[string]*shard, len(symbols)), journal: j, pub: pub}
	e.replies.New = func() any { return make(chan Result, 1) }
	for _, s := range symbols {
		e.shards[s] = &shard{symbol: s, in: make(chan *Command, queue), book: book.New(s)}
	}
	return e
}

// SetJournal attaches the journal after replay, before Start.
func (e *Engine) SetJournal(j *journal.Writer) { e.journal = j }

// NextID hands out server-side order ids.
func (e *Engine) NextID() uint64 { return e.nextID.Add(1) }

// Replay rebuilds books from the journal before the loops start.
func (e *Engine) Replay(path string) (int, error) {
	n := 0
	var buf []book.Trade
	err := journal.Replay(path, func(r journal.Record) error {
		sh, ok := e.shards[r.Symbol]
		if !ok {
			return nil
		}
		if r.ID > e.nextID.Load() {
			e.nextID.Store(r.ID)
		}
		switch Op(r.Op) {
		case OpSubmit:
			buf, _ = sh.book.Submit(r.ID, book.Side(r.Side), book.Kind(r.Kind), r.Price, r.Qty, buf[:0])
		case OpCancel:
			sh.book.Cancel(r.ID)
		}
		n++
		return nil
	})
	return n, err
}

func (e *Engine) Start(ctx context.Context) {
	for _, sh := range e.shards {
		e.wg.Add(1)
		go e.loop(ctx, sh)
	}
}

func (e *Engine) Wait() { e.wg.Wait() }

// Do enqueues a command and waits for its result.
func (e *Engine) Do(ctx context.Context, symbol string, c *Command) (Result, error) {
	sh, ok := e.shards[symbol]
	if !ok {
		return Result{}, ErrUnknownSymbol
	}
	ch := e.replies.Get().(chan Result)
	c.reply = ch
	select {
	case sh.in <- c:
	default:
		e.Stats.Rejected.Add(1)
		e.replies.Put(ch)
		return Result{}, ErrOverloaded
	}
	select {
	case r := <-ch:
		e.replies.Put(ch)
		return r, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

func (e *Engine) Depth(ctx context.Context, symbol string, n int) (Result, error) {
	return e.Do(ctx, symbol, &Command{Op: opDepth, depth: n})
}

const maxBatch = 512

func (e *Engine) loop(ctx context.Context, sh *shard) {
	defer e.wg.Done()
	batch := make([]*Command, 0, maxBatch)
	recs := make([]journal.Record, 0, maxBatch)
	var trades []book.Trade
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-sh.in:
			batch = append(batch[:0], c)
		drain:
			for len(batch) < maxBatch {
				select {
				case c := <-sh.in:
					batch = append(batch, c)
				default:
					break drain
				}
			}
		}

		// group commit: one write + one fsync per batch
		if e.journal != nil {
			recs = recs[:0]
			for _, c := range batch {
				if c.Op == OpSubmit || c.Op == OpCancel {
					recs = append(recs, journal.Record{Symbol: sh.symbol, Op: uint8(c.Op), ID: c.ID, Side: uint8(c.Side), Kind: uint8(c.Kind), Price: c.Price, Qty: c.Qty, TS: time.Now().UnixNano()})
				}
			}
			if len(recs) > 0 {
				if err := e.journal.Append(recs); err != nil {
					for _, c := range batch {
						c.reply <- Result{Err: err}
					}
					continue
				}
			}
		}
		e.Stats.Batches.Add(1)

		for _, c := range batch {
			var res Result
			switch c.Op {
			case OpSubmit:
				trades, res.Err = sh.book.Submit(c.ID, c.Side, c.Kind, c.Price, c.Qty, trades[:0])
				if len(trades) > 0 {
					res.Trades = append([]book.Trade(nil), trades...)
					e.Stats.Trades.Add(uint64(len(trades)))
					if e.pub != nil {
						e.pub.Publish(sh.symbol, res.Trades)
					}
				}
			case OpCancel:
				res.Cancelled, res.Err = sh.book.Cancel(c.ID)
			case opDepth:
				res.Bids, res.Asks = sh.book.Depth(c.depth)
			}
			e.Stats.Commands.Add(1)
			c.reply <- res
		}
	}
}
