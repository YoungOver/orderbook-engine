package engine

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/YoungOver/orderbook-engine/internal/book"
	"github.com/YoungOver/orderbook-engine/internal/journal"
)

func TestJournalReplayRestoresBook(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.bin")
	jw, _ := journal.Open(path, false)
	e := New([]string{"X"}, jw, nil, 1024)
	ctx, cancel := context.WithCancel(context.Background())
	e.Start(ctx)
	for i := 1; i <= 100; i++ {
		side := book.Buy
		if i%2 == 0 {
			side = book.Sell
		}
		price := int64(100 + i%7)
		if _, err := e.Do(ctx, "X", &Command{Op: OpSubmit, ID: uint64(i), Side: side, Kind: book.Limit, Price: price, Qty: 3}); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := e.Depth(ctx, "X", 50)
	cancel()
	e.Wait()
	jw.Close()

	e2 := New([]string{"X"}, nil, nil, 1024)
	n, err := e2.Replay(path)
	if err != nil || n != 100 {
		t.Fatalf("replay n=%d err=%v", n, err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	e2.Start(ctx2)
	after, _ := e2.Depth(ctx2, "X", 50)
	if len(before.Bids) != len(after.Bids) || len(before.Asks) != len(after.Asks) {
		t.Fatalf("depth differs after replay: %v vs %v", before, after)
	}
	for i := range before.Bids {
		if before.Bids[i] != after.Bids[i] {
			t.Fatalf("bid level %d: %v vs %v", i, before.Bids[i], after.Bids[i])
		}
	}
}

func BenchmarkEngineParallel(b *testing.B) {
	e := New([]string{"A", "B", "C", "D"}, nil, nil, 1<<16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	syms := []string{"A", "B", "C", "D"}
	var mu sync.Mutex
	next := uint64(0)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			mu.Lock()
			next++
			id := next
			mu.Unlock()
			side := book.Side(1 + i%2)
			e.Do(ctx, syms[i%4], &Command{Op: OpSubmit, ID: id, Side: side, Kind: book.Limit, Price: int64(1000 + i%11), Qty: 1})
			i++
		}
	})
}
