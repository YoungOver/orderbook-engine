package book

import (
	"math/rand"
	"testing"
)

func TestPriceTimePriority(t *testing.T) {
	b := New("BTC-USDT")
	var tr []Trade
	var err error
	if tr, err = b.Submit(1, Sell, Limit, 101, 5, tr[:0]); err != nil || len(tr) != 0 {
		t.Fatalf("resting sell: %v %v", tr, err)
	}
	b.Submit(2, Sell, Limit, 100, 3, nil)
	b.Submit(3, Sell, Limit, 100, 4, nil)

	tr, err = b.Submit(10, Buy, Limit, 101, 9, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct{ maker, qty, price int64 }{{2, 3, 100}, {3, 4, 100}, {1, 2, 101}}
	if len(tr) != len(want) {
		t.Fatalf("got %d trades: %+v", len(tr), tr)
	}
	for i, w := range want {
		if int64(tr[i].MakerID) != w.maker || tr[i].Qty != w.qty || tr[i].Price != w.price {
			t.Fatalf("trade %d = %+v, want %+v", i, tr[i], w)
		}
	}
	bid, ask, okB, okA := b.BestBidAsk()
	if okB || !okA || ask != 101 || bid != 0 {
		t.Fatalf("top of book after match: bid=%d ask=%d okB=%v okA=%v", bid, ask, okB, okA)
	}
}

func TestCancelAndRest(t *testing.T) {
	b := New("X")
	b.Submit(1, Buy, Limit, 99, 10, nil)
	b.Submit(2, Buy, Limit, 99, 5, nil)
	if left, err := b.Cancel(1); err != nil || left != 10 {
		t.Fatalf("cancel: %d %v", left, err)
	}
	tr, _ := b.Submit(3, Sell, Limit, 99, 7, nil)
	if len(tr) != 1 || tr[0].MakerID != 2 || tr[0].Qty != 5 {
		t.Fatalf("unexpected trades %+v", tr)
	}
	_, ask, _, okA := b.BestBidAsk()
	if !okA || ask != 99 {
		t.Fatalf("remainder of sell must rest at 99, got %d", ask)
	}
	if _, err := b.Cancel(1); err != ErrUnknownOrder {
		t.Fatalf("double cancel must fail, got %v", err)
	}
}

func TestIOCAndMarket(t *testing.T) {
	b := New("X")
	b.Submit(1, Sell, Limit, 100, 2, nil)
	tr, _ := b.Submit(2, Buy, IOC, 100, 5, nil)
	if len(tr) != 1 || b.Resting() != 0 {
		t.Fatalf("IOC must not rest: trades=%d resting=%d", len(tr), b.Resting())
	}
	if _, err := b.Submit(3, Buy, Market, 0, 1, nil); err != ErrNoLiquidity {
		t.Fatalf("market into empty book: %v", err)
	}
}

// TestConservation checks that quantity is never created or lost under a random workload.
func TestConservation(t *testing.T) {
	b := New("X")
	r := rand.New(rand.NewSource(42))
	var submitted, filled, cancelled int64
	var tr []Trade
	live := map[uint64]bool{}
	for id := uint64(1); id <= 200_000; id++ {
		if r.Intn(5) == 0 && len(live) > 0 {
			for victim := range live {
				if left, err := b.Cancel(victim); err == nil {
					cancelled += left
				}
				delete(live, victim)
				break
			}
			continue
		}
		side := Buy
		if r.Intn(2) == 0 {
			side = Sell
		}
		qty := int64(1 + r.Intn(50))
		price := int64(9_900 + r.Intn(200))
		submitted += qty
		tr, _ = b.Submit(id, side, Limit, price, qty, tr[:0])
		for _, x := range tr {
			filled += 2 * x.Qty
			delete(live, x.MakerID)
		}
		if _, ok := b.orders[id]; ok {
			live[id] = true
		}
	}
	var resting int64
	for _, o := range b.orders {
		resting += o.Qty
	}
	if submitted != filled+cancelled+resting {
		t.Fatalf("conservation broken: submitted=%d filled=%d cancelled=%d resting=%d", submitted, filled, cancelled, resting)
	}
}

func BenchmarkSubmitMixed(b *testing.B) {
	bk := New("X")
	r := rand.New(rand.NewSource(1))
	prices := make([]int64, 1<<16)
	sides := make([]Side, 1<<16)
	for i := range prices {
		prices[i] = int64(9_950 + r.Intn(100))
		sides[i] = Side(1 + r.Intn(2))
	}
	var tr []Trade
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i & (1<<16 - 1)
		tr, _ = bk.Submit(uint64(i+1), sides[j], Limit, prices[j], 10, tr[:0])
	}
}

func BenchmarkCancel(b *testing.B) {
	bk := New("X")
	for i := 0; i < b.N; i++ {
		bk.Submit(uint64(i+1), Buy, Limit, int64(1000+i%500), 1, nil)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bk.Cancel(uint64(i + 1))
	}
}
