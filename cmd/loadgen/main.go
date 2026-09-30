// loadgen drives the HTTP API with a closed-loop workload and prints
// throughput and latency percentiles.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	target := flag.String("url", "http://localhost:8080", "server base url")
	conc := flag.Int("c", 256, "concurrent workers")
	dur := flag.Duration("d", 20*time.Second, "test duration")
	symbols := flag.String("symbols", "BTC-USDT,ETH-USDT,SOL-USDT", "symbols")
	flag.Parse()

	syms := bytes.Split([]byte(*symbols), []byte(","))
	tr := &http.Transport{MaxIdleConns: *conc * 2, MaxIdleConnsPerHost: *conc * 2, IdleConnTimeout: 30 * time.Second}
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()
	var ok, errs atomic.Uint64
	lat := make([][]time.Duration, *conc)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			local := make([]time.Duration, 0, 1<<16)
			for ctx.Err() == nil {
				side := "buy"
				if r.Intn(2) == 0 {
					side = "sell"
				}
				body, _ := json.Marshal(map[string]any{
					"symbol": string(syms[r.Intn(len(syms))]),
					"side":   side, "type": "limit",
					"price": 10_000 + r.Intn(40) - 20, "qty": 1 + r.Intn(20),
				})
				t0 := time.Now()
				resp, err := client.Post(*target+"/v1/orders", "application/json", bytes.NewReader(body))
				if err != nil {
					errs.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusCreated {
					ok.Add(1)
					local = append(local, time.Since(t0))
				} else {
					errs.Add(1)
				}
			}
			lat[w] = local
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[int(float64(len(all)-1)*p)]
	}
	fmt.Printf("requests ok=%d errors=%d in %s\n", ok.Load(), errs.Load(), elapsed.Round(time.Millisecond))
	fmt.Printf("throughput %.0f req/s\n", float64(ok.Load())/elapsed.Seconds())
	fmt.Printf("latency p50=%s p90=%s p99=%s p99.9=%s\n", pct(.5), pct(.9), pct(.99), pct(.999))
}
