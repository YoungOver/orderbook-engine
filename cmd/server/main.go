package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/YoungOver/orderbook-engine/internal/api"
	"github.com/YoungOver/orderbook-engine/internal/engine"
	"github.com/YoungOver/orderbook-engine/internal/feed"
	"github.com/YoungOver/orderbook-engine/internal/journal"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	symbols := flag.String("symbols", "BTC-USDT,ETH-USDT,SOL-USDT", "comma-separated symbols")
	jpath := flag.String("journal", "data/journal.bin", "journal path, empty disables persistence")
	fsync := flag.Bool("fsync", true, "fsync every group-commit batch")
	queue := flag.Int("queue", 65536, "per-symbol command queue")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	syms := strings.Split(*symbols, ",")

	var jw *journal.Writer
	hub := feed.NewHub(256)
	eng := engine.New(syms, nil, hub, *queue)
	if *jpath != "" {
		os.MkdirAll("data", 0o755)
		start := time.Now()
		n, err := eng.Replay(*jpath)
		if err != nil {
			log.Error("replay failed", "err", err)
			os.Exit(1)
		}
		log.Info("journal replayed", "records", n, "took", time.Since(start).String())
		if jw, err = journal.Open(*jpath, *fsync); err != nil {
			log.Error("open journal", "err", err)
			os.Exit(1)
		}
		eng.SetJournal(jw)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	eng.Start(ctx)

	srv := &http.Server{Addr: *addr, Handler: api.New(eng, hub, reg), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("listening", "addr", *addr, "symbols", syms)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http", "err", err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdown)
	eng.Wait()
	if jw != nil {
		jw.Close()
	}
	log.Info("stopped")
}
