// Package api exposes the engine over HTTP/JSON.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/YoungOver/orderbook-engine/internal/book"
	"github.com/YoungOver/orderbook-engine/internal/engine"
	"github.com/YoungOver/orderbook-engine/internal/feed"
)

type orderReq struct {
	Symbol string `json:"symbol"`
	Side   string `json:"side"`
	Type   string `json:"type"`
	Price  int64  `json:"price"`
	Qty    int64  `json:"qty"`
}

type orderResp struct {
	ID     uint64       `json:"id"`
	Trades []book.Trade `json:"trades"`
}

type Server struct {
	eng     *engine.Engine
	hub     *feed.Hub
	latency *prometheus.HistogramVec
	mux     *http.ServeMux
}

func New(eng *engine.Engine, hub *feed.Hub, reg *prometheus.Registry) *Server {
	s := &Server{eng: eng, hub: hub, mux: http.NewServeMux()}
	s.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "obe_http_request_seconds",
		Help:    "HTTP latency by route and status.",
		Buckets: []float64{50e-6, 100e-6, 250e-6, 500e-6, 1e-3, 2.5e-3, 5e-3, 10e-3, 50e-3},
	}, []string{"route", "code"})
	reg.MustRegister(s.latency)
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "obe_commands_total", Help: "Commands applied."}, func() float64 { return float64(eng.Stats.Commands.Load()) }))
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "obe_trades_total", Help: "Trades produced."}, func() float64 { return float64(eng.Stats.Trades.Load()) }))
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "obe_batches_total", Help: "Group-commit batches."}, func() float64 { return float64(eng.Stats.Batches.Load()) }))
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "obe_rejected_total", Help: "Commands rejected by backpressure."}, func() float64 { return float64(eng.Stats.Rejected.Load()) }))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "obe_feed_subscribers", Help: "Connected SSE clients."}, func() float64 { return float64(hub.Subscribers()) }))

	s.mux.HandleFunc("POST /v1/orders", s.timed("submit", s.submit))
	s.mux.HandleFunc("DELETE /v1/orders/{symbol}/{id}", s.timed("cancel", s.cancel))
	s.mux.HandleFunc("GET /v1/book/{symbol}", s.timed("depth", s.depth))
	s.mux.Handle("GET /v1/stream", hub)
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }

func (s *Server) timed(route string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: 200}
		h(sw, r)
		s.latency.WithLabelValues(route, strconv.Itoa(sw.code)).Observe(time.Since(start).Seconds())
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, err error) {
	code := http.StatusBadRequest
	switch {
	case errors.Is(err, engine.ErrOverloaded):
		code = http.StatusTooManyRequests
	case errors.Is(err, engine.ErrUnknownSymbol), errors.Is(err, book.ErrUnknownOrder):
		code = http.StatusNotFound
	case errors.Is(err, context.DeadlineExceeded):
		code = http.StatusGatewayTimeout
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	var req orderReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		fail(w, err)
		return
	}
	side := book.Buy
	if req.Side == "sell" {
		side = book.Sell
	}
	kind := book.Limit
	switch req.Type {
	case "market":
		kind = book.Market
	case "ioc":
		kind = book.IOC
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	id := s.eng.NextID()
	res, err := s.eng.Do(ctx, req.Symbol, &engine.Command{Op: engine.OpSubmit, ID: id, Side: side, Kind: kind, Price: req.Price, Qty: req.Qty})
	if err == nil {
		err = res.Err
	}
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, orderResp{ID: id, Trades: res.Trades})
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		fail(w, err)
		return
	}
	res, err := s.eng.Do(r.Context(), r.PathValue("symbol"), &engine.Command{Op: engine.OpCancel, ID: id})
	if err == nil {
		err = res.Err
	}
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"cancelled_qty": res.Cancelled})
}

func (s *Server) depth(w http.ResponseWriter, r *http.Request) {
	n, _ := strconv.Atoi(r.URL.Query().Get("levels"))
	if n <= 0 || n > 500 {
		n = 20
	}
	res, err := s.eng.Depth(r.Context(), r.PathValue("symbol"), n)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"symbol": r.PathValue("symbol"), "bids": res.Bids, "asks": res.Asks})
}
